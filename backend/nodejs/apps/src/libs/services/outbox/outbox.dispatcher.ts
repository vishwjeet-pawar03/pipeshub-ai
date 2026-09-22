import { Logger } from '../logger.service';
import { IMessageProducer, StreamMessage } from '../../types/messaging.types';
import { IOutboxEvent, OutboxEvent } from './outbox.schema';

const DEFAULT_POLL_INTERVAL_MS = 2000;

/** First retry waits this long; each further one doubles it. */
const BASE_BACKOFF_MS = 1000;

/**
 * Backoff stops growing here. A broker that has been down for an hour is no
 * less likely to come back in the next five minutes than in the next hour, and
 * an unbounded delay would mean an event effectively never arriving.
 */
const MAX_BACKOFF_MS = 5 * 60 * 1000;

/**
 * After this many failures an event is parked as `failed` rather than retried
 * forever. A message the broker will never accept — malformed, or too large —
 * would otherwise be tried until the end of time while newer events queued
 * behind it. Parked rows stay in the collection to be inspected and replayed.
 */
const MAX_ATTEMPTS = 20;

/**
 * A claim older than this is treated as abandoned. The holder died mid-publish,
 * and without this the row would sit in `publishing` forever.
 */
const CLAIM_LEASE_MS = 60 * 1000;

/** Failures are only worth waking someone for once they stop looking transient. */
const ALERT_AFTER_ATTEMPTS = 5;

function backoffFor(attempts: number): Date {
  const delay = Math.min(BASE_BACKOFF_MS * 2 ** attempts, MAX_BACKOFF_MS);
  return new Date(Date.now() + delay);
}

/**
 * Delivers outbox events to the broker, one at a time and oldest first.
 *
 * Sequential on purpose. These events describe a sequence of changes to the
 * same entities — a user added, then updated, then deleted — and publishing
 * them in parallel would let the graph receive them out of order. The volume is
 * low enough that doing one at a time costs nothing worth having.
 *
 * Claiming is a single atomic update, so several application instances can run
 * this at once without two of them sending the same event.
 */
export class OutboxDispatcher {
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;

  constructor(
    private readonly producer: IMessageProducer,
    private readonly logger: Logger,
    private readonly pollIntervalMs: number = DEFAULT_POLL_INTERVAL_MS,
  ) {}

  start(): void {
    if (this.timer) return;
    this.stopped = false;
    this.timer = setInterval(() => {
      void this.drain();
    }, this.pollIntervalMs);
    // Long enough between ticks that holding the event loop open would delay
    // shutdown for no reason.
    this.timer.unref();
    this.logger.info('Outbox dispatcher started', {
      pollIntervalMs: this.pollIntervalMs,
    });
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.logger.info('Outbox dispatcher stopped');
  }

  /**
   * Publishes everything currently due. Exposed so a test can drive a tick
   * directly instead of waiting on the timer.
   */
  async drain(): Promise<number> {
    // One pass at a time: a slow broker must not have ticks pile up behind it.
    if (this.running) return 0;
    this.running = true;
    let delivered = 0;
    try {
      for (;;) {
        if (this.stopped) break;
        const event = await this.claimNext();
        if (!event) break;
        const sent = await this.deliver(event);
        if (sent) delivered += 1;
      }
    } catch (error) {
      // The loop itself failing (the database is unreachable, say) is not the
      // same as an event failing, and must not stop future ticks.
      this.logger.error('Outbox dispatcher pass failed', {
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      this.running = false;
    }
    return delivered;
  }

  /**
   * Takes the oldest due event, or an abandoned claim, and marks it as ours.
   * Atomic, so two instances cannot take the same row.
   */
  private async claimNext(): Promise<IOutboxEvent | null> {
    const now = new Date();
    return OutboxEvent.findOneAndUpdate(
      {
        $or: [
          { status: 'pending', nextAttemptAt: { $lte: now } },
          {
            status: 'publishing',
            claimedAt: { $lte: new Date(now.getTime() - CLAIM_LEASE_MS) },
          },
        ],
      },
      { $set: { status: 'publishing', claimedAt: now } },
      { sort: { createdAt: 1 }, new: true },
    ).exec();
  }

  private async deliver(event: IOutboxEvent): Promise<boolean> {
    const message: StreamMessage<string> = {
      key: event.key,
      value: event.value,
      headers: event.headers,
    };

    try {
      if (!this.producer.isConnected()) {
        await this.producer.connect();
      }
      await this.producer.publish(event.topic, message);
    } catch (error) {
      await this.recordFailure(event, error);
      return false;
    }

    await OutboxEvent.updateOne(
      { _id: event._id },
      {
        $set: { status: 'published', publishedAt: new Date() },
        $unset: { claimedAt: '', lastError: '' },
      },
    ).exec();
    return true;
  }

  private async recordFailure(
    event: IOutboxEvent,
    error: unknown,
  ): Promise<void> {
    const attempts = event.attempts + 1;
    const message = error instanceof Error ? error.message : String(error);
    const givingUp = attempts >= MAX_ATTEMPTS;

    await OutboxEvent.updateOne(
      { _id: event._id },
      {
        $set: {
          status: givingUp ? 'failed' : 'pending',
          attempts,
          nextAttemptAt: backoffFor(attempts),
          lastError: message,
        },
        $unset: { claimedAt: '' },
      },
    ).exec();

    // Quiet while it still looks like a blip, loud once it does not. This is
    // the visibility the old code lacked: a failure here is on the record and
    // can be alerted on, rather than swallowed behind a success.
    const detail = {
      outboxId: String(event._id),
      topic: event.topic,
      key: event.key,
      attempts,
      error: message,
    };
    if (givingUp) {
      this.logger.error(
        'Outbox event parked after repeated failures; it will not be retried',
        detail,
      );
    } else if (attempts >= ALERT_AFTER_ATTEMPTS) {
      this.logger.error('Outbox event still failing to publish', detail);
    } else {
      this.logger.warn('Outbox event failed to publish; will retry', detail);
    }
  }
}

/** Counts by status, for a health endpoint or a metric. */
export async function outboxBacklog(): Promise<Record<OutboxCountKey, number>> {
  const rows = await OutboxEvent.aggregate<{ _id: string; count: number }>([
    { $group: { _id: '$status', count: { $sum: 1 } } },
  ]);
  const counts: Record<OutboxCountKey, number> = {
    pending: 0,
    publishing: 0,
    published: 0,
    failed: 0,
  };
  for (const row of rows) {
    if (row._id in counts) {
      counts[row._id as OutboxCountKey] = row.count;
    }
  }
  return counts;
}

export type OutboxCountKey = 'pending' | 'publishing' | 'published' | 'failed';
