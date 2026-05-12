import { Logger } from '../../../../libs/services/logger.service';
import { KeyValueStoreService } from '../../../../libs/services/keyValueStore.service';
import { CrawlingSchedulerService } from '../../../crawling_manager/services/crawling_service';
import { AppConfig } from '../../../tokens_manager/config/config';
import { configPaths } from '../../paths/paths';
import { fetchConfigJwtGenerator } from '../../../../libs/utils/createJwt';
import { executeConnectorCommand } from '../../../tokens_manager/utils/connector.utils';
import { HttpMethod } from '../../../../libs/enums/http-methods.enum';
import {
  ConnectorSyncBlock,
  buildCrawlingScheduleFromSync,
  isScheduledSyncStrategy,
} from '../../../crawling_manager/utils/schedule_config_mapper';

interface ScheduledConnectorRecord {
  connectorId: string;
  type: string;
  orgId: string;
  ownerUserId?: string | null;
  isActive: boolean;
  sync: ConnectorSyncBlock;
}

const MIGRATION_FLAG_DONE = 'true';

/** Page size sent to the Python all-scheduled endpoint. */
const BACKFILL_BATCH_SIZE = 50;

/** Exponential-backoff constants for waiting on the connector service. */
const BACKFILL_MAX_ATTEMPTS = 10;
const BACKFILL_INITIAL_DELAY_MS = 2_000;
const BACKFILL_MAX_DELAY_MS = 30_000;

/**
 * One-time backfill that schedules BullMQ crawling jobs for connectors
 * persisted with `sync.selectedStrategy = SCHEDULED` before the nodejs
 * API took ownership of scheduling. Idempotent in two ways:
 *  - a flag in the KV store skips repeated runs after a successful pass,
 *  - a per-connector `getJobStatus` check skips connectors that already
 *    have a job, so even if the flag write fails the migration is safe
 *    to re-run without duplicating jobs.
 *
 * If the connector service is unavailable at startup the migration retries
 * with exponential backoff (up to ~3 minutes). If the service never
 * becomes reachable the completion flag is NOT written, so the migration
 * will attempt again on the next process restart.
 */
export class ScheduledJobsBackfillMigration {
  private readonly backoffMaxAttempts: number;
  private readonly backoffInitialDelayMs: number;

  constructor(
    private readonly logger: Logger,
    private readonly kvStore: KeyValueStoreService,
    private readonly scheduler: CrawlingSchedulerService,
    private readonly appConfig: AppConfig,
    /**
     * Override the backoff knobs — primarily for testing so failure-path
     * tests don't wait minutes for real delays to elapse.
     */
    backoffOptions: { maxAttempts?: number; initialDelayMs?: number } = {},
  ) {
    this.backoffMaxAttempts = backoffOptions.maxAttempts ?? BACKFILL_MAX_ATTEMPTS;
    this.backoffInitialDelayMs = backoffOptions.initialDelayMs ?? BACKFILL_INITIAL_DELAY_MS;
  }

  async run(): Promise<void> {
    // Guard: skip if a previous successful run already set the flag.
    try {
      const flag = await this.kvStore.get<string>(
        configPaths.connectorSyncScheduledJobsMigration,
      );
      if (flag === MIGRATION_FLAG_DONE) {
        this.logger.info(
          'Connector-sync scheduled-jobs migration already completed; skipping',
        );
        return;
      }
    } catch (error) {
      this.logger.warn(
        'Failed to read migration flag; proceeding with idempotent run',
        { error: error instanceof Error ? error.message : 'Unknown error' },
      );
    }

    this.logger.info('Starting connector-sync scheduled-jobs backfill migration');

    // Fetch all SCHEDULED connectors with retries so a slow Python startup
    // does not permanently fail the migration on the first boot.
    const items = await this.fetchAllWithBackoff();
    if (items === null) {
      this.logger.warn(
        'Connector service did not become available within the retry window; ' +
          'migration deferred to next startup (flag NOT set)',
      );
      return;
    }

    let scheduled = 0;
    let skipped = 0;
    let errored = 0;

    for (const item of items) {
      const ctx = {
        connectorId: item.connectorId,
        type: item.type,
        orgId: item.orgId,
      };
      try {
        if (!item.isActive || !isScheduledSyncStrategy(item.sync)) {
          skipped++;
          continue;
        }

        // Second idempotency check: skip if BullMQ already has a job for
        // this connector (guards against a failed flag write on a prior run).
        const existing = await this.scheduler.getJobStatus(
          item.type,
          item.connectorId,
          item.orgId,
        );
        if (existing) {
          this.logger.debug(
            'Backfill skip: job already exists for connector',
            ctx,
          );
          skipped++;
          continue;
        }

        const ownerId =
          (typeof item.ownerUserId === 'string' && item.ownerUserId) ||
          'system';
        const schedule = buildCrawlingScheduleFromSync(item.sync, ownerId);
        if (!schedule) {
          this.logger.warn(
            'Backfill skip: SCHEDULED strategy with invalid scheduledConfig',
            { ...ctx, sync: item.sync?.scheduledConfig },
          );
          skipped++;
          continue;
        }

        await this.scheduler.scheduleJob(
          item.type,
          item.connectorId,
          schedule,
          item.orgId,
          ownerId,
        );
        scheduled++;
        this.logger.info('Backfill scheduled crawling job', ctx);
      } catch (err) {
        errored++;
        this.logger.error('Backfill failed for connector', {
          ...ctx,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    if (errored > 0) {
      // Do NOT write the completion flag when any connector failed.
      // The migration will re-run on next boot and retry the failed ones.
      // Connectors that already have a BullMQ job will be skipped by the
      // getJobStatus guard, so re-running is always safe.
      this.logger.warn(
        'Connector-sync scheduled-jobs backfill finished with errors; completion flag NOT written — will retry on next boot',
        { scheduled, skipped, errored, total: items.length },
      );
      return;
    }

    // All connectors processed without error — write the completion flag so
    // this migration does not run again on subsequent boots.
    try {
      await this.kvStore.set(
        configPaths.connectorSyncScheduledJobsMigration,
        MIGRATION_FLAG_DONE,
      );
      this.logger.info('Connector-sync scheduled-jobs backfill migration finished', {
        scheduled,
        skipped,
        errored,
        total: items.length,
      });
    } catch (error) {
      this.logger.error(
        'Failed to persist migration completion flag; migration will re-run on next boot',
        { error: error instanceof Error ? error.message : 'Unknown error' },
      );
    }
  }

  /**
   * Fetch all SCHEDULED connector records, retrying the entire operation with
   * exponential backoff if the connector service is unreachable.
   * Returns null only when all attempts are exhausted.
   */
  private async fetchAllWithBackoff(): Promise<ScheduledConnectorRecord[] | null> {
    let delay = this.backoffInitialDelayMs;

    for (let attempt = 1; attempt <= this.backoffMaxAttempts; attempt++) {
      try {
        const items = await this.fetchAllScheduledInBatches();
        this.logger.info('Connector service reachable; enumeration complete', {
          attempt,
          totalItems: items.length,
        });
        return items;
      } catch (error) {
        const isLastAttempt = attempt === this.backoffMaxAttempts;
        this.logger.warn(
          isLastAttempt
            ? 'Connector service unreachable after all retries'
            : 'Connector service unreachable; retrying with backoff',
          {
            attempt,
            maxAttempts: this.backoffMaxAttempts,
            retryInMs: isLastAttempt ? 0 : delay,
            error: error instanceof Error ? error.message : String(error),
          },
        );
        if (!isLastAttempt) {
          await new Promise<void>((resolve) => setTimeout(resolve, delay));
          delay = Math.min(delay * 2, BACKFILL_MAX_DELAY_MS);
        }
      }
    }

    return null;
  }

  /**
   * Page through all SCHEDULED connector records from the Python backend
   * in batches of `BACKFILL_BATCH_SIZE`. Throws on any service error so
   * `fetchAllWithBackoff` can retry the whole operation.
   */
  private async fetchAllScheduledInBatches(): Promise<ScheduledConnectorRecord[]> {
    const { connectorBackend, scopedJwtSecret } = this.appConfig;
    if (!connectorBackend) {
      throw new Error('connectorBackend URL is not configured');
    }
    if (!scopedJwtSecret) {
      throw new Error('scopedJwtSecret is not configured');
    }

    let token: string;
    try {
      token = fetchConfigJwtGenerator('system', 'system', scopedJwtSecret);
    } catch (error) {
      throw new Error(
        `Failed to mint scoped JWT: ${error instanceof Error ? error.message : 'Unknown'}`,
      );
    }

    const headers: Record<string, string> = {
      Authorization: `Bearer ${token}`,
      'X-Is-Admin': 'true',
    };

    const allItems: ScheduledConnectorRecord[] = [];
    let skip = 0;

    for (;;) {
      const url =
        `${connectorBackend}/api/v1/connectors/internal/all-scheduled` +
        `?skip=${skip}&limit=${BACKFILL_BATCH_SIZE}`;

      const resp = await executeConnectorCommand(url, HttpMethod.GET, headers);
      const status = resp?.statusCode;

      if (!status || status < 200 || status >= 300) {
        throw new Error(
          `Connector service returned non-2xx status ${status ?? '(no response)'}`,
        );
      }

      const data = resp.data as {
        items?: ScheduledConnectorRecord[];
        hasMore?: boolean;
      } | null;

      const batchItems = (data?.items ?? []) as ScheduledConnectorRecord[];
      allItems.push(...batchItems);

      this.logger.debug('Fetched backfill batch', {
        skip,
        batchSize: batchItems.length,
        hasMore: data?.hasMore,
        runningTotal: allItems.length,
      });

      if (!data?.hasMore) break;
      skip += BACKFILL_BATCH_SIZE;
    }

    return allItems;
  }
}
