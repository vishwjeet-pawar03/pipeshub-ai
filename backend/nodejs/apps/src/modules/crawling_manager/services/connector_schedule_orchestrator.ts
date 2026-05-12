import { Logger } from '../../../libs/services/logger.service';
import { CrawlingSchedulerService } from './crawling_service';
import {
  ConnectorSyncBlock,
  buildCrawlingScheduleFromSync,
  isScheduledSyncStrategy,
} from '../utils/schedule_config_mapper';

export type ScheduleReconcileOutcome =
  | 'scheduled'
  | 'removed'
  | 'noop'
  | 'skipped';

export interface ScheduleReconcileInput {
  connector: string; // connector type, e.g. 'Confluence'
  connectorId: string;
  orgId: string;
  userId: string;
  isActive: boolean;
  sync: ConnectorSyncBlock | null | undefined;
}

/**
 * Reconcile the BullMQ crawling job for a connector against its saved
 * sync config. Idempotent — `scheduleJob` removes any existing job before
 * creating a new one, and we explicitly remove on disable / non-SCHEDULED.
 */
export const reconcileConnectorSchedule = async (
  scheduler: CrawlingSchedulerService,
  logger: Logger,
  input: ScheduleReconcileInput,
): Promise<ScheduleReconcileOutcome> => {
  const { connector, connectorId, orgId, userId, isActive, sync } = input;
  const ctx = { connector, connectorId, orgId };

  if (!connector || !connectorId || !orgId || !userId) {
    logger.warn('reconcileConnectorSchedule called with missing identifiers', ctx);
    return 'skipped';
  }

  const wantsSchedule = isActive && isScheduledSyncStrategy(sync);

  if (!wantsSchedule) {
    // Make sure no orphaned job lingers when disabled or strategy != SCHEDULED.
    try {
      const existing = await scheduler.getJobStatus(connector, connectorId, orgId);
      if (!existing) return 'noop';
      await scheduler.removeJob(connector, connectorId, orgId);
      logger.info('Removed crawling job (connector disabled or non-SCHEDULED strategy)', ctx);
      return 'removed';
    } catch (error) {
      logger.error('Failed to remove crawling job during reconcile', {
        ...ctx,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  const schedule = buildCrawlingScheduleFromSync(sync, userId);
  if (!schedule) {
    logger.warn(
      'SCHEDULED strategy selected but scheduledConfig is invalid; skipping',
      { ...ctx, sync: sync?.scheduledConfig },
    );
    return 'skipped';
  }

  try {
    await scheduler.scheduleJob(
      connector,
      connectorId,
      schedule,
      orgId,
      userId,
    );
    logger.info('Scheduled crawling job from connector sync config', ctx);
    return 'scheduled';
  } catch (error) {
    logger.error('Failed to schedule crawling job during reconcile', {
      ...ctx,
      error: error instanceof Error ? error.message : 'Unknown error',
    });
    throw error;
  }
};

