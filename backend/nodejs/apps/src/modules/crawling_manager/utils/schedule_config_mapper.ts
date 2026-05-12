import { Types } from 'mongoose';
import { CrawlingScheduleType } from '../schema/enums';
import { ICrawlingSchedule } from '../schema/interface';

export const SCHEDULED_STRATEGY = 'SCHEDULED';

export interface ConnectorScheduledConfigBlock {
  intervalMinutes?: number;
  timezone?: string;
}

export interface ConnectorSyncBlock {
  selectedStrategy?: string;
  customValues?: Record<string, unknown>;
  scheduledConfig?: ConnectorScheduledConfigBlock;
  [key: string]: unknown;
}

export const isScheduledSyncStrategy = (
  sync: ConnectorSyncBlock | null | undefined,
): boolean =>
  !!sync &&
  typeof sync.selectedStrategy === 'string' &&
  sync.selectedStrategy.toUpperCase() === SCHEDULED_STRATEGY;

const toObjectId = (userId: string): Types.ObjectId => {
  if (Types.ObjectId.isValid(userId)) {
    return new Types.ObjectId(userId);
  }
  // Fall back to a deterministic non-throwing id for non-ObjectId user
  // identifiers (e.g. service users); the schema accepts ObjectId but
  // BullMQ never persists this field, so the value is informational.
  return new Types.ObjectId();
};

/**
 * Convert a connector's saved `sync` block to an ICrawlingSchedule.
 * Returns `null` when the connector isn't configured for SCHEDULED sync,
 * or when the schedule values are missing/invalid.
 */
export const buildCrawlingScheduleFromSync = (
  sync: ConnectorSyncBlock | null | undefined,
  ownerUserId: string,
): ICrawlingSchedule | null => {
  if (!isScheduledSyncStrategy(sync)) return null;

  const scheduled = sync!.scheduledConfig;
  const intervalMinutes = Number(scheduled?.intervalMinutes);
  if (!Number.isFinite(intervalMinutes) || intervalMinutes < 1) return null;

  const timezone =
    typeof scheduled?.timezone === 'string' && scheduled.timezone.trim().length
      ? scheduled.timezone
      : 'UTC';

  const ownerObjectId = toObjectId(ownerUserId);

  return {
    scheduleType: CrawlingScheduleType.INTERVAL,
    isEnabled: true,
    createdBy: ownerObjectId,
    lastUpdatedBy: ownerObjectId,
    scheduleConfig: {
      intervalMinutes: Math.floor(intervalMinutes),
      timezone,
    },
  };
};
