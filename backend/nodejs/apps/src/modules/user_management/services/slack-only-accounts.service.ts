import mongoose from 'mongoose';
import { ForbiddenError } from '../../../libs/errors/http.errors';
import { userActivitiesType } from '../../../libs/utils/userActivities.utils';
import { UserActivities } from '../../auth/schema/userActivities.schema';
import { UserCredentials } from '../../auth/schema/userCredentials.schema';
import { User, Users } from '../schema/users.schema';

/**
 * Slack-only accounts (`kind: 'slack'`).
 *
 * A Slack bot whose agent is a service agent can, when its admin turns on
 * `createAccountsForSlackUsers`, give each full member of its own Slack
 * workspace an account the first time they message it. That account can do
 * one thing: chat with the bot's service agent from Slack. It has no password,
 * is in no group (not even everyone), gets no permission-graph user and can
 * never sign in or hold a token of its own, until an admin invites it, adds it
 * as a user, or the person signs in with SSO while just-in-time provisioning
 * is on. Each of those upgrades the same record, so the `_id` the Slack
 * threads belong to never changes.
 */

/** Shown in Slack and on sign-in screens to a person who only has a Slack account. */
export const SLACK_ONLY_INVITE_MESSAGE =
  'Ask your admin to invite you to PipesHub.';

export const SLACK_ONLY_SIGN_IN_MESSAGE =
  'This account was created from Slack and can only be used there. Ask your admin to invite you to PipesHub.';

export const SLACK_ACCOUNT_UNAVAILABLE_MESSAGE =
  'Your PipesHub account was removed or switched off, so this bot cannot answer you. Ask your admin for help.';

/** What the Slack bot signed into its token about the person writing. */
export interface SlackMemberClaims {
  slackUserId: string;
  slackTeamId: string;
  fullName?: string;
}

const nonEmptyString = (value: unknown): value is string =>
  typeof value === 'string' && value.trim() !== '';

/**
 * The bot's signed statement that this person is a full member of its own
 * workspace, or null. The bot sets `slackFullMember` only after `users.info`
 * showed a person who is not a bot, not deactivated, not a guest, not from
 * another workspace, and has a confirmed email (see the bot's
 * `isSlackFullMemberOfTeam`). Anything less is not eligible.
 */
export const slackMemberClaims = (
  payload: Record<string, unknown> | undefined,
): SlackMemberClaims | null => {
  if (payload?.slackFullMember !== true) return null;
  const { slackUserId, slackTeamId, slackFullName } = payload;
  if (!nonEmptyString(slackUserId) || !nonEmptyString(slackTeamId)) {
    return null;
  }
  return {
    slackUserId,
    slackTeamId,
    ...(nonEmptyString(slackFullName) ? { fullName: slackFullName.trim() } : {}),
  };
};

/**
 * Throws unless a Slack-only account may still be used: a disabled or blocked
 * record is never brought back to life by a Slack message.
 */
export const assertSlackOnlyAccountActive = async (user: {
  _id: unknown;
  orgId: unknown;
  isDisabled?: boolean;
  isDeleted?: boolean;
}): Promise<void> => {
  if (user.isDeleted === true || user.isDisabled === true) {
    throw new ForbiddenError(SLACK_ACCOUNT_UNAVAILABLE_MESSAGE);
  }
  const blocked = await UserCredentials.exists({
    userId: String(user._id),
    orgId: String(user.orgId),
    isBlocked: true,
    isDeleted: false,
  });
  if (blocked) {
    throw new ForbiddenError(SLACK_ACCOUNT_UNAVAILABLE_MESSAGE);
  }
};

const isDuplicateKeyError = (error: unknown): boolean =>
  (error as { code?: number } | null | undefined)?.code === 11000;

/**
 * The account for this email in this org: an existing one, or a new
 * Slack-only one. Never revives anything: a deleted, disabled or blocked
 * record for the email refuses the turn instead.
 *
 * Two messages arriving at once both try to insert. The unique email index
 * lets one win; the other reads the winner back, so there is one account.
 */
export const findOrCreateSlackOnlyAccount = async (params: {
  orgId: string;
  email: string;
  claims: SlackMemberClaims;
  configId: string;
}): Promise<User> => {
  const email = params.email.toLowerCase().trim();
  const usable = async (existing: User | null): Promise<User | null> => {
    if (!existing) return null;
    if (String(existing.orgId) !== params.orgId) {
      throw new ForbiddenError(SLACK_ONLY_INVITE_MESSAGE);
    }
    await assertSlackOnlyAccountActive(existing);
    return existing;
  };

  const found = await usable(await Users.findOne({ email, orgId: params.orgId }));
  if (found) return found;

  let created: User;
  try {
    created = await Users.create({
      orgId: new mongoose.Types.ObjectId(params.orgId),
      email,
      fullName: params.claims.fullName,
      kind: 'slack',
      role: 'member',
      hasLoggedIn: false,
      slackUserId: params.claims.slackUserId,
      slackTeamId: params.claims.slackTeamId,
    });
  } catch (error) {
    if (!isDuplicateKeyError(error)) throw error;
    // Another message won the race (or the address is taken elsewhere).
    const winner = await usable(
      await Users.findOne({ email, orgId: params.orgId }),
    );
    if (winner) return winner;
    throw new ForbiddenError(SLACK_ONLY_INVITE_MESSAGE);
  }

  await UserActivities.create({
    orgId: created.orgId,
    userId: created._id,
    email,
    activityType: userActivitiesType.ACCOUNT_CREATED_FROM_SLACK,
    ipAddress: 'system',
    details: `created from Slack bot ${params.configId}`,
  });
  return created;
};

/**
 * Turns a Slack-only account into an ordinary one, keeping its `_id`. Returns
 * whether this call did it, so of two concurrent upgrades only one goes on to
 * add the groups and publish the new-user event.
 */
export const upgradeSlackOnlyAccount = async (
  userId: unknown,
  orgId: unknown,
): Promise<boolean> => {
  const result = await Users.updateOne(
    { _id: String(userId), orgId: String(orgId), kind: 'slack' },
    { $set: { kind: 'human' } },
  );
  return result.modifiedCount === 1;
};
