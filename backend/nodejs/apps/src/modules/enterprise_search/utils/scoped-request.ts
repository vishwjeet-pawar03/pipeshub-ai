import * as crypto from 'crypto';
import { Types } from 'mongoose';
import { AIServiceCommand } from '../../../libs/commands/ai_service/ai.service.command';
import { HttpMethod } from '../../../libs/enums/http-methods.enum';
import { AIServiceResponse } from '../types/conversation.interfaces';
import { TokenScopes } from '../../../libs/enums/token-scopes.enum';
import {
  InternalServerError,
  NotFoundError,
  UnauthorizedError,
} from '../../../libs/errors/http.errors';
import {
  AuthenticatedServiceRequest,
  AuthenticatedUserRequest,
} from '../../../libs/middlewares/types';
import { AuthTokenService } from '../../../libs/services/authtoken.service';
import { KeyValueStoreService } from '../../../libs/services/keyValueStore.service';
import { Logger } from '../../../libs/services/logger.service';
import { getSlackBotStore } from '../../configuration_manager/controller/cm_controller';
import { AppConfig } from '../../tokens_manager/config/config';
import { Org } from '../../user_management/schema/org.schema';
import { Users } from '../../user_management/schema/users.schema';

/**
 * Turns a scoped service token (`/internal/...` routes: Slack bot, service
 * accounts) into the user request the chat handlers expect.
 */

const logger = Logger.getInstance({ service: 'Enterprise Search Service' });

export const STAND_IN_TOKEN_TTL_SECONDS = 60;

/**
 * The OSS Slack bot configuration carries no orgId, so this edition only has a
 * safe answer when there is exactly one org. On a multi-org instance the old
 * `Org.findOne` picked whichever document came back first, which could answer a
 * Slack workspace with another tenant's data — refuse instead. Multi-org Slack
 * is an EE feature and resolves the org from the bot config's own orgId.
 */
const resolveSoleOrgId = async (): Promise<string> => {
  const orgs = await Org.find({ isDeleted: false }, { _id: 1 }).limit(2).lean().exec();
  const soleOrg = orgs.length === 1 ? orgs[0] : undefined;
  if (!soleOrg?._id) {
    if (orgs.length > 1) {
      throw new UnauthorizedError(
        'Slack access is limited to single-organization deployments on this edition',
      );
    }
    throw new NotFoundError('Organization not found');
  }
  return String(soleOrg._id);
};

/** 24-char hex suitable for Mongo ObjectId; stable per email for Slack/service-account callers without a User row. */
export const stableObjectIdHexForExternalEmail = (email: string): string =>
  crypto
    .createHash('sha256')
    .update(`slack-service-account:${email.toLowerCase().trim()}`)
    .digest('hex')
    .slice(0, 24);

export const hydrateScopedRequestAsUser = async (
  req: AuthenticatedServiceRequest | AuthenticatedUserRequest,
  appConfig: AppConfig,
  keyValueStoreService?: KeyValueStoreService,
): Promise<void> => {
  const existingUser = (req as AuthenticatedUserRequest).user;
  if (existingUser?.userId && existingUser?.orgId) {
    return;
  }

  const email: unknown = (req as AuthenticatedServiceRequest).tokenPayload
    ?.email;
  if (typeof email !== 'string' || email === '') {
    throw new UnauthorizedError('Email not found in scoped token');
  }

  const orgId = await resolveSoleOrgId();

  const user = await Users.findOne({
    email,
    orgId,
    isDeleted: false,
  });

  const authTokenService = new AuthTokenService(
    appConfig.jwtSecret,
    appConfig.scopedJwtSecret,
  );

  if (!user) {
    const { agentKey } = req.params;
    if (agentKey && keyValueStoreService) {
      const store = await getSlackBotStore(keyValueStoreService);
      const configs = store.configs;
      for (const config of configs) {
        if (config.agentId === agentKey) {
          const stableUserIdHex = stableObjectIdHexForExternalEmail(email);
          // The incoming Slack token carries neither orgId nor userId, and the
          // AI service needs both to look the agent up in this org.
          const checkToken = authTokenService.generateScopedToken(
            {
              userId: stableUserIdHex,
              orgId,
              email,
              scopes: [TokenScopes.CONVERSATION_CREATE],
            },
            STAND_IN_TOKEN_TTL_SECONDS,
          );
          const isServiceAccount = await checkServiceAccountAccess(
            req,
            appConfig,
            checkToken,
          );
          if (isServiceAccount) {
            const org = await Org.findOne({ _id: orgId, isDeleted: false });
            if (!org?._id) {
              throw new NotFoundError('Organization not found');
            }
            const scopedJwtToken = authTokenService.generateScopedToken(
              {
                userId: stableUserIdHex,
                orgId: org._id,
                email: email,
                scopes: [TokenScopes.CONVERSATION_CREATE],
                isServiceAccount: true,
              },
              '1h',
            );
            (req as AuthenticatedServiceRequest).headers.authorization =
              `Bearer ${scopedJwtToken}`;
            (req as AuthenticatedUserRequest).user = {
              userId: new Types.ObjectId(stableUserIdHex),
              orgId: org._id,
              email: email,
              scopes: [TokenScopes.CONVERSATION_CREATE],
              isServiceAccount: true,
            };
            return;
          }
        }
      }
    }
    throw new NotFoundError(
      'User not found, create an account on the Pipeshub platform first.',
    );
  }

  // The Python services ask Node whether every session-style token is still
  // live, and Node refuses one without a role claim. Member is what this token
  // has always been treated as there, so it stays member. With a role Node
  // honours it like a session, so it lives only as long as the request that
  // minted it needs to reach the AI service, not generateToken's default week.
  const jwtToken = authTokenService.generateToken(
    {
      userId: user._id,
      orgId: user.orgId,
      email: user.email,
      fullName: user.fullName,
      mobile: user.mobile,
      userSlug: user.slug,
      role: 'member',
    },
    STAND_IN_TOKEN_TTL_SECONDS,
  );

  req.headers.authorization = `Bearer ${jwtToken}`;

  (req as AuthenticatedUserRequest).user = {
    userId: user._id,
    orgId: user.orgId,
    email: user.email,
    fullName: user.fullName,
    mobile: user.mobile,
    userSlug: user.slug,
  };
};

/**
 * Asks the AI service whether `agentKey` is a service agent in the org named
 * by `token`. Only that token and the request id travel, never the caller's
 * own headers. 403 (not a service agent) and 404 (no such agent here) are a
 * plain "no"; any other failure is a server error, so a broken check is not
 * reported to the user as a missing account.
 */
export const checkServiceAccountAccess = async (
  req: AuthenticatedServiceRequest,
  appConfig: AppConfig,
  token: string,
): Promise<boolean> => {
  const requestId = req.context?.requestId;
  const agentKey = req.params.agentKey as string;
  const failure = new InternalServerError(
    'Could not check whether this agent is a service agent',
  );
  let aiResponse: AIServiceResponse<{ isServiceAccount?: boolean }>;
  try {
    aiResponse = await new AIServiceCommand<{ isServiceAccount?: boolean }>({
      uri: `${appConfig.aiBackend}/api/v1/agent/${encodeURIComponent(agentKey)}/internal/service-account`,
      method: HttpMethod.GET,
      headers: { Authorization: `Bearer ${token}` },
    }).execute();
  } catch (error) {
    logger.error('Service agent check failed', {
      requestId,
      error: error instanceof Error ? error.message : String(error),
    });
    throw failure;
  }
  if (aiResponse.statusCode === 200) {
    return aiResponse.data?.isServiceAccount === true;
  }
  if (aiResponse.statusCode === 403 || aiResponse.statusCode === 404) {
    return false;
  }
  logger.error('Service agent check failed', {
    requestId,
    statusCode: aiResponse.statusCode,
  });
  throw failure;
};
