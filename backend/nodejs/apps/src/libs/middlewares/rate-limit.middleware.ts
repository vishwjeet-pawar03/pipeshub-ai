import { Request, Response, RequestHandler } from 'express';
import rateLimit, { Options } from 'express-rate-limit';
import { Logger } from '../services/logger.service';
import { TooManyRequestsError } from '../errors/http.errors';
import { AuthenticatedUserRequest, AuthenticatedServiceRequest } from './types';

/**
 * Get client IP address from request
 */
function getClientIp(req: Request): string {
  const forwarded = req.headers['x-forwarded-for'];
  if (forwarded) {
    const forwardedValue = typeof forwarded === 'string' ? forwarded : forwarded[0];
    if (forwardedValue) {
      const ips = forwardedValue.split(',');
      const firstIp = ips[0];
      if (firstIp) {
        return firstIp.trim();
      }
    }
  }
  const realIp = req.headers['x-real-ip'];
  if (realIp) {
    const realIpValue = typeof realIp === 'string' ? realIp : realIp[0];
    if (realIpValue) {
      return realIpValue;
    }
  }
  return req.ip || req.socket.remoteAddress || 'unknown';
}

/**
 * Strict check that /internal is a literal route segment at an expected depth,
 * not a user-supplied parameter value.
 */
function isInternalRoute(path: string): boolean {
  const segments = path.split('/');
  // Pattern: /api/v1/<module>/internal/<sub-path>
  // segments: ['', 'api', 'v1', module, 'internal', sub, ...]
  if (segments[4] === 'internal' && segments.length > 5) {
    return true;
  }
  // Nested pattern: /api/v1/<module>/<param>/<sub-module>/internal/<sub-path>
  // e.g. /api/v1/agents/:agentKey/conversations/internal/stream
  if (segments[6] === 'internal' && segments.length > 7) {
    return true;
  }
  return false;
}

// Single global rate limiter
export function createGlobalRateLimiter(logger: Logger, maxRequestsPerMinute: number): RequestHandler {
  const config: Partial<Options> = {
    windowMs: 60 * 1000,
    max: maxRequestsPerMinute,
    standardHeaders: true,
    legacyHeaders: false,

    keyGenerator: (req: Request): string => {
      const authenticatedUserReq = req as AuthenticatedUserRequest;
      const authenticatedServiceReq = req as AuthenticatedServiceRequest;

      if (authenticatedUserReq.user?.userId) {
        return `user:${authenticatedUserReq.user.userId}`;
      }
      if (authenticatedServiceReq.tokenPayload?.orgId) {
        return `org:${authenticatedServiceReq.tokenPayload.orgId}`;
      }
      const ip = getClientIp(req);
      return `ip:${ip}`;
    },

    skip: (req: Request): boolean => {
      // Internal routes are service-to-service calls 
      if (isInternalRoute(req.path)) {
        return true;
      }
      
      const authenticatedServiceReq = req as AuthenticatedServiceRequest;
      if (authenticatedServiceReq.tokenPayload) {
        logger.debug('Skipping rate limit for service request', {
          orgId: authenticatedServiceReq.tokenPayload.orgId,
          userId: authenticatedServiceReq.tokenPayload.userId,
        });
        return true;
      }
      return false;
    },

    handler: (req: Request, res: Response): void => {
      const retryAfter = res.getHeader('Retry-After');
      const rateLimitKey = getRateLimitKey(req);

      logger.warn('Rate limit exceeded', {
        key: rateLimitKey,
        path: req.path,
        method: req.method,
        ip: getClientIp(req),
        retryAfter,
      });

      const error = new TooManyRequestsError('Too many requests. Please try again later.');
      res.status(429).json({
        error: {
          code: error.code,
          message: error.message,
          retryAfter: retryAfter ? parseInt(retryAfter as string, 10) : null,
        },
      });
    },
  };

  function getRateLimitKey(req: Request): string {
    const authenticatedUserReq = req as AuthenticatedUserRequest;
    const authenticatedServiceReq = req as AuthenticatedServiceRequest;
    if (authenticatedUserReq.user?.userId) {
      return `user:${authenticatedUserReq.user.userId}`;
    }
    if (authenticatedServiceReq.tokenPayload?.orgId) {
      return `org:${authenticatedServiceReq.tokenPayload.orgId}`;
    }
    return `ip:${getClientIp(req)}`;
  }

  return rateLimit(config);
}

/**
 * Rate limiter for OAuth client management endpoints
 * Stricter limits: 10 requests per minute per user/IP
 * Used for creating, updating, and deleting OAuth applications
 */
export function createOAuthClientRateLimiter(logger: Logger, maxRequestsPerMinute: number): RequestHandler {
  const config: Partial<Options> = {
    windowMs: 60 * 1000, // 1 minute
    max: maxRequestsPerMinute,
    standardHeaders: true,
    legacyHeaders: false,

    keyGenerator: (req: Request): string => {
      const authenticatedUserReq = req as AuthenticatedUserRequest;

      if (authenticatedUserReq.user?.userId) {
        return `oauth-client:user:${authenticatedUserReq.user.userId}`;
      }
      const ip = getClientIp(req);
      return `oauth-client:ip:${ip}`;
    },

    handler: (req: Request, res: Response): void => {
      const retryAfter = res.getHeader('Retry-After');
      const authenticatedUserReq = req as AuthenticatedUserRequest;
      const key = authenticatedUserReq.user?.userId
        ? `oauth-client:user:${authenticatedUserReq.user.userId}`
        : `oauth-client:ip:${getClientIp(req)}`;

      logger.warn('OAuth client rate limit exceeded', {
        key,
        path: req.path,
        method: req.method,
        ip: getClientIp(req),
        retryAfter,
      });

      const error = new TooManyRequestsError(
        'Too many OAuth client requests. Please try again later.',
      );
      res.status(429).json({
        error: {
          code: error.code,
          message: error.message,
          retryAfter: retryAfter ? parseInt(retryAfter as string, 10) : null,
        },
      });
    },
  };

  return rateLimit(config);
}