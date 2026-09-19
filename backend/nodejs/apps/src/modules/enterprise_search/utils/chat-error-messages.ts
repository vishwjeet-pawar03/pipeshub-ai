import { HttpError } from '../../../libs/errors/http.errors';

/**
 * What a person reads in the chat when an answer fails. These are also saved
 * into the conversation, so they must never carry raw error text; the cause
 * belongs in the logs.
 */
export const CHAT_ERROR_MESSAGES = {
  interrupted:
    'The answer was interrupted before it finished. Click Regenerate or send your message again.',
  saveFailed:
    "The answer couldn't be saved to this conversation. Click Regenerate or send your message again.",
  unavailable: "PipesHub couldn't answer right now. Please try again in a minute.",
  failed:
    'Something went wrong while answering. Please send your message again, and if it keeps happening, contact your workspace admin.',
} as const;

// The service answering the chat can't be reached at all.
const UNREACHABLE_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN']);
// The connection broke or stalled part-way through an answer.
const INTERRUPTED_CODES = new Set([
  'ECONNRESET',
  'EPIPE',
  'ETIMEDOUT',
  'ECONNABORTED',
  'UND_ERR_SOCKET',
  'UND_ERR_BODY_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_ABORTED',
  'ERR_STREAM_PREMATURE_CLOSE',
]);
const INTERRUPTED_TEXT =
  /\b(terminated|socket hang up|premature close|other side closed|aborted|timed? ?out|econnreset|etimedout|epipe)\b/i;

interface ErrorLike {
  message?: unknown;
  code?: unknown;
  cause?: { code?: unknown } | null;
}

const errorCodes = (error: ErrorLike): string[] =>
  [error.code, error.cause?.code].filter((code): code is string => typeof code === 'string');

/**
 * Turns whatever made a chat answer fail into a message fit for the user.
 * A deliberate 4xx from our own API (bad input, no access, no model set up)
 * already speaks to the user and is kept; everything else maps to a fixed
 * message with a next step.
 */
export const userFacingChatError = (error: unknown): string => {
  if (error instanceof HttpError) {
    return userFacingStatusError(error.statusCode, error.message);
  }
  if (error && typeof error === 'object') {
    const err = error as ErrorLike;
    const codes = errorCodes(err);
    if (codes.some((code) => UNREACHABLE_CODES.has(code))) {
      return CHAT_ERROR_MESSAGES.unavailable;
    }
    const message = typeof err.message === 'string' ? err.message : '';
    if (codes.some((code) => INTERRUPTED_CODES.has(code)) || INTERRUPTED_TEXT.test(message)) {
      return CHAT_ERROR_MESSAGES.interrupted;
    }
  }
  return CHAT_ERROR_MESSAGES.failed;
};

/** Same rule for a failed response we only have a status and message for. */
export const userFacingStatusError = (statusCode: number | undefined, message?: string): string => {
  if (statusCode !== undefined && statusCode < 500 && message?.trim()) {
    return message;
  }
  if (statusCode !== undefined && [502, 503, 504].includes(statusCode)) {
    return CHAT_ERROR_MESSAGES.unavailable;
  }
  return CHAT_ERROR_MESSAGES.failed;
};
