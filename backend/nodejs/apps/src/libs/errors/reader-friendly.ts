/**
 * Whether a message was written for the person who made the request, rather
 * than about the machine that failed. Used before repeating anything a service
 * said, so our internals don't travel to a screen.
 */
const INTERNAL_TEXT_PATTERNS: RegExp[] = [
  /traceback/i,
  /^[A-Za-z_]*(Error|Exception)\b/,
  /\b(KeyError|TypeError|ValueError|AttributeError|NoneType)\b/,
  /\[object object\]/i,
  /\b(kafka|redis|mongodb|qdrant|arangodb|neo4j|etcd)\b/i,
  /ECONNREFUSED|ECONNRESET|ENOTFOUND|socket hang up/i,
  /request failed with status code/i,
];

export const isReaderFriendly = (text: string | undefined): boolean =>
  typeof text === 'string' &&
  text.trim().length > 0 &&
  !INTERNAL_TEXT_PATTERNS.some((pattern) => pattern.test(text));
