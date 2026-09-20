/**
 * Whether a 5xx message may be repeated to the person who made the request.
 *
 * The rule is an allowlist, not a filter: a service's own words are kept only
 * when we know a person wrote them, because guessing from shape lets anything
 * unlisted through — `connector-service connection refused at 10.0.0.4` reads
 * like a sentence and still describes our topology. Everything else is replaced
 * with a plain line and logged.
 *
 * Two ways to be on the list:
 *
 * - an error we build ourselves carries `clientSafe: true` in its metadata;
 * - a message from another service matches one we know was written for a
 *   reader, named below.
 */

/** Metadata flag on errors whose message we wrote for the person. */
export const CLIENT_SAFE = 'clientSafe';

/**
 * Messages another PipesHub service sends that were written for a reader.
 * Each entry names where it comes from, so a wording change there shows up as
 * a failing test here rather than as a mystery.
 */
const READER_WRITTEN_MESSAGES: ReadonlySet<string> = new Set([
  // backend/python/app/api/middlewares/auth.py — the 503 when Node can't be
  // reached to check an access token.
  "We couldn't confirm your sign-in just now. Please try again in a few seconds.",
]);

const normalise = (text: string): string => text.trim().replace(/\s+/g, ' ');

/**
 * True when this exact message is one a person wrote. An unknown message is
 * not client-safe, however readable it looks.
 */
export const isReaderWritten = (text: string | undefined): boolean =>
  typeof text === 'string' && READER_WRITTEN_MESSAGES.has(normalise(text));

/** True when we built this error and marked its message for the reader. */
export const isMarkedClientSafe = (metadata: unknown): boolean =>
  typeof metadata === 'object' &&
  metadata !== null &&
  (metadata as Record<string, unknown>)[CLIENT_SAFE] === true;
