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
 * - we built the error ourselves and marked it as we created it;
 * - a message from another service matches one we know was written for a
 *   reader, named below.
 */

/**
 * Errors whose message this codebase wrote for the person who made the request.
 * A set rather than a flag on the error: nothing arriving from another service
 * can put itself in here, and it keeps the marker out of the metadata we echo
 * back in development.
 */
const clientSafeErrors = new WeakSet<Error>();

/** Records that we wrote this error's message for the reader. */
export const markClientSafe = <T extends Error>(error: T): T => {
  clientSafeErrors.add(error);
  return error;
};

/** True when we built this error and wrote its message for the reader. */
export const isClientSafeError = (error: Error): boolean =>
  clientSafeErrors.has(error);

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
