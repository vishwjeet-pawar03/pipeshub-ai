// Query values that grant access on their own: an OAuth callback's code and
// state, and any token or secret a caller put in a URL.
const SENSITIVE_QUERY_PARAMS = new Set([
  'code',
  'state',
  'token',
  'access_token',
  'refresh_token',
  'id_token',
  'client_secret',
]);

/** The URL with sensitive query values replaced, for writing to logs. */
export const logSafeUrl = (url: string): string => {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return url.split('?')[0] ?? '';
  }
  for (const key of Array.from(parsed.searchParams.keys())) {
    if (SENSITIVE_QUERY_PARAMS.has(key.toLowerCase())) {
      parsed.searchParams.set(key, 'REDACTED');
    }
  }
  return parsed.toString();
};
