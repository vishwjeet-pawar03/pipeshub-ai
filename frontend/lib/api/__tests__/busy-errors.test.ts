import { describe, it, expect } from 'vitest';
import { AxiosError, AxiosHeaders } from 'axios';
import { busyMessage, processError } from '../api-error';

function httpError(status: number, data: unknown, headers: Record<string, string> = {}) {
  const error = new AxiosError(`Request failed with status code ${status}`);
  error.response = {
    status,
    statusText: '',
    data,
    headers: new AxiosHeaders(headers),
    config: { headers: new AxiosHeaders() },
  } as never;
  return error as AxiosError<never>;
}

describe('busy and slow responses', () => {
  it.each([429, 503, 504])('%i with no message reads as busy, never as axios text', (status) => {
    const processed = processError(httpError(status, {}));
    expect(processed.message).toBe(busyMessage());
    expect(processed.message).not.toMatch(/status code/);
  });

  it('uses the Retry-After seconds when the server sends them', () => {
    const processed = processError(httpError(503, {}, { 'retry-after': '5' }));
    expect(processed.message).toBe('PipesHub is busy right now. Please try again in 5 seconds.');
  });

  it("prefers the server's own words", () => {
    const processed = processError(
      httpError(503, { error: { message: "We couldn't confirm your sign-in just now." } }),
    );
    expect(processed.message).toBe("We couldn't confirm your sign-in just now.");
  });
});
