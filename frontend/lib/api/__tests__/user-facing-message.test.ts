import { describe, it, expect } from 'vitest';
import { AxiosError, AxiosHeaders } from 'axios';
import { ErrorType, getUserFacingErrorMessage, processError } from '../api-error';

const FALLBACK = 'We couldn\'t do that. Please try again in a moment.';

function httpError(status: number, data: unknown) {
  const error = new AxiosError(`Request failed with status code ${status}`);
  error.response = {
    status,
    statusText: '',
    data,
    headers: new AxiosHeaders(),
    config: { headers: new AxiosHeaders() },
  } as never;
  return error as AxiosError<never>;
}

describe('getUserFacingErrorMessage', () => {
  it("keeps the server's words when they were written for a reader", () => {
    const message = 'This collection was removed. Refresh the page to see the current list.';
    expect(getUserFacingErrorMessage(new Error(message), FALLBACK)).toBe(message);
  });

  it.each([
    ['axios wording', 'Request failed with status code 500'],
    ['a Python repr', "KeyError: 'llm'"],
    ['a traceback', 'Traceback (most recent call last): ...'],
    ['an object that never stringified', 'Backend error: [object Object]'],
    ['an internal service name', 'Error publishing to Kafka topic records'],
    ['a socket code', 'read ECONNRESET'],
    ['a record id', 'Record 65f1c2ab9e4d7a3b1c0d8e2f could not be read'],
  ])('replaces %s with the caller\'s fallback', (_label, message) => {
    expect(getUserFacingErrorMessage(new Error(message), FALLBACK)).toBe(FALLBACK);
  });

  it('falls back when there is no message at all', () => {
    expect(getUserFacingErrorMessage(undefined, FALLBACK)).toBe(FALLBACK);
    expect(getUserFacingErrorMessage({ message: '   ' }, FALLBACK)).toBe(FALLBACK);
  });

  it('reads a processed API error', () => {
    const processed = processError(httpError(403, { reason: 'You can only share collections you own.' }));
    expect(processed.type).toBe(ErrorType.AUTHORIZATION_ERROR);
    expect(getUserFacingErrorMessage(processed, FALLBACK)).toBe(
      'You can only share collections you own.',
    );
  });

  it('never surfaces axios text for a 500 with an empty body', () => {
    const processed = processError(httpError(500, {}));
    expect(processed.message).not.toMatch(/status code/);
    expect(getUserFacingErrorMessage(processed, FALLBACK)).toBe(
      'Server error. Please try again later.',
    );
  });
});
