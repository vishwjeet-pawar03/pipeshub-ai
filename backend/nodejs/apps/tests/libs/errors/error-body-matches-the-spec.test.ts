import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import { readFileSync } from 'fs'
import { join } from 'path'
import yaml from 'js-yaml'
import type { NextFunction, Request, Response } from 'express'
import { ErrorMiddleware } from '../../../src/libs/middlewares/error.middleware'
import { UnauthorizedError } from '../../../src/libs/errors/http.errors'

/**
 * What the middleware sends must be what the published spec promises.
 *
 * The error schema sets `additionalProperties: false`, so a field added to the
 * body without being added to the spec makes every response-validation test
 * fail at once - which is how `requestId` took out 127 of them, and only on the
 * nightly, hours after it merged. This checks the same thing on every run.
 */

const SPEC = join(
  __dirname, '..', '..', '..', 'src', 'modules', 'api-docs', 'pipeshub-openapi.yaml',
)

interface ErrorSchema {
  additionalProperties?: boolean
  required?: string[]
  properties?: Record<string, unknown>
}

function errorPayloadSchema(): ErrorSchema {
  const spec = yaml.load(readFileSync(SPEC, 'utf8')) as {
    components: { schemas: Record<string, { properties: { error: ErrorSchema } }> }
  }
  return spec.components.schemas.ErrorResponse.properties.error
}

function bodyFromMiddleware(error: Error, requestId?: string): Record<string, unknown> {
  let body: { error: Record<string, unknown> } | undefined
  const res = {
    headersSent: false,
    status: () => res,
    json: (payload: { error: Record<string, unknown> }) => {
      body = payload
      return res
    },
    send: () => res,
    setHeader: () => res,
    getHeader: () => undefined,
  } as unknown as Response

  const req = {
    headers: {}, path: '/t', method: 'GET', query: {}, params: {},
    get: () => undefined,
    ...(requestId ? { context: { requestId } } : {}),
  } as unknown as Request

  const middleware = ErrorMiddleware as unknown as { logger: Record<string, sinon.SinonStub> }
  const original = middleware.logger
  middleware.logger = {
    error: sinon.stub(), warn: sinon.stub(), info: sinon.stub(), debug: sinon.stub(),
  }
  try {
    ErrorMiddleware.handleError()(
      error, req, res, sinon.stub() as unknown as NextFunction,
    )
  } finally {
    middleware.logger = original
  }
  if (!body) throw new Error('the middleware sent no JSON body')
  return body.error
}

describe('the error body matches the published spec', () => {
  afterEach(() => {
    sinon.restore()
  })

  it('sends no field the spec does not allow', () => {
    const schema = errorPayloadSchema()
    expect(schema.additionalProperties, 'the spec should still forbid extras').to.equal(false)
    const allowed = Object.keys(schema.properties ?? {})

    const sent = Object.keys(bodyFromMiddleware(new UnauthorizedError('No token provided'), 'req-123'))

    expect(
      sent.filter((key) => !allowed.includes(key)),
      `add it to ErrorResponse in ${SPEC.split('src/')[1]} before shipping it`,
    ).to.deep.equal([])
  })

  it('sends everything the spec requires', () => {
    const schema = errorPayloadSchema()
    const sent = Object.keys(bodyFromMiddleware(new UnauthorizedError('No token provided')))
    expect(sent).to.include.members(schema.required ?? [])
  })

  it('still names the request when one was assigned', () => {
    const sent = bodyFromMiddleware(new UnauthorizedError('No token provided'), 'req-123')
    expect(sent.requestId).to.equal('req-123')
  })
})
