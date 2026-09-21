import 'reflect-metadata'
import { expect } from 'chai'
import { readFileSync } from 'fs'
import { join } from 'path'
import yaml from 'js-yaml'
import { Users } from '../../../src/modules/user_management/schema/users.schema'

/**
 * What a user endpoint sends must be what the published spec promises.
 *
 * `User` and `UpdateUserResponse` both set `additionalProperties: false`, and
 * both list every stored field by hand. A field added to the Mongoose schema
 * without being added to both makes every user response-validation test fail
 * at once - which is what `kind` and `isDisabled` did, and only on the
 * nightly. This checks it on every run.
 */

const SPEC = join(
  __dirname, '..', '..', '..', 'src', 'modules', 'api-docs', 'pipeshub-openapi.yaml',
)

/**
 * Every closed schema that describes a stored user. `refreshToken` answers with
 * its own copy rather than referencing `User`, and it is just as closed, so a
 * field missing there fails just as hard.
 */
const SPEC_SCHEMAS = ['User', 'UpdateUserResponse', 'RefreshTokenUser'] as const

function declaredProperties(name: string): string[] {
  const spec = yaml.load(readFileSync(SPEC, 'utf8')) as {
    components: { schemas: Record<string, { properties?: Record<string, unknown> }> }
  }
  const schema = spec.components.schemas[name]
  if (!schema) throw new Error(`the spec no longer declares a ${name} schema`)
  return Object.keys(schema.properties ?? {})
}

/**
 * The stored field names, one level deep: Mongoose reports a subdocument as
 * `address.city`, but the spec declares `address` once and refs its own
 * schema, so comparing full paths would report a gap that is not there.
 */
function storedFields(): string[] {
  return [
    ...new Set(
      Object.keys(Users.schema.paths).map((path) => path.split('.')[0]),
    ),
  ]
}

describe('the user document matches the published spec', () => {
  it('stores no field the spec has not been told about', () => {
    const stored = storedFields()
    expect(stored, 'the user schema should still have fields').to.not.be.empty

    const missing = SPEC_SCHEMAS.flatMap((name) => {
      const declared = declaredProperties(name)
      return stored.filter((field) => !declared.includes(field)).map((field) => `${name}: ${field}`)
    })

    expect(
      missing,
      'add each one to that schema in modules/api-docs/pipeshub-openapi.yaml before shipping it',
    ).to.deep.equal([])
  })

  it('describes the same user in every schema', () => {
    const user = declaredProperties('User')

    // UpdateUserResponse is User plus exactly `meta`, and RefreshTokenUser is
    // User exactly. Asserting the extras rather than only the omissions is what
    // makes this catch a schema that drops `meta` as well as one that forgets a
    // field, and the whole point is that three hand-kept copies drift.
    const extras = { UpdateUserResponse: ['meta'], RefreshTokenUser: [] as string[] }

    for (const [name, expected] of Object.entries(extras)) {
      const other = declaredProperties(name)
      expect({
        [`${name} is missing`]: user.filter((field) => !other.includes(field)),
        [`${name} adds`]: other.filter((field) => !user.includes(field)),
      }).to.deep.equal({ [`${name} is missing`]: [], [`${name} adds`]: expected })
    }
  })
})
