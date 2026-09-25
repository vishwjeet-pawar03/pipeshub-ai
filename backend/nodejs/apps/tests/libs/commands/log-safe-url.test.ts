import { expect } from 'chai'
import { logSafeUrl } from '../../../src/libs/commands/log-safe-url'

describe('logSafeUrl', () => {
  it('hides an OAuth callback code and state but keeps the rest of the URL', () => {
    expect(
      logSafeUrl('http://connector:8088/api/v1/connectors/oauth/callback?code=4%2F0Ab-secret&state=s1&base_url=https%3A%2F%2Fapp.acme.test'),
    ).to.equal(
      'http://connector:8088/api/v1/connectors/oauth/callback?code=REDACTED&state=REDACTED&base_url=https%3A%2F%2Fapp.acme.test',
    )
  })

  it('hides token-like params whatever their case', () => {
    const logged = logSafeUrl('http://iam:3000/api/v1/x?Token=t1&access_token=a1&REFRESH_TOKEN=r1&id_token=i1&client_secret=c1&page=2')
    for (const secret of ['t1', 'a1', 'r1', 'i1', 'c1']) expect(logged).to.not.include(`=${secret}`)
    expect(logged).to.include('page=2')
  })

  it('leaves a URL without a query as it is', () => {
    expect(logSafeUrl('http://connector:8088/api/v1/connectors/active')).to.equal('http://connector:8088/api/v1/connectors/active')
  })

  it('drops the whole query of a string that is not a URL', () => {
    expect(logSafeUrl('not a url?code=secret')).to.equal('not a url')
  })
})
