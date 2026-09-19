import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import { PassThrough } from 'stream'
import multer from 'multer'
import { createMulter } from '../../../src/libs/utils/multer.utils'
import { FileProcessorService } from '../../../src/libs/middlewares/file_processor/fp.service'
import { FileProcessingType } from '../../../src/libs/middlewares/file_processor/fp.constant'
import { Logger } from '../../../src/libs/services/logger.service'

const NAME = 'résumé 日本語 🎉.txt'
const BOUNDARY = 'pipeshub-test-boundary'

// A request as a browser sends it: the filename parameter is raw UTF-8 and
// there is no separate path field.
function multipartRequest(field: string, filename: string): any {
  const body = Buffer.from(
    `--${BOUNDARY}\r\n` +
      `Content-Disposition: form-data; name="${field}"; filename="${filename}"\r\n` +
      'Content-Type: text/plain\r\n\r\n' +
      'hello\r\n' +
      `--${BOUNDARY}--\r\n`,
    'utf8',
  )
  const req: any = new PassThrough()
  req.headers = {
    'content-type': `multipart/form-data; boundary=${BOUNDARY}`,
    'content-length': String(body.length),
  }
  req.body = {}
  req.method = 'POST'
  req.end(body)
  return req
}

function run(handler: any, req: any): Promise<void> {
  return new Promise((resolve, reject) => {
    handler(req, {}, (err?: unknown) => (err ? reject(err) : resolve()))
  })
}

describe('createMulter', () => {
  it('keeps a UTF-8 filename intact', async () => {
    const req = multipartRequest('file', NAME)
    await run(createMulter({ storage: multer.memoryStorage() }).single('file'), req)
    expect(req.file.originalname).to.equal(NAME)
  })

  it('differs from bare multer, which decodes the name as Latin-1', async () => {
    const req = multipartRequest('file', NAME)
    await run(multer({ storage: multer.memoryStorage() }).single('file'), req)
    expect(req.file.originalname).to.not.equal(NAME)
  })
})

describe('FileProcessorService upload keeps non-ASCII names', () => {
  beforeEach(() => {
    const logger = Logger.getInstance()
    sinon.stub(logger, 'error')
    sinon.stub(logger, 'warn')
    sinon.stub(logger, 'debug')
    sinon.stub(logger, 'info')
  })

  afterEach(() => {
    sinon.restore()
  })

  for (const partialUpload of [true, false]) {
    it(`with partialUpload=${partialUpload}`, async () => {
      const service = new FileProcessorService({
        fieldName: 'files',
        maxFileSize: 1024 * 1024,
        allowedMimeTypes: ['text/plain'],
        allowedExtensions: ['txt'],
        maxFilesAllowed: 10,
        isMultipleFilesAllowed: true,
        processingType: FileProcessingType.BUFFER,
        strictFileUpload: true,
        partialUpload,
      })
      const req = multipartRequest('files', NAME)
      await run(service.upload(), req)
      expect(req.files.map((f: any) => f.originalname)).to.deep.equal([NAME])
    })
  }
})
