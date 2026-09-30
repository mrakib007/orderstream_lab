export class HttpError extends Error {
  constructor(statusCode, body) {
    super(body?.error ?? 'Request failed.');
    this.name = 'HttpError';
    this.statusCode = statusCode;
    this.body = body;
  }
}
