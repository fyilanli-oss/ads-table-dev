"use strict";

const crypto = require("node:crypto");

const REQUEST_ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

function validateLogger(logger) {
  if (!logger || typeof logger.info !== "function" || typeof logger.error !== "function") {
    throw new TypeError("logger must expose info and error functions");
  }
}

function createRequestBoundary({ logger = console, createId = crypto.randomUUID, clock = Date.now } = {}) {
  validateLogger(logger);
  if (typeof createId !== "function") throw new TypeError("createId must be a function");
  if (typeof clock !== "function") throw new TypeError("clock must be a function");

  return function requestBoundary(req, res, next) {
    const suppliedId = String(req.get("x-request-id") || "");
    const requestId = REQUEST_ID_PATTERN.test(suppliedId) ? suppliedId : String(createId());
    const startedAt = clock();

    req.requestId = requestId;
    res.set("x-request-id", requestId);
    res.once("finish", () => {
      logger.info({
        event: "http_request_completed",
        requestId,
        method: req.method,
        path: req.path,
        status: res.statusCode,
        durationMs: Math.max(0, clock() - startedAt),
      });
    });
    next();
  };
}

function createErrorBoundary({ logger = console } = {}) {
  validateLogger(logger);

  return function errorBoundary(error, req, res, next) {
    if (res.headersSent) return next(error);

    const status = Number.isInteger(error?.status) && error.status >= 400 && error.status < 600
      ? error.status
      : 500;
    const code = typeof error?.code === "string" && /^[A-Z0-9_]{1,64}$/.test(error.code)
      ? error.code
      : "INTERNAL_ERROR";
    const exposed = status < 500 || error?.expose === true;
    const message = exposed && typeof error?.message === "string"
      ? error.message
      : "Internal server error";

    logger.error({
      event: "http_request_failed",
      requestId: req.requestId || null,
      method: req.method,
      path: req.path,
      status,
      code,
    });
    return res.status(status).json({ error: message, code, requestId: req.requestId || null });
  };
}

function installErrorBoundary(app, options) {
  if (!app || typeof app.use !== "function") throw new TypeError("Express application is required");
  app.use(createErrorBoundary(options));
  return app;
}

module.exports = { createRequestBoundary, createErrorBoundary, installErrorBoundary };
