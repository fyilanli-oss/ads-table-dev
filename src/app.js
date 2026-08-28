"use strict";

const express = require("express");
const { createRequestBoundary } = require("./middleware/http-boundary");

function createApplication({ publicDirectory, tiktokTestPageEnabled = false, logger = console } = {}) {
  if (!publicDirectory) throw new TypeError("publicDirectory is required");

  const app = express();
  app.set("trust proxy", 1);
  app.use(createRequestBoundary({ logger }));
  app.use(express.json());
  app.use((req, res, next) =>
    req.path === "/tiktok-test.html" && !tiktokTestPageEnabled
      ? res.sendStatus(404)
      : next(),
  );
  app.use(express.static(publicDirectory));
  return app;
}

function startApplication(app, { port, logger = console } = {}) {
  if (!app || typeof app.listen !== "function") {
    throw new TypeError("Express application is required");
  }
  if (port === undefined || port === null || port === "") {
    throw new TypeError("port is required");
  }
  return app.listen(port, () => logger.log(`AdsTable server running on ${port}`));
}

module.exports = { createApplication, startApplication };

