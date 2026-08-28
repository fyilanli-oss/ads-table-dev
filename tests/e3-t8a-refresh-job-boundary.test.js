"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createRefreshJobBoundary } = require("../src/jobs/refresh-job-boundary");

function client({ existing = null } = {}) {
  const calls = [];
  let mode = null;
  const builder = {
    select(value) { calls.push(["select", value]); return this; },
    eq(key, value) { calls.push(["eq", key, value]); return this; },
    in(key, value) { calls.push(["in", key, value]); return this; },
    limit(value) { calls.push(["limit", value]); return this; },
    insert(value) { mode = "insert"; calls.push(["insert", value]); return this; },
    update(value) { mode = "update"; calls.push(["update", value]); return this; },
    async maybeSingle() { if (!mode) return { data: existing, error: null }; return { data: mode === "insert" ? { id: "job-1", status: "queued" } : { id: "job-1" }, error: null }; },
  };
  return { calls, from(table) { calls.push(["from", table]); mode = null; return builder; } };
}

test("creates one queued job with deterministic lifecycle metadata", async () => {
  const database = client();
  const boundary = createRefreshJobBoundary({ getClient: () => database, lifecycleVersion: "v1", now: () => "2026-08-28T00:00:00.000Z" });
  const job = await boundary.create({ userId: "user", platform: "meta", platformAccountId: "account", metadata: { trigger: "manual", captureReason: "manual_refresh" } });
  assert.equal(job.id, "job-1");
  const insert = database.calls.find(([name]) => name === "insert")[1];
  assert.deepEqual(insert, { user_id: "user", platform: "meta", platform_account_id: "account", status: "queued", job_type: "manual", capture_reason: "manual_refresh", lifecycle_version: "v1", metadata: { trigger: "manual", captureReason: "manual_refresh" }, created_at: "2026-08-28T00:00:00.000Z", updated_at: "2026-08-28T00:00:00.000Z" });
});

test("rejects a duplicate active job before insert", async () => {
  const database = client({ existing: { id: "active", status: "running" } });
  const boundary = createRefreshJobBoundary({ getClient: () => database, lifecycleVersion: "v1" });
  await assert.rejects(boundary.create({ userId: "user", platform: "meta", platformAccountId: "account" }), error => error.status === 409 && error.job.id === "active");
  assert.equal(database.calls.some(([name]) => name === "insert"), false);
});

test("run owns queued-running-completed orchestration and returns work result", async () => {
  const database = client();
  const boundary = createRefreshJobBoundary({ getClient: () => database, lifecycleVersion: "v1", now: () => "now" });
  const output = await boundary.run({ userId: "user", platform: "google", platformAccountId: "account", work: async ({ jobId }) => ({ snapshotId: `${jobId}-snapshot` }), completed: result => ({ snapshot_id: result.snapshotId }) });
  assert.equal(output.result.snapshotId, "job-1-snapshot");
  const updates = database.calls.filter(([name]) => name === "update").map(([, patch]) => patch);
  assert.deepEqual(updates, [{ status: "running", updated_at: "now", started_at: "now" }, { status: "completed", updated_at: "now", snapshot_id: "job-1-snapshot", finished_at: "now" }]);
});

test("run records a safe failed terminal transition and rethrows", async () => {
  const database = client();
  const boundary = createRefreshJobBoundary({ getClient: () => database, lifecycleVersion: "v1", now: () => "now" });
  const failure = new Error("provider unavailable");
  await assert.rejects(boundary.run({ userId: "user", platform: "tiktok", platformAccountId: "account", work: async () => { throw failure; } }), failure);
  assert.deepEqual(database.calls.filter(([name]) => name === "update").at(-1)[1], { status: "failed", updated_at: "now", error_message: "provider unavailable", finished_at: "now" });
});

test("fails closed for invalid composition dependencies", async () => {
  assert.throws(() => createRefreshJobBoundary(), /getClient/);
  const lazy = createRefreshJobBoundary({ getClient: () => null, lifecycleVersion: "v1" });
  await assert.rejects(lazy.create({}), /database client/);
  assert.throws(() => createRefreshJobBoundary({ getClient: () => ({ from() {} }) }), /lifecycleVersion/);
  assert.throws(() => createRefreshJobBoundary({ getClient: () => ({ from() {} }), lifecycleVersion: "v1", now: null }), /now must be a function/);
});
