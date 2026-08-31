"use strict";

const TERMINAL_STATUSES = new Set(["completed", "failed"]);

function createRefreshJobBoundary({ getClient, lifecycleVersion, now = () => new Date().toISOString() } = {}) {
  if (typeof getClient !== "function") throw new TypeError("getClient must be a function");
  if (typeof lifecycleVersion !== "string" || !lifecycleVersion) throw new TypeError("lifecycleVersion is required");
  if (typeof now !== "function") throw new TypeError("now must be a function");

  function database() {
    const client = getClient();
    if (!client || typeof client.from !== "function") throw new TypeError("server-side database client is required");
    return client;
  }

  async function create({ userId, platform, platformAccountId, metadata = {} } = {}) {
    const client = database();
    const existing = await client.from("snapshot_jobs").select("id,status").eq("user_id", userId).eq("platform", platform).eq("platform_account_id", platformAccountId).in("status", ["queued", "running"]).limit(1).maybeSingle();
    if (existing.error) throw existing.error;
    if (existing.data) {
      const error = new Error("Refresh job already queued or running for this platform account");
      error.status = 409;
      error.job = existing.data;
      throw error;
    }
    const timestamp = now();
    const { data, error } = await client.from("snapshot_jobs").insert({
      user_id: userId,
      platform,
      platform_account_id: platformAccountId,
      status: "queued",
      job_type: metadata.jobType || metadata.job_type || metadata.trigger || "refresh",
      capture_reason: metadata.captureReason || metadata.capture_reason || null,
      lifecycle_version: metadata.lifecycleVersion || metadata.lifecycle_version || lifecycleVersion,
      metadata,
      created_at: timestamp,
      updated_at: timestamp,
    }).select("*").maybeSingle();
    if (error) throw error;
    return data;
  }

  async function transition(jobId, status, extra = {}) {
    const client = database();
    const timestamp = now();
    const patch = { status, updated_at: timestamp, ...extra };
    if (status === "running") patch.started_at = timestamp;
    if (TERMINAL_STATUSES.has(status)) patch.finished_at = timestamp;
    const { data, error } = await client.from("snapshot_jobs").update(patch).eq("id", jobId).select("*").maybeSingle();
    if (error) throw error;
    return data;
  }

  async function run({ userId, platform, platformAccountId, metadata = {}, work, completed = () => ({}) } = {}) {
    if (typeof work !== "function") throw new TypeError("work must be a function");
    if (typeof completed !== "function") throw new TypeError("completed must be a function");
    const job = await create({ userId, platform, platformAccountId, metadata });
    await transition(job.id, "running");
    try {
      const result = await work(Object.freeze({ jobId: job.id, job }));
      await transition(job.id, "completed", completed(result, job) || {});
      return Object.freeze({ job, result });
    } catch (error) {
      await transition(job.id, "failed", { error_message: error.message, metadata: { ...(job.metadata || {}), failure_stage: typeof error?.safe_stage === "string" ? error.safe_stage : "UNCLASSIFIED" } }).catch(() => null);
      if (error && typeof error === "object" && Object.isExtensible(error)) Object.defineProperty(error, "refreshJob", { value: job, enumerable: false });
      throw error;
    }
  }

  return Object.freeze({ create, transition, run });
}

module.exports = { createRefreshJobBoundary };
