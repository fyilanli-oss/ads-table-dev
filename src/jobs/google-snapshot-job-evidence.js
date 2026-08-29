"use strict";

function entityEvidence(report = {}) {
  return {
    rawCount: report.rawCount,
    effectiveRows: report.rows?.length || 0,
    entityFallback: report.entityFallback || false,
    entityRawCount: report.entityRawCount || 0,
    entityFallbackError: report.entityFallbackError || null,
    entityDiagnosticFallback: report.entityDiagnosticFallback || false,
    conversionBreakdownError: report.conversionBreakdownError,
    landingPageViewError: report.landingPageViewError,
  };
}

function googleSnapshotJobEvidence(result, job) {
  if (!result || !result.google_api) throw new TypeError("Google snapshot API evidence is required");
  return {
    snapshot_id: result.snapshot?.id || null,
    metadata: {
      ...(job?.metadata || {}),
      row_counts: result.row_counts,
      performance_spread_result: result.performance_spread_result || null,
      google_api: {
        campaign: entityEvidence(result.google_api.campaign),
        adgroup: entityEvidence(result.google_api.adgroup),
        ad: entityEvidence(result.google_api.ad),
      },
    },
  };
}

module.exports = { googleSnapshotJobEvidence };
