'use strict';

const GOOGLE_SHEETS_EXPORT_ENABLED = false;
const GOOGLE_SHEETS_PARK_REASON = 'dataset_v2_export_not_designed';

function googleSheetsParkedError() {
  return Object.assign(new Error('Google Sheets export is parked'), {
    status: 410,
    code: 'GOOGLE_SHEETS_EXPORT_PARKED',
  });
}

function requireGoogleSheetsExport() {
  if (!GOOGLE_SHEETS_EXPORT_ENABLED) throw googleSheetsParkedError();
  return true;
}

module.exports = Object.freeze({
  GOOGLE_SHEETS_EXPORT_ENABLED,
  GOOGLE_SHEETS_PARK_REASON,
  googleSheetsParkedError,
  requireGoogleSheetsExport,
});
