'use strict';

const ORGANIC_GA4_INGEST_ENABLED=false;
const ORGANIC_GA4_PARK_REASON='utm_attribution_not_reliable';

function organicGa4ParkedError(){return Object.assign(new Error('GA4 Organic ingestion is parked'),{status:410,code:'ORGANIC_GA4_INGESTION_PARKED'});}
function requireOrganicGa4Ingest(){if(!ORGANIC_GA4_INGEST_ENABLED)throw organicGa4ParkedError();return true;}

module.exports=Object.freeze({ORGANIC_GA4_INGEST_ENABLED,ORGANIC_GA4_PARK_REASON,organicGa4ParkedError,requireOrganicGa4Ingest});
