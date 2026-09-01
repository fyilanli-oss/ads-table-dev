'use strict';

const {validateCanonicalRow}=require('../../../funnel-core/canonical-contract');
const {buildEntityKey}=require('../../../funnel-core/entity-hierarchy');
const {normalizeCurrencyCode,normalizeMonetaryRawFields}=require('../../../funnel-core/fx-service');
const {normalizeBusinessDate}=require('../../../funnel-core/time-service');
const {mapTikTokDeliveryAd}=require('./delivery-mapper');
const {isolateTikTokProductionRows}=require('./synthetic-isolation');

function text(value,field){if(typeof value!=='string'&&typeof value!=='number')throw new TypeError(`${field} is required`);const normalized=String(value).trim();if(!normalized)throw new TypeError(`${field} is required`);return normalized;}
function advertiser(context={}){const value=context.advertiser;if(!value||typeof value!=='object'||Array.isArray(value))throw new TypeError('context.advertiser is required');const id=text(value.id,'context.advertiser.id');if(id!==text(context.advertiserId,'context.advertiserId'))throw new Error('TikTok advertiser identity mismatch');return Object.freeze({id,sourceCurrency:normalizeCurrencyCode(value.currency,'context.advertiser.currency'),sourceTimezone:text(value.timezone,'context.advertiser.timezone')});}

function mapTikTokTimeFxAd(input,context={}){
  const account=advertiser(context),providerDate=text(context.providerDate??input?.dimensions?.stat_time_day??input?.date,'context.providerDate');
  const time=normalizeBusinessDate({providerDate,sourceTimezone:account.sourceTimezone});
  const mapped=mapTikTokDeliveryAd(input,{...context,advertiserId:account.id,businessDate:time.business_date,sourceCurrency:account.sourceCurrency,targetCurrency:account.sourceCurrency,sourceTimezone:account.sourceTimezone,fxEngineVersion:'v1',timeEngineVersion:time.time_engine_version});
  const targetCurrency=normalizeCurrencyCode(context.targetCurrency??account.sourceCurrency,'context.targetCurrency'),crossCurrency=targetCurrency!==account.sourceCurrency;
  const fxProvider=crossCurrency?text(context.fxProvider,'context.fxProvider'):(context.fxProvider??'same_currency');
  const row=normalizeMonetaryRawFields(mapped.row,{sourceCurrency:account.sourceCurrency,targetCurrency,fxRate:context.fxRate??null,fxRateDate:context.fxRateDate??time.business_date,fxProvider});
  row.identity.date=time.business_date;row.time=time;validateCanonicalRow(row);
  return Object.freeze({row:Object.freeze(row),entityKey:buildEntityKey(row.identity,row.entity)});
}

function mapTikTokProductionRowsWithTimeFx(rows,context={}){
  const isolated=isolateTikTokProductionRows(rows),mapped=isolated.productionRows.map(row=>mapTikTokTimeFxAd(row,context)),keys=new Set();
  for(const item of mapped){const key=`${item.row.identity.date}:${item.entityKey}`;if(keys.has(key))throw new Error('Duplicate normalized TikTok Ad leaf would double-count delivery facts');keys.add(key);}
  return Object.freeze({mapped:Object.freeze(mapped),isolation:isolated.evidence});
}

module.exports=Object.freeze({mapTikTokTimeFxAd,mapTikTokProductionRowsWithTimeFx});
