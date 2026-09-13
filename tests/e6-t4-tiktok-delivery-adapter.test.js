'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const fixture=require('./fixtures/e6-t4-tiktok-delivery-row.json');
const {validateCanonicalRow}=require('../funnel-core/canonical-contract');
const {buildEntityKey,validateEntityHierarchy}=require('../funnel-core/entity-hierarchy');
const {mapTikTokDeliveryAd,mapTikTokDeliveryRows}=require('../src/providers/tiktok/delivery-mapper');

test('E6-T4+T4A+T4B maps one Ad leaf into the canonical seven-block envelope',()=>{
  const result=mapTikTokDeliveryAd(fixture.input,fixture.context),row=result.row;
  assert.deepEqual(Object.keys(row),['identity','entity','raw_metrics','metric_support','currency','time','provenance']);
  validateCanonicalRow(row);validateEntityHierarchy(row.identity,row.entity);
  assert.equal(row.entity.root_entity_type,'campaign');assert.equal(row.entity.parent_entity_type,'adgroup');assert.equal(row.entity.entity_type,'ad');
  assert.equal(result.entityKey,buildEntityKey(row.identity,row.entity));
  assert.deepEqual({impression:row.raw_metrics.impression,ad_click:row.raw_metrics.ad_click,spend_value:row.raw_metrics.spend_value},{impression:1250,ad_click:75,spend_value:42.5});
});

test('delivery-only mapping ignores generic conversion and leaves every event unsupported/null',()=>{
  const {row}=mapTikTokDeliveryAd(fixture.input,fixture.context);
  for(const key of ['add_to_cart','add_to_cart_value','checkout','checkout_value','purchase','purchase_value']){assert.equal(row.raw_metrics[key],null);assert.equal(row.metric_support[key],'unsupported');}
  assert.equal(row.provenance.synthetic,false);assert.equal(row.provenance.raw_reference.event_fields_ignored.includes('purchase'),true);
});

test('missing delivery facts remain unknown/null and are never converted to zero',()=>{
  const input=structuredClone(fixture.input);delete input.metrics.clicks;
  const {row}=mapTikTokDeliveryAd(input,fixture.context);
  assert.equal(row.raw_metrics.ad_click,null);assert.equal(row.metric_support.ad_click,'unknown');assert.equal(row.provenance.source_confidence,'partial');
});

test('non-leaf, synthetic and duplicate rows fail closed before double counting',()=>{
  assert.throws(()=>mapTikTokDeliveryAd({...fixture.input,level:'campaign'},fixture.context),/AUCTION_AD leaf/);
  assert.throws(()=>mapTikTokDeliveryAd({...fixture.input,synthetic:true},fixture.context),/Synthetic TikTok/);
  assert.throws(()=>mapTikTokDeliveryRows([fixture.input,structuredClone(fixture.input)],fixture.context),/double-count/);
});

test('complete lineage and non-negative delivery facts are mandatory',()=>{
  const noParent=structuredClone(fixture.input);delete noParent.dimensions.adgroup_id;
  assert.throws(()=>mapTikTokDeliveryAd(noParent,fixture.context),/dimensions.adgroup_id/);
  const negative=structuredClone(fixture.input);negative.metrics.spend='-1';
  assert.throws(()=>mapTikTokDeliveryAd(negative,fixture.context),/non-negative/);
});
