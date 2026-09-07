'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const {validateCanonicalRow}=require('../funnel-core/canonical-contract');
const {mapKlaviyoMessage}=require('../src/providers/klaviyo/mapper');

const context={userId:'user-1',accountId:'account-1',businessDate:'2026-09-07',sourceCurrency:'USD',targetCurrency:'USD',sourceTimezone:'America/New_York'};
const input=(patch={})=>({branch:'campaign',channel:'email',root:{id:'root-1',name:'Campaign'},message:{id:'message-1',name:'Message'},metrics:{delivered:100,unique_clicks:12,unique_opens:40,add_to_cart:null,add_to_cart_value:null,checkout:null,checkout_value:null,purchase:null,purchase_value:null},metric_support:{add_to_cart:'unknown',add_to_cart_value:'unknown',checkout:'unknown',checkout_value:'unknown',purchase:'unknown',purchase_value:'unknown'},...patch});

test('E7 T1-T3 map email campaign message and SMS flow message into one canonical envelope',()=>{
  const email=mapKlaviyoMessage(input(),context),sms=mapKlaviyoMessage(input({branch:'flow',channel:'sms',root:{id:'root-2',name:'Flow'},metrics:{...input().metrics,provider_spend:4}}),context);
  validateCanonicalRow(email.row);validateCanonicalRow(sms.row);assert.equal(email.row.entity.entity_type,'campaign_message');assert.equal(sms.row.entity.entity_type,'flow_message');assert.equal(email.row.identity.channel,'email');assert.equal(sms.row.identity.channel,'sms');
});

test('branch-aware keys prevent same message id collision',()=>{
  const campaign=mapKlaviyoMessage(input(),context),flow=mapKlaviyoMessage(input({branch:'flow'}),context);
  assert.notEqual(campaign.entityKey,flow.entityKey);assert.match(campaign.entityKey,/campaign/);assert.match(flow.entityKey,/flow/);
});

test('E7 T4 never substitutes opens for clicks and preserves journey support',()=>{
  const result=mapKlaviyoMessage(input(),context).row;
  assert.equal(result.raw_metrics.ad_click,12);assert.equal(result.provenance.raw_reference.open_count_present,true);assert.equal(result.provenance.raw_reference.click_source,'unique_clicks');assert.equal(result.raw_metrics.purchase,null);assert.equal(result.metric_support.purchase,'unknown');
});

test('E7 T5 accepts only provider SMS spend and never invents missing spend',()=>{
  const sms=mapKlaviyoMessage(input({channel:'sms',metrics:{...input().metrics,provider_spend:3.5}}),context).row,missing=mapKlaviyoMessage(input({channel:'sms'}),context).row,email=mapKlaviyoMessage(input({metrics:{...input().metrics,provider_spend:99}}),context).row;
  assert.equal(sms.raw_metrics.spend_value,3.5);assert.equal(sms.metric_support.spend_value,'supported');assert.equal(missing.raw_metrics.spend_value,null);assert.equal(missing.metric_support.spend_value,'unsupported');assert.equal(email.raw_metrics.spend_value,null);assert.equal(email.provenance.raw_reference.spend_source,null);
});

test('invalid channel, branch, hierarchy facts and metric values fail closed',()=>{
  assert.throws(()=>mapKlaviyoMessage(input({channel:'push'}),context),/channel/);assert.throws(()=>mapKlaviyoMessage(input({branch:'segment'}),context),/branch/);assert.throws(()=>mapKlaviyoMessage(input({message:{id:'',name:'Message'}}),context),/message.id/);assert.throws(()=>mapKlaviyoMessage(input({metrics:{...input().metrics,unique_clicks:-1}}),context),/non-negative/);assert.throws(()=>mapKlaviyoMessage(input({metrics:{...input().metrics,purchase:1},metric_support:{...input().metric_support,purchase:'unsupported'}}),context),/requires supported/);
});
