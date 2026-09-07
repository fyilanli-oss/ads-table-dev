'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const {validateCanonicalRow}=require('../funnel-core/canonical-contract');
const {allocateKlaviyoDailySpend,mapKlaviyoMessage}=require('../src/providers/klaviyo/mapper');

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
  assert.equal(sms.raw_metrics.spend_value,3.5);assert.equal(sms.metric_support.spend_value,'supported');assert.equal(sms.provenance.raw_reference.spend.spend_kind,'actual');assert.equal(missing.raw_metrics.spend_value,null);assert.equal(missing.metric_support.spend_value,'unsupported');assert.equal(email.raw_metrics.spend_value,null);assert.equal(email.provenance.raw_reference.spend.spend_source,null);
});

test('E7 T6 allocates Email monthly plan by sent-volume share instead of calendar days',()=>{
  const result=allocateKlaviyoDailySpend({channel:'email',monthlyPlanCost:100,dailySentCount:5000,monthlySentCount:50000,periodClosed:false});
  assert.equal(result.spend_value,10);assert.equal(result.metric_support,'supported');assert.equal(result.provenance.spend_kind,'allocated');assert.equal(result.provenance.allocation_status,'provisional');
});

test('E7 T6 finalizes closed periods and estimates only explicitly priced overage',()=>{
  const result=allocateKlaviyoDailySpend({channel:'email',monthlyPlanCost:100,dailySentCount:5000,monthlySentCount:50000,includedMonthlySends:40000,overageUnitCost:.001,cumulativeSentYesterday:39000,cumulativeSentToday:43000,periodClosed:true});
  assert.equal(result.spend_value,13);assert.equal(result.provenance.base_allocated,10);assert.equal(result.provenance.overage_estimated,3);assert.equal(result.provenance.spend_kind,'allocated_plus_estimated_overage');assert.equal(result.provenance.allocation_status,'finalized');
});

test('E7 T6 uses provider actual before estimates and never embeds an internet default price',()=>{
  const actual=allocateKlaviyoDailySpend({channel:'sms',providerSpend:7,usageUnitCost:99,dailySentCount:20}),estimated=allocateKlaviyoDailySpend({channel:'sms',usageUnitCost:.02,dailySentCount:20}),missing=allocateKlaviyoDailySpend({channel:'email',monthlyPlanCost:100,dailySentCount:0,monthlySentCount:0});
  assert.equal(actual.spend_value,7);assert.equal(actual.provenance.spend_kind,'actual');assert.equal(estimated.spend_value,.4);assert.equal(estimated.provenance.spend_kind,'estimated');assert.equal(missing.spend_value,null);assert.equal(missing.metric_support,'unknown');
});

test('E7 T6 rejects contradictory period and cumulative usage inputs',()=>{assert.throws(()=>allocateKlaviyoDailySpend({channel:'email',monthlyPlanCost:100,dailySentCount:20,monthlySentCount:10}),/cannot exceed/);assert.throws(()=>allocateKlaviyoDailySpend({channel:'email',monthlyPlanCost:100,dailySentCount:5,monthlySentCount:10,includedMonthlySends:4,overageUnitCost:1,cumulativeSentYesterday:8,cumulativeSentToday:7}),/cannot be below/);assert.throws(()=>allocateKlaviyoDailySpend({channel:'sms',periodClosed:'yes'}),/must be boolean/)});

test('invalid channel, branch, hierarchy facts and metric values fail closed',()=>{
  assert.throws(()=>mapKlaviyoMessage(input({channel:'push'}),context),/channel/);assert.throws(()=>mapKlaviyoMessage(input({branch:'segment'}),context),/branch/);assert.throws(()=>mapKlaviyoMessage(input({message:{id:'',name:'Message'}}),context),/message.id/);assert.throws(()=>mapKlaviyoMessage(input({metrics:{...input().metrics,unique_clicks:-1}}),context),/non-negative/);assert.throws(()=>mapKlaviyoMessage(input({metrics:{...input().metrics,purchase:1},metric_support:{...input().metric_support,purchase:'unsupported'}}),context),/requires supported/);
});
