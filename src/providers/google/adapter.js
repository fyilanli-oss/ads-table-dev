'use strict';
const {googleCapability}=require('./capabilities');
const {createGooglePmaxAdapter}=require('./pmax-adapter');
const {createGoogleStandardAdapter}=require('./standard-adapter');
function createGoogleAdapter({client}={}){if(!client||typeof client!=='object')throw new TypeError('Google client is required');const standard=createGoogleStandardAdapter({client}),pmax=createGooglePmaxAdapter({client});return Object.freeze({capability:googleCapability,async fetchCanonicalRows(input){const type=input?.campaignType;googleCapability(type);if(type==='standard')return standard.fetchCanonicalRows(input);if(type==='performance_max')return pmax.fetchCanonicalRows(input);throw new Error('Unsupported Google campaign type');}});}
module.exports=Object.freeze({createGoogleAdapter});
