'use strict';
const {mapGoogleStandardAd}=require('./standard-mapper');
function createGoogleStandardAdapter({client}={}){if(!client||typeof client.fetchStandardAdRows!=='function')throw new TypeError('Google Standard client is required');return Object.freeze({async fetchCanonicalRows(input){const response=await client.fetchStandardAdRows(input);if(!response||!Array.isArray(response.results))throw new Error('Google Standard response must contain results[]');return response.results.map(result=>mapGoogleStandardAd(result,input.context));}});}
module.exports=Object.freeze({createGoogleStandardAdapter});
