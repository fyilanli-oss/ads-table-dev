'use strict';
const GOOGLE_CAPABILITIES=Object.freeze({
 standard:Object.freeze({campaign_type:'standard',root_entity_type:'campaign',parent_entity_type:'adgroup',entity_type:'ad',parent_supported:true}),
 performance_max:Object.freeze({campaign_type:'performance_max',root_entity_type:'campaign',parent_entity_type:null,entity_type:'asset_group',parent_supported:false})
});
function googleCapability(campaignType){const capability=GOOGLE_CAPABILITIES[campaignType];if(!capability)throw new Error('Unsupported Google campaign type');return capability;}
module.exports=Object.freeze({GOOGLE_CAPABILITIES,googleCapability});
