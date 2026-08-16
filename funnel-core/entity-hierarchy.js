'use strict';

const { validateEntityShape } = require('./canonical-contract');

const ENTITY_TYPES = Object.freeze(['ad', 'asset_group', 'campaign_message', 'flow_message', 'organic']);
const ROOT_TYPES = Object.freeze(['campaign', 'flow', 'organic']);
const PARENT_TYPES = Object.freeze(['adset', 'adgroup', 'campaign', 'flow']);

function fail(message) {
  throw new Error(`Hierarchy validation failed: ${message}`);
}

function isNullish(value) {
  return value === null || value === undefined;
}

function requireRoot(entity, expectedType) {
  if (entity.root_entity_type !== expectedType) fail(`root_entity_type must be ${expectedType}`);
  if (!entity.root_entity_id) fail('root_entity_id is required');
}

function requireParent(entity, expectedType) {
  if (entity.parent_entity_type !== expectedType) fail(`parent_entity_type must be ${expectedType}`);
  if (!entity.parent_entity_id) fail('parent_entity_id is required');
}

function requireNoParent(entity) {
  if (!isNullish(entity.parent_entity_type) || !isNullish(entity.parent_entity_id) || !isNullish(entity.parent_entity_name)) {
    fail('this hierarchy branch must not invent a parent level');
  }
}

function validateEntityHierarchy(identity, entity) {
  validateEntityShape(entity);
  if (!ENTITY_TYPES.includes(entity.entity_type)) fail(`unsupported entity_type=${entity.entity_type}`);
  if (entity.root_entity_type !== null && entity.root_entity_type !== undefined && !ROOT_TYPES.includes(entity.root_entity_type)) fail(`unsupported root_entity_type=${entity.root_entity_type}`);
  if (entity.parent_entity_type !== null && entity.parent_entity_type !== undefined && !PARENT_TYPES.includes(entity.parent_entity_type)) fail(`unsupported parent_entity_type=${entity.parent_entity_type}`);

  if (identity.traffic_type === 'organic') {
    if (entity.campaign_type !== null && entity.campaign_type !== undefined) fail('Organic rows must have campaign_type=null');
    requireRoot(entity, 'organic');
    requireNoParent(entity);
    if (entity.entity_type !== 'organic') fail('Organic rows must use entity_type=organic');
    return entity;
  }

  if (identity.platform === 'meta') {
    if (entity.campaign_type !== null && entity.campaign_type !== undefined) fail('Meta rows use campaign_type=null in the canonical contract');
    requireRoot(entity, 'campaign');
    requireParent(entity, 'adset');
    if (entity.entity_type !== 'ad') fail('Meta paid leaf must be Ad');
    return entity;
  }

  if (identity.platform === 'google') {
    requireRoot(entity, 'campaign');
    if (entity.campaign_type === 'performance_max') {
      requireNoParent(entity);
      if (entity.entity_type !== 'asset_group') fail('Performance Max leaf must be Asset Group; fake AdGroup/Ad is forbidden');
    } else if (entity.campaign_type === 'standard') {
      requireParent(entity, 'adgroup');
      if (entity.entity_type !== 'ad') fail('Google Standard paid leaf must be Ad');
    } else {
      fail('Google paid rows require campaign_type=standard|performance_max');
    }
    return entity;
  }

  if (identity.platform === 'tiktok') {
    if (entity.campaign_type !== null && entity.campaign_type !== undefined) fail('TikTok rows use campaign_type=null in the current canonical contract');
    requireRoot(entity, 'campaign');
    requireParent(entity, 'adgroup');
    if (entity.entity_type !== 'ad') fail('TikTok paid leaf must be Ad');
    return entity;
  }

  if (identity.platform === 'klaviyo') {
    if (entity.campaign_type !== null && entity.campaign_type !== undefined) fail('Klaviyo rows must have campaign_type=null');
    requireNoParent(entity);
    if (entity.root_entity_type === 'campaign') {
      requireRoot(entity, 'campaign');
      if (entity.entity_type !== 'campaign_message') fail('Klaviyo Campaign branch leaf must be Campaign Message');
    } else if (entity.root_entity_type === 'flow') {
      requireRoot(entity, 'flow');
      if (entity.entity_type !== 'flow_message') fail('Klaviyo Flow branch leaf must be Flow Message');
    } else {
      fail('Klaviyo paid root must be campaign|flow');
    }
    return entity;
  }

  fail(`unsupported platform=${identity.platform}`);
}

function keyPart(value) {
  return encodeURIComponent(value === null || value === undefined ? '~' : String(value));
}

function buildEntityKey(identity, entity) {
  validateEntityHierarchy(identity, entity);
  return [
    identity.platform,
    identity.platform_account_id,
    identity.traffic_type,
    identity.channel ?? 'none',
    entity.root_entity_type ?? 'none',
    entity.root_entity_id ?? 'none',
    entity.entity_type,
    entity.entity_id
  ].map(keyPart).join(':');
}

module.exports = Object.freeze({
  ENTITY_TYPES,
  ROOT_TYPES,
  PARENT_TYPES,
  validateEntityHierarchy,
  buildEntityKey
});
