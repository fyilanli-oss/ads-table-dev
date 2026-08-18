"use strict";

const crypto=require("node:crypto");

const TOKEN_VERSION="v1";
const KEY_BYTES=32;
const IV_BYTES=12;

class TokenVaultConfigError extends Error{
  constructor(message){super(message);this.name="TokenVaultConfigError";this.code="TOKEN_VAULT_CONFIG_ERROR";}
}

function decodeKey(value,keyId){
  let key;
  try{key=Buffer.from(String(value||""),"base64");}catch{key=null;}
  if(!key||key.length!==KEY_BYTES)throw new TokenVaultConfigError(`Invalid provider token encryption key: ${keyId}`);
  return key;
}

function parseKeyring(env={}){
  const activeKeyId=String(env.PROVIDER_TOKEN_ACTIVE_KEY_ID||"").trim();
  const raw=String(env.PROVIDER_TOKEN_ENCRYPTION_KEYS||"").trim();
  if(!activeKeyId||!raw)throw new TokenVaultConfigError("Provider token encryption keyring is not configured");
  let parsed;
  try{parsed=JSON.parse(raw);}catch{throw new TokenVaultConfigError("Provider token encryption keyring is invalid JSON");}
  if(!parsed||Array.isArray(parsed)||typeof parsed!=="object")throw new TokenVaultConfigError("Provider token encryption keyring must be an object");
  const keys=new Map(Object.entries(parsed).map(([id,value])=>[id,decodeKey(value,id)]));
  if(!keys.has(activeKeyId))throw new TokenVaultConfigError("Active provider token encryption key is missing from the keyring");
  return {activeKeyId,keys};
}

function tokenAad({userId,platform,tokenType}){
  const values=[userId,platform,tokenType].map(value=>String(value||"").trim());
  if(values.some(value=>!value))throw new TypeError("Token encryption context is incomplete");
  return Buffer.from(values.join("\u0000"),"utf8");
}

function createProviderTokenVault({activeKeyId,keys,randomBytes=crypto.randomBytes}){
  if(!activeKeyId||!(keys instanceof Map)||!keys.has(activeKeyId))throw new TokenVaultConfigError("Provider token vault keyring is incomplete");
  function encrypt(value,context){
    if(value===null||value===undefined||value==="")return null;
    const iv=randomBytes(IV_BYTES);
    const cipher=crypto.createCipheriv("aes-256-gcm",keys.get(activeKeyId),iv);
    cipher.setAAD(tokenAad(context));
    const ciphertext=Buffer.concat([cipher.update(String(value),"utf8"),cipher.final()]);
    return {version:TOKEN_VERSION,keyId:activeKeyId,iv:iv.toString("base64"),tag:cipher.getAuthTag().toString("base64"),ciphertext:ciphertext.toString("base64")};
  }
  function decrypt(envelope,context){
    if(envelope===null||envelope===undefined)return null;
    if(!envelope||envelope.version!==TOKEN_VERSION)throw new Error("Unsupported provider token envelope version");
    const key=keys.get(envelope.keyId);
    if(!key)throw new TokenVaultConfigError(`Provider token decryption key is unavailable: ${envelope.keyId}`);
    try{
      const decipher=crypto.createDecipheriv("aes-256-gcm",key,Buffer.from(envelope.iv,"base64"));
      decipher.setAAD(tokenAad(context));
      decipher.setAuthTag(Buffer.from(envelope.tag,"base64"));
      return Buffer.concat([decipher.update(Buffer.from(envelope.ciphertext,"base64")),decipher.final()]).toString("utf8");
    }catch{throw Object.assign(new Error("Provider token decryption failed"),{code:"TOKEN_DECRYPTION_FAILED"});}
  }
  return Object.freeze({activeKeyId,encrypt,decrypt,needsRotation:envelope=>Boolean(envelope&&envelope.keyId!==activeKeyId)});
}

function createProviderTokenVaultFromEnv(env=process.env){return createProviderTokenVault(parseKeyring(env));}

module.exports={TOKEN_VERSION,TokenVaultConfigError,parseKeyring,createProviderTokenVault,createProviderTokenVaultFromEnv};
