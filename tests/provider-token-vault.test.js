"use strict";

const assert=require("node:assert/strict");
const test=require("node:test");
const {parseKeyring,createProviderTokenVault}=require("../security/provider-token-vault");

const keyA=Buffer.alloc(32,1);
const keyB=Buffer.alloc(32,2);
const context={userId:"user-a",platform:"google",tokenType:"refresh"};

test("keyring parsing requires an explicit active 256-bit key without exposing key material",()=>{
  const secret="not-a-valid-key";
  assert.throws(()=>parseKeyring({PROVIDER_TOKEN_ACTIVE_KEY_ID:"a",PROVIDER_TOKEN_ENCRYPTION_KEYS:JSON.stringify({a:secret})}),error=>error.code==="TOKEN_VAULT_CONFIG_ERROR"&&!error.message.includes(secret));
  const parsed=parseKeyring({PROVIDER_TOKEN_ACTIVE_KEY_ID:"a",PROVIDER_TOKEN_ENCRYPTION_KEYS:JSON.stringify({a:keyA.toString("base64")})});
  assert.equal(parsed.activeKeyId,"a");
});

test("AES-256-GCM envelope round-trips without storing plaintext",()=>{
  const vault=createProviderTokenVault({activeKeyId:"a",keys:new Map([["a",keyA]])});
  const envelope=vault.encrypt("provider-secret-token",context);
  assert.equal(envelope.version,"v1");
  assert.equal(envelope.keyId,"a");
  assert.doesNotMatch(JSON.stringify(envelope),/provider-secret-token/);
  assert.equal(vault.decrypt(envelope,context),"provider-secret-token");
});

test("AAD binds ciphertext to user, platform and token type",()=>{
  const vault=createProviderTokenVault({activeKeyId:"a",keys:new Map([["a",keyA]])});
  const envelope=vault.encrypt("token",context);
  for(const changed of [{...context,userId:"user-b"},{...context,platform:"meta"},{...context,tokenType:"access"}]){
    assert.throws(()=>vault.decrypt(envelope,changed),error=>error.code==="TOKEN_DECRYPTION_FAILED");
  }
});

test("tampering is rejected and error output never contains plaintext",()=>{
  const vault=createProviderTokenVault({activeKeyId:"a",keys:new Map([["a",keyA]])});
  const envelope=vault.encrypt("never-log-me",context);
  envelope.ciphertext=Buffer.from("tampered").toString("base64");
  assert.throws(()=>vault.decrypt(envelope,context),error=>error.code==="TOKEN_DECRYPTION_FAILED"&&!error.message.includes("never-log-me"));
});

test("old keys remain readable and are marked for rotation",()=>{
  const oldVault=createProviderTokenVault({activeKeyId:"a",keys:new Map([["a",keyA]])});
  const envelope=oldVault.encrypt("token",context);
  const rotated=createProviderTokenVault({activeKeyId:"b",keys:new Map([["a",keyA],["b",keyB]])});
  assert.equal(rotated.decrypt(envelope,context),"token");
  assert.equal(rotated.needsRotation(envelope),true);
  assert.equal(rotated.needsRotation(rotated.encrypt("token",context)),false);
});

test("null tokens remain null",()=>{
  const vault=createProviderTokenVault({activeKeyId:"a",keys:new Map([["a",keyA]])});
  assert.equal(vault.encrypt(null,context),null);
  assert.equal(vault.decrypt(null,context),null);
});
