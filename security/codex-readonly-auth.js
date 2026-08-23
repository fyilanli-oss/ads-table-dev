"use strict";

const crypto=require("node:crypto");

const MIN_TOKEN_BYTES=32;

function configuredToken(token){
  return typeof token==="string"&&Buffer.byteLength(token,"utf8")>=MIN_TOKEN_BYTES;
}

function extractBearer(header){
  if(typeof header!=="string")return null;
  const match=/^Bearer ([^\s]+)$/.exec(header);
  return match?match[1]:null;
}

function constantTimeTokenEqual(candidate,expected){
  if(typeof candidate!=="string"||typeof expected!=="string")return false;
  const candidateDigest=crypto.createHash("sha256").update(candidate,"utf8").digest();
  const expectedDigest=crypto.createHash("sha256").update(expected,"utf8").digest();
  return crypto.timingSafeEqual(candidateDigest,expectedDigest);
}

function createCodexReadonlyAuth(expectedToken){
  const configured=configuredToken(expectedToken);
  return {
    configured,
    authenticate(header){
      if(!configured)return {ok:false,configurationUnavailable:true};
      const candidate=extractBearer(header);
      return {ok:Boolean(candidate&&constantTimeTokenEqual(candidate,expectedToken)),configurationUnavailable:false};
    }
  };
}

module.exports={MIN_TOKEN_BYTES,configuredToken,extractBearer,constantTimeTokenEqual,createCodexReadonlyAuth};
