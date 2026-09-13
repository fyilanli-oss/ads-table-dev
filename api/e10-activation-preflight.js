"use strict";
const {activationPreflight} = require("../scripts/e10-t6c-activation-preflight");
const {verifyGitHubActionsOidc} = require("../security/github-actions-oidc");
const AUDIENCE = "https://dev.adstable.app/api/e10/activation-preflight";
function send(res, status, body) { res.statusCode=status; res.setHeader("Content-Type","application/json; charset=utf-8"); res.setHeader("Cache-Control","no-store"); return res.end(JSON.stringify(body)); }
function createHandler({verify=verifyGitHubActionsOidc,preflight=activationPreflight}={}) { return async function handler(req,res) {
  if(req.method!=="POST"){res.setHeader("Allow","POST");return send(res,405,{status:"METHOD_NOT_ALLOWED"});}
  const authorization=String(req.headers?.authorization||"");
  if(!authorization.startsWith("Bearer "))return send(res,401,{status:"AUTHORIZATION_REQUIRED"});
  try{await verify(authorization.slice(7),{audience:AUDIENCE});return send(res,200,preflight(process.env));}
  catch{return send(res,403,{status:"AUTHORIZATION_REJECTED"});}
};}
module.exports=createHandler(); module.exports.createHandler=createHandler; module.exports.AUDIENCE=AUDIENCE;
