"use strict";

function createProviderTokenStore({client,vault,legacyReadsEnabled=true}){
  if(!client||typeof client.from!=="function")throw new TypeError("Provider token store requires a Supabase client");
  if(!vault||typeof vault.encrypt!=="function"||typeof vault.decrypt!=="function")throw new TypeError("Provider token store requires a token vault");

  const context=(userId,platform,tokenType)=>({userId,platform,tokenType});

  async function getEnvelopeRow(userId,platform){
    const {data,error}=await client.from("platform_connection_tokens").select("access_token_envelope,refresh_token_envelope").eq("user_id",userId).eq("platform",platform).maybeSingle();
    if(error)throw new Error(error.message);
    return data||null;
  }

  async function resolve({userId,platform,legacyAccessToken=null,legacyRefreshToken=null}){
    const row=await getEnvelopeRow(userId,platform);
    const accessToken=row?.access_token_envelope?vault.decrypt(row.access_token_envelope,context(userId,platform,"access")):(legacyReadsEnabled?legacyAccessToken:null);
    const refreshToken=row?.refresh_token_envelope?vault.decrypt(row.refresh_token_envelope,context(userId,platform,"refresh")):(legacyReadsEnabled?legacyRefreshToken:null);
    return {
      accessToken:accessToken||null,
      refreshToken:refreshToken||null,
      source:row?"encrypted":"legacy",
      needsRotation:Boolean(
        (row?.access_token_envelope&&vault.needsRotation(row.access_token_envelope))||
        (row?.refresh_token_envelope&&vault.needsRotation(row.refresh_token_envelope))
      )
    };
  }

  async function write({userId,platform,accessToken=null,refreshToken=null}){
    if(!accessToken&&!refreshToken)throw new TypeError("At least one provider token is required");
    const row={
      user_id:userId,
      platform,
      access_token_envelope:vault.encrypt(accessToken,context(userId,platform,"access")),
      refresh_token_envelope:vault.encrypt(refreshToken,context(userId,platform,"refresh")),
      updated_at:new Date().toISOString()
    };
    const {error}=await client.from("platform_connection_tokens").upsert(row,{onConflict:"user_id,platform"});
    if(error)throw new Error(error.message);
  }

  async function remove({userId,platform}){
    const {error}=await client.from("platform_connection_tokens").delete().eq("user_id",userId).eq("platform",platform);
    if(error)throw new Error(error.message);
  }

  return Object.freeze({resolve,write,remove});
}

module.exports={createProviderTokenStore};
