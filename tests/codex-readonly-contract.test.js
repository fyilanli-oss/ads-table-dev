"use strict";

const assert=require("node:assert/strict");
const test=require("node:test");
const {OPERATIONS,validateOperationResponse}=require("../security/codex-readonly-contract");

test("operation allowlist is exact and immutable",()=>{
  assert.deepEqual(OPERATIONS,["health","dataset-v2-contract","dataset-v2-access-boundary","dataset-v2-safe-counts","migration-inventory"]);
  assert.equal(Object.isFrozen(OPERATIONS),true);
});

test("response validation rejects extra, sensitive, missing, and invalid count fields",()=>{
  const safe={datasetV2Rows:12};
  assert.strictEqual(validateOperationResponse("dataset-v2-safe-counts",safe),safe);
  for(const value of [{...safe,user_id:"x"},{...safe,token:"x"},{datasetV2Rows:-1},{datasetV2Rows:1.5},{}]){
    assert.throws(()=>validateOperationResponse("dataset-v2-safe-counts",value),/Sensitive response field|Unexpected contract field|Invalid contract/);
  }
  assert.throws(()=>validateOperationResponse("anything",{}));
});
