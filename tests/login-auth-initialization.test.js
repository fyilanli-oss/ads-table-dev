const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');

const login=fs.readFileSync(path.join(__dirname,'..','public','login.html'),'utf8');

test('sign-in actions initialize or retry public auth configuration when clicked',()=>{
  assert.doesNotMatch(login,/id="passwordSignInButton"[^>]*disabled/);
  assert.doesNotMatch(login,/id="googleSignInButton"[^>]*disabled/);
  assert.match(login,/if\(!cfg\.supabaseUrl\|\|!cfg\.supabaseAnonKey\)throw new Error/);
  assert.match(login,/async function requireClient\(\)/);
  assert.match(login,/clientInitialization=null;throw error/);
});

test('both sign-in paths fail visibly instead of calling an uninitialized client',()=>{
  assert.equal((login.match(/if\(!await requireClient\(\)\)return/g)||[]).length,3);
  assert.match(login,/signInWithOAuth/);
  assert.match(login,/signInWithPassword/);
  assert.doesNotMatch(login,/email:email\.value|password:password\.value/);
});
