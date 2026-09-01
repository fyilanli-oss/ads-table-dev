const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');

const login=fs.readFileSync(path.join(__dirname,'..','public','login.html'),'utf8');

test('sign-in actions stay disabled until public auth configuration is ready',()=>{
  assert.match(login,/id="passwordSignInButton"[^>]*disabled/);
  assert.match(login,/id="googleSignInButton"[^>]*disabled/);
  assert.match(login,/if\(!cfg\.supabaseUrl\|\|!cfg\.supabaseAnonKey\)throw new Error/);
  assert.match(login,/passwordSignInButton'\)\.disabled=false/);
  assert.match(login,/googleSignInButton'\)\.disabled=false/);
});

test('both sign-in paths fail visibly instead of calling an uninitialized client',()=>{
  assert.equal((login.match(/if\(!client\)\{showMsg\('Sign-in is still initializing\./g)||[]).length,2);
  assert.match(login,/signInWithOAuth/);
  assert.match(login,/signInWithPassword/);
  assert.doesNotMatch(login,/email:email\.value|password:password\.value/);
});
