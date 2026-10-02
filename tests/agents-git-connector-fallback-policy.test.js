"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const agents = fs.readFileSync(path.join(__dirname, "..", "AGENTS.md"), "utf8");

test("AGENTS defines the Connector-first managed-worktree delivery model", () => {
  for (const requiredText of [
    "## Zorunlu Codex Windows çalışma ve GitHub teslim modeli",
    "GitHub `main` ürün kaynağı ve başlangıç otoritesidir.",
    "aktif geliştirme alanı değildir",
    "görev bazlı Codex-managed worktree'de yapılır",
    "birincil uzak yol GitHub Connector'dır",
    "güncel uzak `main` kanıtı yoksa implementasyona başlanmaz",
    "Güncel blob SHA/content yeniden okunur",
    "PR ve zorunlu CI PASS olmadan iş merge-ready sayılmaz",
    "Merge ayrıca açık kullanıcı onayı gerektirir.",
  ]) {
    assert.ok(agents.includes(requiredText), `Missing delivery control: ${requiredText}`);
  }
});

test("Connector-first policy fails closed without web, local Git repair, or system mutation", () => {
  for (const safetyBoundary of [
    "GitHub web editörü veya tarayıcı üzerinden repository mutation yalnız kullanıcı bunu açıkça isterse kullanılabilir.",
    "Yerel Git/GCM/ACL, elevated/unelevated işlem, process sonlandırma, servis/VM/server yeniden başlatma",
    "Connector kullanılamıyorsa web editörü, yerel Git onarımı veya server müdahalesine geçilmez",
    "Ana Local klasör veya ortak `.git`, ona bağlı worktree'ler kapanmadan silinmez",
  ]) {
    assert.ok(agents.includes(safetyBoundary), `Missing safety boundary: ${safetyBoundary}`);
  }
});

test("AGENTS enforces a zero local-only work finish gate", () => {
  for (const requiredText of [
    "## Zorunlu iş bitiş kapısı: local-only iş sıfır",
    "doğrulanmamış local-only proje içeriği sıfır olmalıdır",
    "beklenen commit SHA ve exact content/hash doğrulanır",
    "merge bekleyen çalışma dahi GitHub branch/PR üzerinde dayanıklı olmalıdır",
    "Yerel stash, reflog, worktree veya makine yedeği tek başına dayanıklı teslim ya da uzak yedek sayılmaz",
    "uzak eşdeğeri ve kurtarılabilirliği doğrulanmadan silinmez",
    "Repository ZIP veya günlük makine yedeği ikincil kurtarma katmanıdır",
    "Merge sonrası görev worktree'si arşivlenir",
  ]) {
    assert.ok(agents.includes(requiredText), `Missing finish-gate control: ${requiredText}`);
  }
});
