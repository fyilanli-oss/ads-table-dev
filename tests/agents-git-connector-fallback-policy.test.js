"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const agents = fs.readFileSync(path.join(__dirname, "..", "AGENTS.md"), "utf8");

test("AGENTS defines a fail-closed GitHub connector fallback for local Git metadata failures", () => {
  for (const requiredText of [
    "## Yerel Git yazma kapısı ve GitHub connector fallback politikası",
    "GitHub ağı/API'si, yerel repository bütünlüğü ve Codex sandbox/ACL",
    "Stash'ler, bağlı worktree'ler ve upstream'siz/ahead yerel branch uçları ayrıca envanterlenir.",
    "GitHub'da bulunmayan veya eşdeğerliği kanıtlanmamış yerel içerik varsa otomatik uzak yazım",
    "kullanıcının açık onayıyla GitHub connector fallback kullanılabilir",
    "güncel blob SHA yeniden okunur",
    "Contents API yazımları aynı path için seri yapılır",
    "zorunlu CI/testleri PASS olmadan",
    "Kullanıcı ayrıca açıkça istemeden PR merge edilmez.",
  ]) {
    assert.ok(agents.includes(requiredText), `Missing fallback control: ${requiredText}`);
  }
});

test("fallback policy forbids destructive local recovery and unrelated uploads", () => {
  for (const forbiddenAction of [
    "ACL, sahiplik, process sonlandırma, servis/VM yeniden başlatma",
    "reset, checkout, stash apply/drop veya overwrite yapılmaz",
    "Ignored dosyalar, secret'lar, build çıktıları ve unrelated yerel değişiklikler uzak repository'ye taşınmaz.",
  ]) {
    assert.ok(agents.includes(forbiddenAction), `Missing safety boundary: ${forbiddenAction}`);
  }
});
