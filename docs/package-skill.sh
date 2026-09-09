#!/usr/bin/env bash
# Package the NoQL Agent Skill as noql.zip for GitHub Pages / Claude upload.
# Zip root must be the skill folder (noql/), not loose files.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKILL_DIR="$ROOT/skill"
OUT="$ROOT/docs/docs/assets/noql.zip"

mkdir -p "$(dirname "$OUT")"
rm -f "$OUT"

(
    cd "$SKILL_DIR"
    zip -r "$OUT" noql \
        -x "*.DS_Store" \
        -x "*/.DS_Store"
)

echo "Wrote $OUT"
unzip -l "$OUT"
