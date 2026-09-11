# Agent Skill

NoQL ships as an [Agent Skill](https://agentskills.io/specification): a folder with `SKILL.md` plus on-demand references. Upload or copy it into Claude, Claude Code, Cursor, or any other agent that implements the standard so the model writes valid NoQL instead of generic SQL.

<div style="margin: 1.5rem 0;">
  <button id="noql-download-btn" onclick="downloadSkillFile()" style="background:#3e8db9;color:#fff;border:none;padding:0.6rem 1.4rem;border-radius:4px;font-size:1rem;cursor:pointer;font-weight:600;letter-spacing:0.02em;">
    ⬇ Download noql.zip
  </button>
</div>

The zip is `noql/` at the root (not loose files), which is the layout [Claude expects](https://support.claude.com/en/articles/12512198-how-to-create-custom-skills).

## Install

**Claude.ai** — Customize → Skills → upload `noql.zip`. Enable the skill. Code execution must be on.

**Claude Code** — unzip into `~/.claude/skills/noql/` (personal) or `.claude/skills/noql/` (this repo).

**Cursor** — unzip into `~/.cursor/skills/noql/` (personal) or `.cursor/skills/noql/` (this repo).

The folder name must stay `noql` to match the `name` field in `SKILL.md`.

## Package contents

```
noql.zip
└── noql/
    ├── SKILL.md              # metadata + core syntax, quoting, caveats
    ├── LICENSE.txt
    └── references/
        ├── joins.md
        ├── arrays.md
        ├── objects.md
        ├── functions.md
        └── examples.md
```

`SKILL.md` is loaded when the skill activates. Reference files are read only when the task needs joins, arrays, objects, or the function catalog.

Source: [`skill/noql`](https://github.com/synatic/noql/tree/main/skill/noql). Human-readable syntax docs: [SQL Syntax](sql-syntax/introduction.md).
