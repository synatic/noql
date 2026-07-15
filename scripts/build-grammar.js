const fs = require('fs');
const path = require('path');
const peggy = require('peggy');

const GRAMMAR_PATH = path.join(__dirname, '../pegjs/noql.pegjs');
const OUTPUT_PATH = path.join(__dirname, '../lib/parser/noqlGrammar.js');

const source = fs.readFileSync(GRAMMAR_PATH, 'utf-8');
const generated = peggy.generate(source, {output: 'source', format: 'commonjs'});

fs.mkdirSync(path.dirname(OUTPUT_PATH), {recursive: true});
fs.writeFileSync(
    OUTPUT_PATH,
    `// Generated from pegjs/noql.pegjs by scripts/build-grammar.js. Do not edit directly.\n${generated}`
);

console.log(`Generated ${path.relative(process.cwd(), OUTPUT_PATH)}`);
