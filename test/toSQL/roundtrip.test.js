const assert = require('assert');
const fs = require('fs');
const path = require('path');
const SQLParser = require('../../lib/SQLParser');

/**
 * Extracts every SELECT statement appearing anywhere in the test corpus
 * (JSON fixtures and .test.js template-literal SQL), for use as a large,
 * organic edge-case corpus for the pipeline -> SQL reverse converter.
 * @returns {Set<string>}
 */
function extractCorpusQueries() {
    const testRoot = path.join(__dirname, '..');
    const queries = new Set();

    function extractFromValue(value) {
        if (typeof value === 'string') {
            const s = value.trim();
            if (/^select\s/i.test(s)) queries.add(s);
        } else if (Array.isArray(value)) {
            value.forEach(extractFromValue);
        } else if (value && typeof value === 'object') {
            Object.values(value).forEach(extractFromValue);
        }
    }

    function walk(dir) {
        for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                walk(full);
                continue;
            }
            if (entry.name.endsWith('.json')) {
                try {
                    extractFromValue(JSON.parse(fs.readFileSync(full, 'utf-8')));
                } catch (e) {
                    // not JSON (or malformed) - skip
                }
            } else if (entry.name.endsWith('.test.js') || entry.name.endsWith('.js')) {
                const content = fs.readFileSync(full, 'utf-8');
                const re = /`([^`]*select[^`]*)`/gis;
                let m;
                while ((m = re.exec(content))) {
                    if (/^\s*select\s/i.test(m[1])) queries.add(m[1].trim());
                }
            }
        }
    }

    walk(testRoot);
    return queries;
}

/**
 * Recursively sorts object keys so pipeline comparisons aren't sensitive to
 * key-insertion order (which carries no semantic meaning for these stages).
 * @param {*} value
 * @returns {*}
 */
function canonicalize(value) {
    if (Array.isArray(value)) return value.map(canonicalize);
    if (value && typeof value === 'object' && !(value instanceof Date)) {
        const out = {};
        for (const key of Object.keys(value).sort()) out[key] = canonicalize(value[key]);
        return out;
    }
    return value;
}
function canonicalJSON(value) {
    return JSON.stringify(canonicalize(value));
}

describe('pipeline -> SQL reverse conversion', function () {
    this.timeout(30000);

    it('round-trips (SQL -> pipeline -> SQL -> pipeline) over the test corpus without producing wrong output', function () {
        const queries = extractCorpusQueries();
        assert.ok(queries.size > 400, `expected a large corpus of queries, got ${queries.size}`);

        let forwardFailed = 0;
        let unsupported = 0;
        let invalidSqlBugs = 0;
        let mismatchBugs = 0;
        let passed = 0;
        const invalidSqlSamples = [];
        const mismatchSamples = [];

        for (const sql of queries) {
            let pipeline, collections;
            try {
                ({pipeline, collections} = SQLParser.makeMongoAggregate(sql));
            } catch (e) {
                // not something this suite is responsible for - the query
                // itself may be invalid, unsupported by the forward
                // converter, or need a DB/schema this test doesn't have.
                forwardFailed++;
                continue;
            }

            let sql2;
            try {
                sql2 = SQLParser.pipelineToSQL(pipeline, collections);
            } catch (e) {
                // an explicit, documented gap (subqueries, UNION, window
                // functions, PIVOT/UNPIVOT, certain array functions, etc.)
                // — acceptable as long as it fails loudly rather than
                // producing wrong SQL.
                unsupported++;
                continue;
            }

            let pipeline2;
            try {
                ({pipeline: pipeline2} = SQLParser.makeMongoAggregate(sql2));
            } catch (e) {
                invalidSqlBugs++;
                invalidSqlSamples.push({sql, sql2, error: e.message});
                continue;
            }

            if (canonicalJSON(pipeline) === canonicalJSON(pipeline2)) {
                passed++;
            } else {
                mismatchBugs++;
                mismatchSamples.push({sql, sql2, pipeline, pipeline2});
            }
        }

        const attempted = unsupported + invalidSqlBugs + mismatchBugs + passed;
        console.log(
            `[reverse round-trip] corpus=${queries.size} forward-failed=${forwardFailed} ` +
                `unsupported=${unsupported} invalid-sql-bugs=${invalidSqlBugs} ` +
                `mismatch-bugs=${mismatchBugs} passed=${passed}/${attempted}`
        );

        // Known, accepted structural gaps as of this writing (see
        // lib/toSQL/pipelineToAST.js's doc comment for the full unsupported
        // list). These thresholds are a regression net, not a target — if
        // they drop, tighten them; if a genuinely new gap is discovered and
        // accepted, bump the threshold with a comment explaining why.
        const MAX_KNOWN_INVALID_SQL_BUGS = 1; // arithmetic binary op as a bare WHERE operand
        const MAX_KNOWN_MISMATCH_BUGS = 7; // e.g. Date-literal WHERE comparisons re-wrap via
        // DATE_FROM_STRING(...), which the forward parser treats as a function
        // call (triggering $expr) rather than the original literal fast-path

        if (invalidSqlBugs > MAX_KNOWN_INVALID_SQL_BUGS) {
            console.log('Invalid SQL samples:', JSON.stringify(invalidSqlSamples, null, 2));
        }
        if (mismatchBugs > MAX_KNOWN_MISMATCH_BUGS) {
            console.log('Mismatch samples:', JSON.stringify(mismatchSamples, null, 2));
        }

        assert.ok(
            invalidSqlBugs <= MAX_KNOWN_INVALID_SQL_BUGS,
            `reverse conversion produced unparseable SQL for ${invalidSqlBugs} quer(ies) (baseline: ${MAX_KNOWN_INVALID_SQL_BUGS})`
        );
        assert.ok(
            mismatchBugs <= MAX_KNOWN_MISMATCH_BUGS,
            `reverse conversion round-tripped to a different pipeline for ${mismatchBugs} quer(ies) (baseline: ${MAX_KNOWN_MISMATCH_BUGS})`
        );
        assert.ok(passed >= 300, `expected at least 300 queries to round-trip cleanly, got ${passed}`);
    });
});
