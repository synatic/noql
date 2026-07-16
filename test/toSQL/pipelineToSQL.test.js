const assert = require('assert');
const SQLParser = require('../../lib/SQLParser');

/**
 * Asserts that converting `sql` to a pipeline and back to SQL produces a
 * pipeline that is functionally identical to the original (re-parsing the
 * reconstructed SQL must reproduce the exact same pipeline).
 * @param {string} sql
 * @param {object} [options] - forward-conversion options (e.g. optimizeJoins)
 * @returns {string} the reconstructed SQL, for optional further assertions
 */
function assertRoundTrips(sql, options) {
    const {pipeline, collections} = SQLParser.makeMongoAggregate(sql, options);
    const sql2 = SQLParser.pipelineToSQL(pipeline, collections);
    const {pipeline: pipeline2} = SQLParser.makeMongoAggregate(sql2, options);
    assert.deepStrictEqual(
        pipeline2,
        pipeline,
        `expected "${sql2}" to produce the same pipeline as "${sql}"`
    );
    return sql2;
}

describe('SQLParser.pipelineToSQL', function () {
    it('round-trips a plain SELECT with WHERE', function () {
        assertRoundTrips("select a, b as c from mytable where x > 1 and y < 2");
    });

    it('round-trips comparisons, LIKE, IN, and IS NULL', function () {
        assertRoundTrips(
            "select * from t where a in (1,2,3) and b like '%foo%' and c is not null"
        );
    });

    it('round-trips an INNER JOIN with a simple equi-join', function () {
        assertRoundTrips(
            'select * from t1 s inner join t2 j on s.id = j.id where s.a >= 5'
        );
    });

    it('round-trips a LEFT JOIN with a |first hint and complex ON condition', function () {
        assertRoundTrips(
            "select c.*,cn.* from customers c left outer join `customer-notes|first` as cn on cn.id=c.id and cn.id<5"
        );
    });

    it('round-trips GROUP BY with an aggregate and HAVING', function () {
        assertRoundTrips(
            'select Category,count(1) as cnt from films group by Category having cnt > 5'
        );
    });

    it('round-trips COUNT(DISTINCT ...)', function () {
        assertRoundTrips(
            'select Category,count(distinct Rating) as cntDis from films group by Category'
        );
    });

    it('round-trips CASE WHEN', function () {
        assertRoundTrips(
            "select id, case when item not in ('a','b') then 1 else 0 end as x from orders"
        );
    });

    it('round-trips ORDER BY / LIMIT / OFFSET', function () {
        assertRoundTrips('select * from t order by a desc, b asc limit 10 offset 5');
    });

    it('round-trips UNSET(...)', function () {
        assertRoundTrips("select *, unset(_id) from orders where orderDate <= '2024-01-01'");
    });

    it('round-trips the originally-reported nested-alias bug query', function () {
        assertRoundTrips("select * from mytable s where LOWER(s.a.b) != 'error'");
    });

    it('pretty-prints SQL across multiple lines by default', function () {
        const {pipeline, collections} = SQLParser.makeMongoAggregate(
            "select a, b as c from mytable t where x > 1 and y < 2 limit 10"
        );
        const sql = SQLParser.pipelineToSQL(pipeline, collections);
        assert.match(sql, /^SELECT\n/m);
        assert.match(sql, /\nFROM /);
        assert.match(sql, /\nWHERE /);
        assert.match(sql, /\nLIMIT /);
        assert.doesNotMatch(sql, /^SELECT .+ FROM /);
        // still round-trips when re-parsed
        const {pipeline: pipeline2} = SQLParser.makeMongoAggregate(sql);
        assert.deepStrictEqual(pipeline2, pipeline);
    });

    it('can emit a single-line SQL string when pretty:false', function () {
        const {pipeline, collections} = SQLParser.makeMongoAggregate(
            'select a from mytable where x > 1'
        );
        const sql = SQLParser.pipelineToSQL(pipeline, collections, {pretty: false});
        assert.doesNotMatch(sql, /\n/);
        assert.match(sql, /^SELECT .+ FROM /);
    });

    it('accepts a full Mongo aggregate command document ({aggregate, pipeline})', function () {
        const {pipeline, collections} = SQLParser.makeMongoAggregate(
            "select a from mytable where x > 1"
        );
        const command = {aggregate: collections[0], pipeline, allowDiskUse: true};
        const sqlFromArray = SQLParser.pipelineToSQL(pipeline, collections);
        const sqlFromCommand = SQLParser.pipelineToSQL(command);
        assert.strictEqual(sqlFromCommand, sqlFromArray);
    });

    it('throws a clear error when neither a pipeline array nor a command document is provided', function () {
        assert.throws(
            () => SQLParser.pipelineToSQL({aggregate: 't'}, ['t']),
            /pipeline array|aggregate command/
        );
    });

    it('throws a clear error for unsupported constructs instead of producing wrong SQL', function () {
        const {pipeline, collections} = SQLParser.makeMongoAggregate(
            'select * from t1 union select * from t2'
        );
        assert.throws(() => SQLParser.pipelineToSQL(pipeline, collections), /Unsupported/);
    });

    it('throws for a FROM sub-select instead of misreading it as a hoisted WHERE fragment', function () {
        const {pipeline, collections} = SQLParser.makeMongoAggregate(
            'select * from (select clanId, count(1) as userCount from films group by clanId) c order by c.userCount desc'
        );
        assert.throws(() => SQLParser.pipelineToSQL(pipeline, collections), /Unsupported/);
    });

    it('round-trips DATE_FROM_PARTS', function () {
        assertRoundTrips(
            "select DATE_FROM_PARTS(2024,1,1) as d from orders where orderDate > DATE_FROM_PARTS(2020,6,15)"
        );
    });

    describe('joins against a derived table (subquery joins)', function () {
        it('round-trips a simple derived-table join with a WHERE clause', function () {
            assertRoundTrips(
                "select c.*, o.* from customers c left join (select * from orders where status = 'shipped') o on o.customerId = c.id"
            );
        });

        it('round-trips a derived-table join with the |optimize hint', function () {
            const sql2 = assertRoundTrips(
                "select c.*, cn.* from customers c inner join (select * from `customer-notes` where id > 2) `cn|optimize` on cn.id = c.id"
            );
            assert.match(sql2, /`cn\|optimize`/);
        });

        it('round-trips a derived-table join combining |first and |optimize hints', function () {
            const sql2 = assertRoundTrips(
                "select c.*, cn.* from customers c inner join (select * from `customer-notes` where id > 2) `cn|first|optimize` on cn.id = c.id"
            );
            assert.match(sql2, /`cn\|(first\|optimize|optimize\|first)`/);
        });

        it('round-trips a derived-table join whose own body has a nested join (correlation on the nested alias)', function () {
            assertRoundTrips(
                'select c.*, h.* from customers c left join (select * from hazards h inner join policies p on h.policyId = p.id where h.classcode in (1,2,3)) h on h.amspolicies.customerId = c.id'
            );
        });
    });

    describe('optimizer-reordered pipelines', function () {
        it('round-trips a pipeline where optimizeJoins hoists WHERE conditions above joins', function () {
            assertRoundTrips(
                "select pol.customerId, cust.name from policies pol inner join customers cust on pol.customerId = cust.customerId where pol.subType = 'P' and (pol.status = 'A' or pol.status = 'R')",
                {optimizeJoins: true}
            );
        });

        it('reverses a pipeline produced by optimizeMongoAggregate (WHERE hoisted above the root-wrap project)', function () {
            const sql =
                "select pol.customerId, cust.name from policies pol inner join customers cust on pol.customerId = cust.customerId where pol.subType = 'P' and (pol.status = 'A' or pol.status = 'R')";
            const parsed = SQLParser.makeMongoAggregate(sql, {optimizeJoins: true});
            const optimized = SQLParser.optimizeMongoAggregate(parsed.pipeline, {});
            const sql2 = SQLParser.pipelineToSQL(optimized, parsed.collections);
            const reparsed = SQLParser.makeMongoAggregate(sql2, {optimizeJoins: true});
            const reoptimized = SQLParser.optimizeMongoAggregate(reparsed.pipeline, {});
            assert.deepStrictEqual(
                reoptimized,
                optimized,
                'reversing an optimized pipeline and re-optimizing the result should reach a stable fixed point'
            );
        });
    });
});
