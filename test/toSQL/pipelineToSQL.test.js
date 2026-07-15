const assert = require('assert');
const SQLParser = require('../../lib/SQLParser');

/**
 * Asserts that converting `sql` to a pipeline and back to SQL produces a
 * pipeline that is functionally identical to the original (re-parsing the
 * reconstructed SQL must reproduce the exact same pipeline).
 * @param {string} sql
 * @returns {string} the reconstructed SQL, for optional further assertions
 */
function assertRoundTrips(sql) {
    const {pipeline, collections} = SQLParser.makeMongoAggregate(sql);
    const sql2 = SQLParser.pipelineToSQL(pipeline, collections);
    const {pipeline: pipeline2} = SQLParser.makeMongoAggregate(sql2);
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

    it('throws a clear error for unsupported constructs instead of producing wrong SQL', function () {
        const {pipeline, collections} = SQLParser.makeMongoAggregate(
            'select * from t1 union select * from t2'
        );
        assert.throws(() => SQLParser.pipelineToSQL(pipeline, collections), /Unsupported/);
    });
});
