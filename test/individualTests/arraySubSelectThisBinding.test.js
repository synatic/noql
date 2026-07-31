const assert = require('assert');
const SQLParser = require('../../lib/SQLParser.js');

/**
 * @param {string} sql
 * @returns {any}
 */
function parseProjectionField(sql) {
    const result = SQLParser.parseSQL(sql);
    if (result.type === 'query') {
        return result.projection.activePremiums;
    }
    const projectStage = (result.pipeline || []).find((stage) => stage.$project);
    assert.ok(projectStage, 'expected a $project stage');
    return projectStage.$project.activePremiums;
}

/**
 * Assert every field path string under a node that looks like a document field
 * reference uses $$this. (array element) rather than a root $field.
 * Allows $$NOW and similar system vars.
 * @param {any} node
 * @param {string[]} [path]
 */
function assertNoRootFieldRefsInArrayContext(node, path = []) {
    if (typeof node === 'string') {
        if (
            node.startsWith('$') &&
            !node.startsWith('$$this') &&
            !node.startsWith('$$NOW') &&
            !node.startsWith('$$ROOT') &&
            !node.startsWith('$$REMOVE') &&
            node !== '$$this'
        ) {
            // Allow mapping input path like $policies
            if (path[path.length - 1] === 'input' && !node.includes('.')) {
                return;
            }
            assert.fail(
                `Found root field ref "${node}" at ${path.join('.')} ; ` +
                    'array subselect element fields must use $$this.'
            );
        }
        return;
    }
    if (Array.isArray(node)) {
        node.forEach((item, index) =>
            assertNoRootFieldRefsInArrayContext(item, path.concat(String(index)))
        );
        return;
    }
    if (node && typeof node === 'object') {
        for (const [key, value] of Object.entries(node)) {
            assertNoRootFieldRefsInArrayContext(value, path.concat(key));
        }
    }
}

describe('Array subselect $$this binding', function () {
    describe('SELECT list', function () {
        it('should bind bare columns in CASE to $$this', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT
                            CASE
                                WHEN numberOfTerms > 0 THEN premium * (12 / numberOfTerms)
                                ELSE premium
                            END AS s
                        FROM policies
                    ) AS activePremiums
                FROM clients
            `);

            assert.deepStrictEqual(expr, {
                $map: {
                    input: '$policies',
                    in: {
                        s: {
                            $switch: {
                                branches: [
                                    {
                                        case: {
                                            $gt: ['$$this.numberOfTerms', 0],
                                        },
                                        then: {
                                            $multiply: [
                                                '$$this.premium',
                                                {
                                                    $divide: [
                                                        12,
                                                        '$$this.numberOfTerms',
                                                    ],
                                                },
                                            ],
                                        },
                                    },
                                ],
                                default: '$$this.premium',
                            },
                        },
                    },
                },
            });
        });

        it('should bind bare columns in arithmetic expressions to $$this', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT premium * numberOfTerms AS s
                        FROM policies
                    ) AS activePremiums
                FROM clients
            `);

            assert.deepStrictEqual(expr, {
                $map: {
                    input: '$policies',
                    in: {
                        s: {
                            $multiply: [
                                '$$this.premium',
                                '$$this.numberOfTerms',
                            ],
                        },
                    },
                },
            });
        });

        it('should bind bare columns inside SELECT functions to $$this', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT TO_DATE(expirationDate) AS s
                        FROM policies
                    ) AS activePremiums
                FROM clients
            `);

            assert.deepStrictEqual(expr, {
                $map: {
                    input: '$policies',
                    in: {
                        s: {
                            $toDate: '$$this.expirationDate',
                        },
                    },
                },
            });
        });

        it('should bind CASE AS $$ROOT using $$this field refs', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT
                            CASE
                                WHEN numberOfTerms > 0 THEN premium * (12 / numberOfTerms)
                                ELSE premium
                            END AS \`$$ROOT\`
                        FROM policies
                    ) AS activePremiums
                FROM clients
            `);

            assert.deepStrictEqual(expr, {
                $map: {
                    input: '$policies',
                    in: {
                        $switch: {
                            branches: [
                                {
                                    case: {
                                        $gt: ['$$this.numberOfTerms', 0],
                                    },
                                    then: {
                                        $multiply: [
                                            '$$this.premium',
                                            {
                                                $divide: [
                                                    12,
                                                    '$$this.numberOfTerms',
                                                ],
                                            },
                                        ],
                                    },
                                },
                            ],
                            default: '$$this.premium',
                        },
                    },
                },
            });
        });

        it('should still bind plain column projections to $$this', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT premium AS s
                        FROM policies
                    ) AS activePremiums
                FROM clients
            `);

            assert.deepStrictEqual(expr, {
                $map: {
                    input: '$policies',
                    in: {
                        s: '$$this.premium',
                    },
                },
            });
        });

        it('should not double-prefix SUM_ARRAY inputs already scoped with $$this', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT SUM_ARRAY(Payments, 'Amount') AS s
                        FROM policies
                    ) AS activePremiums
                FROM clients
            `);

            assert.deepStrictEqual(expr, {
                $map: {
                    input: '$policies',
                    in: {
                        s: {
                            $reduce: {
                                input: '$$this.Payments',
                                initialValue: 0,
                                in: {
                                    $sum: ['$$value', '$$this.Amount'],
                                },
                            },
                        },
                    },
                },
            });
        });
    });

    describe('WHERE filter', function () {
        it('should keep binding simple WHERE comparisons to $$this', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT premium AS s
                        FROM policies
                        WHERE numberOfTerms > 0 AND status != 'Lead'
                    ) AS activePremiums
                FROM clients
            `);

            assert.deepStrictEqual(expr.$map.input, {
                $filter: {
                    input: '$policies',
                    cond: {
                        $and: [
                            {
                                $and: [
                                    {
                                        $gt: ['$$this.numberOfTerms', 0],
                                    },
                                    {
                                        $ne: ['$$this.status', 'Lead'],
                                    },
                                ],
                            },
                        ],
                    },
                },
            });
            assert.deepStrictEqual(expr.$map.in, {
                s: '$$this.premium',
            });
        });

        it('should bind bare columns inside WHERE functions to $$this', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT premium AS s
                        FROM policies
                        WHERE TO_DATE(expirationDate) >= CURRENT_DATE()
                           OR expirationDate = null
                    ) AS activePremiums
                FROM clients
            `);

            assertNoRootFieldRefsInArrayContext(expr.$map.input.$filter.cond);
            assert.deepStrictEqual(expr.$map.input.$filter.cond, {
                $and: [
                    {
                        $or: [
                            {
                                $gte: [
                                    {
                                        $toDate: '$$this.expirationDate',
                                    },
                                    '$$NOW',
                                ],
                            },
                            {
                                $eq: ['$$this.expirationDate', null],
                            },
                        ],
                    },
                ],
            });
        });

        it('should bind bare columns inside WHERE CASE to $$this', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT premium AS s
                        FROM policies
                        WHERE (
                            CASE
                                WHEN status IN ('NonRenew', 'Cancelled')
                                     AND TO_DATE(statusDate) < CURRENT_DATE()
                                THEN false
                                ELSE true
                            END
                        )
                    ) AS activePremiums
                FROM clients
            `);

            assertNoRootFieldRefsInArrayContext(expr.$map.input.$filter.cond);
            assert.deepStrictEqual(expr.$map.input.$filter.cond, {
                $and: [
                    {
                        $switch: {
                            branches: [
                                {
                                    case: {
                                        $and: [
                                            {
                                                $in: [
                                                    '$$this.status',
                                                    ['NonRenew', 'Cancelled'],
                                                ],
                                            },
                                            {
                                                $lt: [
                                                    {
                                                        $toDate:
                                                            '$$this.statusDate',
                                                    },
                                                    '$$NOW',
                                                ],
                                            },
                                        ],
                                    },
                                    then: false,
                                },
                            ],
                            default: {
                                $literal: true,
                            },
                        },
                    },
                ],
            });
        });
    });

    describe('combined developer repro', function () {
        it('should not require $this/$$this hacks for CASE + functions in SELECT and WHERE', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT
                            CASE
                                WHEN numberOfTerms > 0 THEN premium * (12 / numberOfTerms)
                                ELSE premium
                            END AS \`$$ROOT\`
                        FROM policies
                        WHERE status != 'Lead'
                          AND status != 'Prospect'
                          AND (TO_DATE(expirationDate) >= CURRENT_DATE() OR expirationDate = null)
                          AND (TO_DATE(effectiveDate) <= CURRENT_DATE() OR effectiveDate = null)
                          AND (
                            CASE
                                WHEN status IN ('NonRenew', 'Cancelled', 'Replaced')
                                     AND TO_DATE(statusDate) < CURRENT_DATE()
                                THEN false
                                ELSE true
                            END
                          )
                    ) AS activePremiums
                FROM \`agencysync-hawksoft-raw-data\`
                WHERE _entity = 'Clients'
            `);

            assertNoRootFieldRefsInArrayContext(expr);

            assert.deepStrictEqual(expr.$map.in, {
                $switch: {
                    branches: [
                        {
                            case: {
                                $gt: ['$$this.numberOfTerms', 0],
                            },
                            then: {
                                $multiply: [
                                    '$$this.premium',
                                    {
                                        $divide: [12, '$$this.numberOfTerms'],
                                    },
                                ],
                            },
                        },
                    ],
                    default: '$$this.premium',
                },
            });

            const filterCond = expr.$map.input.$filter.cond;
            assert.ok(filterCond.$and);
            assertNoRootFieldRefsInArrayContext(filterCond);
        });
    });

    describe('non-array CASE remains root-scoped', function () {
        it('should still use root $field refs for top-level CASE', function () {
            const result = SQLParser.parseSQL(`
                SELECT
                    CASE
                        WHEN numberOfTerms > 0 THEN premium
                        ELSE 0
                    END AS s
                FROM policies
            `);

            assert.deepStrictEqual(result.projection.s, {
                $switch: {
                    branches: [
                        {
                            case: {
                                $gt: ['$numberOfTerms', 0],
                            },
                            then: '$premium',
                        },
                    ],
                    default: {
                        $literal: 0,
                    },
                },
            });
        });
    });
});
