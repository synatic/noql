const assert = require('assert');
const SQLParser = require('../../lib/SQLParser.js');
const {normalizeFieldReference} = require('../../lib/make/buildFieldReference');

/**
 * @param {string} sql
 * @param {string} [fieldName]
 * @returns {any}
 */
function parseProjectionField(sql, fieldName = 'activePremiums') {
    const result = SQLParser.parseSQL(sql);
    if (result.type === 'query') {
        assert.ok(
            result.projection && fieldName in result.projection,
            `expected projection field ${fieldName}`
        );
        return result.projection[fieldName];
    }
    const projectStage = (result.pipeline || []).find(
        (stage) => stage.$project
    );
    assert.ok(projectStage, 'expected a $project stage');
    assert.ok(
        fieldName in projectStage.$project,
        `expected $project field ${fieldName}`
    );
    return projectStage.$project[fieldName];
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
            assertNoRootFieldRefsInArrayContext(
                item,
                path.concat(String(index))
            )
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

    describe('backwards compatibility with $this / $$this workarounds', function () {
        it('should still accept mixed $this/$$this hacks in SELECT CASE', function () {
            // Production workaround before includeThis was threaded through CASE /
            // arithmetic: `$this` in comparisons/else, `$$this` in multiply/divide.
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT
                            CASE
                                WHEN \`$this.numberOfTerms\` > 0
                                THEN \`$$this.premium\` * (12 / \`$$this.numberOfTerms\`)
                                ELSE \`$this.premium\`
                            END AS s
                        FROM policies
                    ) AS activePremiums
                FROM \`agencysync-hawksoft-raw-data\`
                WHERE _entity = 'Clients'
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

        it('should still accept mixed $this/$$this hacks in SELECT CASE + WHERE', function () {
            const expr = parseProjectionField(`
                SELECT
                    (
                        SELECT
                            CASE
                                WHEN \`$this.numberOfTerms\` > 0
                                THEN \`$$this.premium\` * (12 / \`$$this.numberOfTerms\`)
                                ELSE \`$this.premium\`
                            END AS \`$$ROOT\`
                        FROM policies
                        WHERE status != 'Lead'
                          AND status != 'Prospect'
                          AND status != 'Refused'
                          AND status != 'DeadFiled'
                          AND status != 'Void'
                          AND status != 'Rejected'
                          AND (
                              TO_DATE(\`$this.expirationDate\`) >= CURRENT_DATE()
                              OR expirationDate = null
                          )
                          AND (
                              TO_DATE(\`$this.effectiveDate\`) <= CURRENT_DATE()
                              OR effectiveDate = null
                          )
                          AND (
                              CASE
                                  WHEN \`$$this.status\` IN (
                                      'NonRenew',
                                      'Cancelled',
                                      'Replaced'
                                  )
                                  AND TO_DATE(\`$this.statusDate\`) < CURRENT_DATE()
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

            const serialized = JSON.stringify(expr);
            assert.strictEqual(
                serialized.includes('$$this.$this.'),
                false,
                'legacy $this columns must not be double-prefixed as $$this.$this.'
            );
            assert.strictEqual(
                serialized.includes('$$this.$$this.'),
                false,
                'legacy $$this columns must not be double-prefixed as $$this.$$this.'
            );
            assert.ok(
                serialized.includes('"$$this.expirationDate"'),
                'TO_DATE($this.expirationDate) workaround should normalize to $$this.expirationDate'
            );
            assert.ok(
                serialized.includes('"$$this.status"'),
                '$$this.status workaround should remain $$this.status'
            );
            assert.ok(
                serialized.includes('"$$this.statusDate"'),
                'TO_DATE($this.statusDate) workaround should normalize to $$this.statusDate'
            );
        });
    });

    describe('parent / root field refs in array subselects', function () {
        describe('normalizeFieldReference', function () {
            it('should bind bare names to $$this when includeThis is set', function () {
                assert.strictEqual(
                    normalizeFieldReference('id', true),
                    '$$this.id'
                );
                assert.strictEqual(
                    normalizeFieldReference('details.mainContacts', true),
                    '$$this.details.mainContacts'
                );
            });

            it('should leave $-prefixed parent paths unchanged when includeThis is set', function () {
                assert.strictEqual(
                    normalizeFieldReference('$details.mainContacts', true),
                    '$details.mainContacts'
                );
                assert.strictEqual(
                    normalizeFieldReference('$status', true),
                    '$status'
                );
            });

            it('should leave $$ROOT / $$NOW system paths unchanged', function () {
                assert.strictEqual(
                    normalizeFieldReference(
                        '$$ROOT.details.mainContacts',
                        true
                    ),
                    '$$ROOT.details.mainContacts'
                );
                assert.strictEqual(
                    normalizeFieldReference('$$NOW', true),
                    '$$NOW'
                );
            });

            it('should still normalize legacy $this / $$this element hacks', function () {
                assert.strictEqual(
                    normalizeFieldReference('$this.id', true),
                    '$$this.id'
                );
                assert.strictEqual(
                    normalizeFieldReference('$$this.id', true),
                    '$$this.id'
                );
                assert.strictEqual(
                    normalizeFieldReference('$$$this.id', true),
                    '$$this.id'
                );
            });

            it('should still prefix bare names with $ outside array subselects', function () {
                assert.strictEqual(normalizeFieldReference('id', false), '$id');
                assert.strictEqual(
                    normalizeFieldReference('$details.mainContacts', false),
                    '$details.mainContacts'
                );
            });
        });

        it('should keep `$parent.path` as a root field in INDEXOF_ARRAY while binding id to $$this', function () {
            const expr = parseProjectionField(
                `
                SELECT
                    (
                        SELECT *
                        FROM people
                        WHERE INDEXOF_ARRAY(\`$details.mainContacts\`, id) > 0
                    ) AS mainContacts
                FROM "agencysync-hawksoft-raw-data"
            `,
                'mainContacts'
            );

            assert.deepStrictEqual(expr, {
                $map: {
                    input: {
                        $filter: {
                            input: '$people',
                            cond: {
                                $and: [
                                    {
                                        $gt: [
                                            {
                                                $indexOfArray: [
                                                    '$details.mainContacts',
                                                    '$$this.id',
                                                ],
                                            },
                                            0,
                                        ],
                                    },
                                ],
                            },
                        },
                    },
                    in: '$$this',
                },
            });
            assert.strictEqual(
                JSON.stringify(expr).includes('$$this.$details'),
                false,
                'parent `$details` must not be prefixed as $$this.$details'
            );
        });

        it('should accept $$ROOT.field as an explicit parent/root path', function () {
            const expr = parseProjectionField(
                `
                SELECT
                    (
                        SELECT *
                        FROM people
                        WHERE INDEXOF_ARRAY(\`$$ROOT.details.mainContacts\`, id) >= 0
                    ) AS mainContacts
                FROM "agencysync-hawksoft-raw-data"
            `,
                'mainContacts'
            );

            assert.deepStrictEqual(expr.$map.input.$filter.cond.$and[0], {
                $gte: [
                    {
                        $indexOfArray: [
                            '$$ROOT.details.mainContacts',
                            '$$this.id',
                        ],
                    },
                    0,
                ],
            });
            assert.strictEqual(
                JSON.stringify(expr).includes('$$this.$$ROOT'),
                false,
                '$$ROOT must not be prefixed as $$this.$$ROOT'
            );
        });

        it('should compare an element field to a parent field', function () {
            const expr = parseProjectionField(
                `
                SELECT
                    (
                        SELECT *
                        FROM people
                        WHERE id = \`$details.ownerId\`
                    ) AS mainContacts
                FROM clients
            `,
                'mainContacts'
            );

            assert.deepStrictEqual(expr.$map.input.$filter.cond.$and[0], {
                $eq: ['$$this.id', '$details.ownerId'],
            });
        });

        it('should project a parent field next to an element field', function () {
            const expr = parseProjectionField(
                `
                SELECT
                    (
                        SELECT
                            id,
                            \`$details.agencyId\` AS agencyId
                        FROM people
                    ) AS mainContacts
                FROM clients
            `,
                'mainContacts'
            );

            assert.deepStrictEqual(expr, {
                $map: {
                    input: '$people',
                    in: {
                        id: '$$this.id',
                        agencyId: '$details.agencyId',
                    },
                },
            });
        });

        it('should keep parent paths in CASE while binding bare columns to $$this', function () {
            const expr = parseProjectionField(
                `
                SELECT
                    (
                        SELECT
                            CASE
                                WHEN INDEXOF_ARRAY(\`$details.mainContacts\`, id) >= 0
                                THEN id
                                ELSE \`$details.ownerId\`
                            END AS s
                        FROM people
                    ) AS mainContacts
                FROM clients
            `,
                'mainContacts'
            );

            assert.deepStrictEqual(expr.$map.in, {
                s: {
                    $switch: {
                        branches: [
                            {
                                case: {
                                    $gte: [
                                        {
                                            $indexOfArray: [
                                                '$details.mainContacts',
                                                '$$this.id',
                                            ],
                                        },
                                        0,
                                    ],
                                },
                                then: '$$this.id',
                            },
                        ],
                        default: '$details.ownerId',
                    },
                },
            });
        });

        it('should not treat a bare parent-shaped path as a root ref', function () {
            const expr = parseProjectionField(
                `
                SELECT
                    (
                        SELECT *
                        FROM people
                        WHERE INDEXOF_ARRAY(details.mainContacts, id) > 0
                    ) AS mainContacts
                FROM clients
            `,
                'mainContacts'
            );

            assert.deepStrictEqual(
                expr.$map.input.$filter.cond.$and[0].$gt[0],
                {
                    $indexOfArray: ['$$this.details.mainContacts', '$$this.id'],
                }
            );
        });
    });
});
