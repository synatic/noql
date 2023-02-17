module.exports = [
    {
        name: 'Convert:CONVERT',
        query: "select SUBTRACT(CONVERT('1','int'),ABS(`Replacement Cost`)) as d,Title from `films`",
        output: {
            limit: 100,
            collection: 'films',
            projection: {
                d: {
                    $subtract: [
                        {
                            $convert: {
                                input: {
                                    $literal: '1',
                                },
                                to: 'int',
                            },
                        },
                        {
                            $abs: '$Replacement Cost',
                        },
                    ],
                },
                Title: '$Title',
            },
        },
    },
    {
        name: 'Convert:CAST',
        query: 'select CAST(abs(`id`) as decimal) as `id` from `customers`',
        output: {
            collection: 'customers',
            limit: 100,
            projection: {
                id: {
                    $convert: {
                        input: {
                            $abs: '$id',
                        },
                        to: 'decimal',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:CAST',
        query: "select CAST('123' as int) as `id` from `customers`",
        output: {
            collection: 'customers',
            limit: 100,
            projection: {
                id: {
                    $convert: {
                        input: '123',
                        to: 'int',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:CAST',
        query: 'select CAST(1+`id` as varchar) as `id` from `customers`',
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                id: {
                    $convert: {
                        input: {
                            $add: [1, '$id'],
                        },
                        to: 'string',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:TO_DATE',
        query: "select TO_DATE('2021-12-15T00:00:00Z') as `conv` from `customers`",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $toDate: {
                        $literal: '2021-12-15T00:00:00Z',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:TO_STRING',
        query: 'select TO_STRING(id) as `conv` from `customers`',
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $toString: '$id',
                },
            },
        },
    },
    {
        name: 'Convert:TO_INT',
        query: "select TO_INT('123456') as `conv` from `customers`",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $toInt: {
                        $literal: '123456',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:TO_LONG',
        query: "select TO_LONG('1234567891') as `conv` from `customers`",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $toLong: {
                        $literal: '1234567891',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:TO_BOOL',
        query: "select TO_BOOL('true') as `conv` from `customers`",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $toBool: {
                        $literal: 'true',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:TO_DECIMAL',
        query: "select TO_DECIMAL('123.35') as `conv` from `customers`",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $toDecimal: {
                        $literal: '123.35',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:TO_DOUBLE',
        query: "select TO_DOUBLE('123.35') as `conv` from `customers`",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $toDouble: {
                        $literal: '123.35',
                    },
                },
            },
        },
    },
    {
        name: 'Convert:TYPEOF',
        query: 'select TYPEOF(id) as `conv` from `customers`',
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $type: '$id',
                },
            },
        },
    },
    {
        name: 'Convert:IFNULL',
        query: 'select IFNULL(id,1) as `conv` from `customers`',
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $ifNull: [
                        '$id',
                        {
                            $literal: 1,
                        },
                    ],
                },
            },
        },
    },
    {
        name: 'Convert:IFNULL',
        query: "select IFNULL(NULL,(select 'a' as val,1 as num)) as `conv` from `customers`",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                conv: {
                    $ifNull: [
                        {
                            $literal: null,
                        },
                        {
                            $arrayToObject: {
                                $concatArrays: [
                                    {
                                        $objectToArray: {
                                            num: {
                                                $literal: 1,
                                            },
                                            val: {
                                                $literal: 'a',
                                            },
                                        },
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
        },
    },
    {
        name: 'Convert:to_date on where',
        query: "select to_date('2012-01-01') as i from customers where date > to_date('2012-01-01')",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                i: {
                    $toDate: {
                        $literal: '2012-01-01',
                    },
                },
            },
            query: {
                date: {
                    $gt: new Date('2012-01-01'),
                },
            },
        },
    },
    {
        name: 'Convert:to_date on where no literal',
        query: "select to_date('2012-01-01') as i from customers where to_date(date) > to_date('2012-01-01')",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                i: {
                    $toDate: {
                        $literal: '2012-01-01',
                    },
                },
            },
            query: {
                $expr: {
                    $gt: [
                        {
                            $toDate: '$date',
                        },
                        new Date('2012-01-01'),
                    ],
                },
            },
        },
    },
    {
        name: 'Convert:to_bool on where',
        query: "select to_bool('false') as i from customers where bool = to_bool('true')",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                i: {
                    $toBool: {
                        $literal: 'false',
                    },
                },
            },
            query: {
                bool: {
                    $eq: true,
                },
            },
        },
    },
    {
        name: 'Convert:to_int on where',
        query: "select to_int('123') as i from customers where intval = to_int('123')",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                i: {
                    $toInt: {
                        $literal: '123',
                    },
                },
            },
            query: {
                intval: {
                    $eq: 123,
                },
            },
        },
    },
    {
        name: 'Convert:to_objectid on where',
        query: "select to_objectid('61b0fdcbdee485f7c0682db6') as i from customers where _id = to_objectid('61b0fdcbdee485f7c0682db6')",
        output: {
            limit: 100,
            collection: 'customers',
            projection: {
                i: {
                    $toObjectId: {
                        $literal: '61b0fdcbdee485f7c0682db6',
                    },
                },
            },
            query: {
                _id: {
                    $eq: {
                        _bsontype: 'ObjectID',
                        id: 'a°ýËÞä÷Àh-¶',
                    },
                },
            },
        },
    },
    {
        name: 'Query:in with primitive dates',
        query: " select * from films where Rating in (to_date('2021-05-10'),'X')",
        output: {
            limit: 100,
            collection: 'films',
            query: {
                Rating: {
                    $in: [new Date('2021-05-10T00:00:00.000Z'), 'X'],
                },
            },
        },
    },
    {
        name: 'Convert:to_objectid on where with error',
        query: "select to_objectid('61b0fdcbdee485f7c0682db6') as i from customers where _id = to_objectid('xxx')",
        error: 'Error converting xxx to ObjectId',
    },
    {
        name: 'Convert:to_bool on where with error',
        query: "select to_bool('false') as i from customers where bool = to_bool('xxx')",
        error: 'Error converting xxx to boolean',
    },
];
