const assert = require('assert');
const optimizer = require('../lib/optimizer');
const SQLParser = require('../lib/SQLParser');

describe('Optimizer', function () {
    // todo create separate tests as well as generlized ones

    describe('Optimize Aggregate', function () {
        it('should optimize a powerbi dropdown query', function () {
            // used by powerbi for dropdowns
            const sql = `select "_"."StartOfWeekYear" as "c98"
                         from
                             (
                                 select "StartOfWeekYear",
                                        "_"."t0_0" as "t0_0",
                                        "_"."t1_0" as "t1_0"
                                 from
                                     (
                                         select "_"."StartOfWeekYear",
                                                "_"."o0",
                                                "_"."t0_0",
                                                "_"."t1_0"
                                         from
                                             (
                                                 select "_"."StartOfWeekYear" as "StartOfWeekYear",
                                                        "_"."o0" as "o0",
                                                        case
                                                            when "_"."o0" is not null
                                                                then "_"."o0"
                                                            else 0
                                                            end as "t0_0",
                                                        case
                                                            when "_"."o0" is null
                                                                then 0
                                                            else 1
                                                            end as "t1_0"
                                                 from
                                                     (
                                                         select "rows"."StartOfWeekYear" as "StartOfWeekYear",
                                                                "rows"."o0" as "o0"
                                                         from
                                                             (
                                                                 select "StartOfWeekYear" as "StartOfWeekYear",
                                                                        "StartOfWeekYear" as "o0"
                                                                 from "public"."Activities" "$Table"
                                                             ) "rows"
                                                         group by "StartOfWeekYear",
                                                                  "o0"
                                                     ) "_"
                                             ) "_"
                                     ) "_"
                             ) "_"
                         order by "_"."t0_0",
                                  "_"."t1_0"
                             limit 101
            `;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $group: {
                            _id: {
                                StartOfWeekYear: '$StartOfWeekYear',
                                o0: '$StartOfWeekYear',
                            },
                        },
                    },
                    {
                        $project: {
                            StartOfWeekYear: '$_id.StartOfWeekYear',
                            o0: '$_id.o0',
                            _id: 0,
                        },
                    },
                    {
                        $project: {
                            StartOfWeekYear: '$StartOfWeekYear',
                            t0_0: {
                                $switch: {
                                    branches: [
                                        {
                                            case: {
                                                $ne: ['$o0', null],
                                            },
                                            then: '$o0',
                                        },
                                    ],
                                    default: {
                                        $literal: 0,
                                    },
                                },
                            },
                            t1_0: {
                                $switch: {
                                    branches: [
                                        {
                                            case: {
                                                $eq: ['$o0', null],
                                            },
                                            then: 0,
                                        },
                                    ],
                                    default: {
                                        $literal: 1,
                                    },
                                },
                            },
                        },
                    },
                    {
                        $sort: {
                            t0_0: 1,
                            t1_0: 1,
                        },
                    },
                    {
                        $project: {
                            c98: '$StartOfWeekYear',
                        },
                    },
                    {
                        $limit: 101,
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a cast decimal', function () {
            // used by powerbi for dropdowns
            const sql = `SELECT "_"."_id"         AS "t1._id",
                                "_"."policyid"    AS "t1.PolicyID",
                                "_"."productname" AS "t1.ProductName"
                         FROM   "dwh-data-views-policies" AS "_"
                         WHERE  Cast("_"."startofweekdatecreatedyear" AS DECIMAL) = Cast(2025 AS DECIMAL) `;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            $expr: {
                                $eq: [
                                    {
                                        $convert: {
                                            input: '$startofweekdatecreatedyear',
                                            to: 'decimal',
                                        },
                                    },
                                    {
                                        $convert: {
                                            input: 2025,
                                            to: 'decimal',
                                        },
                                    },
                                ],
                            },
                        },
                    },
                    {
                        $project: {
                            't1._id': '$_id',
                            't1.PolicyID': '$policyid',
                            't1.ProductName': '$productname',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi basic filter query', function () {
            // used by powerbi for dropdowns
            const sql = `select "rows"."ActivityType" as "ActivityType"
                         from
                             (
                                 select "_"."_id",
                                        "_"."ActivityID",
                                        "_"."ActivityDate",
                                        "_"."ActivityDateDay",
                                        "_"."ActivityDateMonth",
                                        "_"."ActivityDateQuarter",
                                        "_"."ActivityDateYear",
                                        "_"."ActivityType"
                                 from "public"."Activities" "_"
                                 where "_"."Status" = 'Complete' and ("_"."StartOfWeekYear" in (2024, 2023))
                             ) "rows"
                         group by "ActivityType"
                             limit 1000001
       `;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            $and: [
                                {
                                    Status: {
                                        $eq: 'Complete',
                                    },
                                },
                                {
                                    StartOfWeekYear: {
                                        $in: [2024, 2023],
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $group: {
                            _id: {
                                ActivityType: '$ActivityType',
                            },
                        },
                    },
                    {
                        $project: {
                            ActivityType: '$_id.ActivityType',
                            _id: 0,
                        },
                    },
                    {
                        $limit: 1000001,
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi matrix date hiearchy', function () {
            // used by powerbi for dropdowns
            const sql = `select "rows"."StartOfWeekYear" as "StartOfWeekYear",
                                count("rows"."ActivityID") as "a0"
                         from
                             (
                                 select "_"."_id",
                                        "_"."ActivityID",
                                        "_"."ActivityDate",
                                     "_"."StartOfWeekYear"
                                 from "public"."Activities" "_"
                                 where (("_"."StartOfWeekYear" in (2024, 2023)) and "_"."ActivityType" = 'Learning Opportunity ') and "_"."Status" = 'Complete'
                             ) "rows"
                         group by "StartOfWeekYear"
                             limit 1000001
       `;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            $and: [
                                {
                                    $and: [
                                        {
                                            StartOfWeekYear: {
                                                $in: [2024, 2023],
                                            },
                                        },
                                        {
                                            ActivityType: {
                                                $eq: 'Learning Opportunity ',
                                            },
                                        },
                                    ],
                                },
                                {
                                    Status: {
                                        $eq: 'Complete',
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $group: {
                            _id: {
                                StartOfWeekYear: '$StartOfWeekYear',
                            },
                            a0: {
                                $sum: 1,
                            },
                        },
                    },
                    {
                        $project: {
                            StartOfWeekYear: '$_id.StartOfWeekYear',
                            _id: 0,
                            a0: '$a0',
                        },
                    },
                    {
                        $limit: 1000001,
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi matrix date hiearchy 2', function () {
            // used by powerbi for dropdowns
            const sql = `select "rows"."StartOfWeekMonth" as "StartOfWeekMonth",
                                "rows"."StartOfWeekQuarter" as "StartOfWeekQuarter",
                                count("rows"."ActivityID") as "a0"
                         from
                             (
                                 select "_"."_id",
                                        "_"."ActivityID",

                                        "_"."StartOfWeek",
                                        "_"."StartOfWeekDay",
                                        "_"."StartOfWeekMonth",
                                        "_"."StartOfWeekQuarter",
                                        "_"."StartOfWeekYear",
                                        "_"."Status",
                                        "_"."StatusLookupID",
                                        "_"."Subject"

                                 from "public"."Activities" "_"
                                 where (("_"."ActivityType" = 'Learning Opportunity ' and cast("_"."StartOfWeekYear" as decimal) = cast(2024 as decimal)) and "_"."Status" = 'Complete') and (cast("_"."StartOfWeekYear" as decimal) = cast(2024 as decimal) and cast("_"."StartOfWeekQuarter" as decimal) = cast(3 as decimal) or cast("_"."StartOfWeekYear" as decimal) = cast(2024 as decimal) and cast("_"."StartOfWeekQuarter" as decimal) = cast(4 as decimal))
                             ) "rows"
                         group by "StartOfWeekMonth",
                                  "StartOfWeekQuarter"
                             limit 1000001
       `;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            $and: [
                                {
                                    $and: [
                                        {
                                            $and: [
                                                {
                                                    ActivityType: {
                                                        $eq: 'Learning Opportunity ',
                                                    },
                                                },
                                                {
                                                    $expr: {
                                                        $eq: [
                                                            {
                                                                $convert: {
                                                                    input: '$StartOfWeekYear',
                                                                    to: 'decimal',
                                                                },
                                                            },
                                                            {
                                                                $convert: {
                                                                    input: 2024,
                                                                    to: 'decimal',
                                                                },
                                                            },
                                                        ],
                                                    },
                                                },
                                            ],
                                        },
                                        {
                                            Status: {
                                                $eq: 'Complete',
                                            },
                                        },
                                    ],
                                },
                                {
                                    $and: [
                                        {
                                            $or: [
                                                {
                                                    $and: [
                                                        {
                                                            $expr: {
                                                                $eq: [
                                                                    {
                                                                        $convert:
                                                                            {
                                                                                input: '$StartOfWeekYear',
                                                                                to: 'decimal',
                                                                            },
                                                                    },
                                                                    {
                                                                        $convert:
                                                                            {
                                                                                input: 2024,
                                                                                to: 'decimal',
                                                                            },
                                                                    },
                                                                ],
                                                            },
                                                        },
                                                        {
                                                            $expr: {
                                                                $eq: [
                                                                    {
                                                                        $convert:
                                                                            {
                                                                                input: '$StartOfWeekQuarter',
                                                                                to: 'decimal',
                                                                            },
                                                                    },
                                                                    {
                                                                        $convert:
                                                                            {
                                                                                input: 3,
                                                                                to: 'decimal',
                                                                            },
                                                                    },
                                                                ],
                                                            },
                                                        },
                                                    ],
                                                },
                                                {
                                                    $expr: {
                                                        $eq: [
                                                            {
                                                                $convert: {
                                                                    input: '$StartOfWeekYear',
                                                                    to: 'decimal',
                                                                },
                                                            },
                                                            {
                                                                $convert: {
                                                                    input: 2024,
                                                                    to: 'decimal',
                                                                },
                                                            },
                                                        ],
                                                    },
                                                },
                                            ],
                                        },
                                        {
                                            $expr: {
                                                $eq: [
                                                    {
                                                        $convert: {
                                                            input: '$StartOfWeekQuarter',
                                                            to: 'decimal',
                                                        },
                                                    },
                                                    {
                                                        $convert: {
                                                            input: 4,
                                                            to: 'decimal',
                                                        },
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                },
                            ],
                        },
                    },
                    {
                        $group: {
                            _id: {
                                StartOfWeekMonth: '$StartOfWeekMonth',
                                StartOfWeekQuarter: '$StartOfWeekQuarter',
                            },
                            a0: {
                                $sum: 1,
                            },
                        },
                    },
                    {
                        $project: {
                            StartOfWeekMonth: '$_id.StartOfWeekMonth',
                            StartOfWeekQuarter: '$_id.StartOfWeekQuarter',
                            _id: 0,
                            a0: '$a0',
                        },
                    },
                    {
                        $limit: 1000001,
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi matrix date hiearchy 3', function () {
            // used by powerbi for dropdowns
            const sql = `select "_"."StartOfWeekYear" as "c98"
from
(
    select "StartOfWeekYear",
        "_"."t0_0" as "t0_0",
        "_"."t1_0" as "t1_0"
    from
    (
        select "_"."StartOfWeekYear",
            "_"."o0",
            "_"."t0_0",
            "_"."t1_0"
        from
        (
            select "_"."StartOfWeekYear" as "StartOfWeekYear",
                "_"."o0" as "o0",
                case
                    when "_"."o0" is not null
                    then "_"."o0"
                    else 0
                end as "t0_0",
                case
                    when "_"."o0" is null
                    then 0
                    else 1
                end as "t1_0"
            from
            (
                select "rows"."StartOfWeekYear" as "StartOfWeekYear",
                    "rows"."o0" as "o0"
                from
                (
                    select "StartOfWeekYear" as "StartOfWeekYear",
                        "StartOfWeekYear" as "o0"
                    from "public"."Activities" "$Table"
                ) "rows"
                group by "StartOfWeekYear",
                    "o0"
            ) "_"
        ) "_"
    ) "_"
) "_"
order by "_"."t0_0",
        "_"."t1_0"
limit 101
       `;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $group: {
                            _id: {
                                StartOfWeekYear: '$StartOfWeekYear',
                                o0: '$StartOfWeekYear',
                            },
                        },
                    },
                    {
                        $project: {
                            StartOfWeekYear: '$_id.StartOfWeekYear',
                            o0: '$_id.o0',
                            _id: 0,
                        },
                    },
                    {
                        $project: {
                            StartOfWeekYear: '$StartOfWeekYear',
                            t0_0: {
                                $switch: {
                                    branches: [
                                        {
                                            case: {
                                                $ne: ['$o0', null],
                                            },
                                            then: '$o0',
                                        },
                                    ],
                                    default: {
                                        $literal: 0,
                                    },
                                },
                            },
                            t1_0: {
                                $switch: {
                                    branches: [
                                        {
                                            case: {
                                                $eq: ['$o0', null],
                                            },
                                            then: 0,
                                        },
                                    ],
                                    default: {
                                        $literal: 1,
                                    },
                                },
                            },
                        },
                    },
                    {
                        $sort: {
                            t0_0: 1,
                            t1_0: 1,
                        },
                    },
                    {
                        $project: {
                            c98: '$StartOfWeekYear',
                        },
                    },

                    {
                        $limit: 101,
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi matrix date hiearchy 4', function () {
            // used by powerbi for dropdowns
            const sql = ` select "rows"."CreatedByUserFullName" as "CreatedByUserFullName",
                                 "rows"."StartOfWeekQuarter" as "StartOfWeekQuarter",
                                 "rows"."StartOfWeekYear" as "StartOfWeekYear",
                                 count("rows"."ActivityID") as "a0"
                          from
                              (
                                  select "_"."_id",
                                         "_"."ActivityID",
                                         "_"."ActivityDate",
                                         "_"."CreatedByUserFullName",
                                         "_"."FollowUpDateEnd",
                                         "_"."FollowUpDateMonth",
                                         "_"."FollowUpDateQuarter",
                                         "_"."FollowUpDateYear",
                                         "_"."FollowUpTeamName",
                                         "_"."FollowUpUserActive",
                                         "_"."FollowUpUserEmail",
                                         "_"."FollowUpUserFullName",
                                         "_"."StartOfWeek",
                                         "_"."StartOfWeekDay",
                                         "_"."StartOfWeekMonth",
                                         "_"."StartOfWeekQuarter",
                                         "_"."StartOfWeekYear",
                                         "_"."Status",
                                         "_"."Last12Weeks"
                                  from "public"."Activities" "_"
                                  where (("_"."StartOfWeekYear" in (2024, 2023)) and (case
                                                                                          when "_"."Last12Weeks" is null
                                                                                              then null
                                                                                          when "_"."Last12Weeks" = true
                                                                                              then 1
                                                                                          else 0
                                      end) = 1) and ("_"."CreatedByUserFullName" in ('user1', 'user2'))
                              ) "rows"
                          group by "CreatedByUserFullName",
                                   "StartOfWeekQuarter",
                                   "StartOfWeekYear"
                              limit 1000001
       `;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            $and: [
                                {
                                    $and: [
                                        {
                                            StartOfWeekYear: {
                                                $in: [2024, 2023],
                                            },
                                        },
                                        {
                                            $expr: {
                                                $eq: [
                                                    {
                                                        $switch: {
                                                            branches: [
                                                                {
                                                                    case: {
                                                                        $eq: [
                                                                            '$Last12Weeks',
                                                                            null,
                                                                        ],
                                                                    },
                                                                    then: null,
                                                                },
                                                                {
                                                                    case: {
                                                                        $eq: [
                                                                            '$Last12Weeks',
                                                                            true,
                                                                        ],
                                                                    },
                                                                    then: 1,
                                                                },
                                                            ],
                                                            default: {
                                                                $literal: 0,
                                                            },
                                                        },
                                                    },
                                                    1,
                                                ],
                                            },
                                        },
                                    ],
                                },
                                {
                                    CreatedByUserFullName: {
                                        $in: ['user1', 'user2'],
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $group: {
                            _id: {
                                CreatedByUserFullName: '$CreatedByUserFullName',
                                StartOfWeekQuarter: '$StartOfWeekQuarter',
                                StartOfWeekYear: '$StartOfWeekYear',
                            },
                            a0: {
                                $sum: 1,
                            },
                        },
                    },
                    {
                        $project: {
                            CreatedByUserFullName: '$_id.CreatedByUserFullName',
                            StartOfWeekQuarter: '$_id.StartOfWeekQuarter',
                            StartOfWeekYear: '$_id.StartOfWeekYear',
                            _id: 0,
                            a0: '$a0',
                        },
                    },
                    {
                        $limit: 1000001,
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi  query 1', function () {
            const sql = `
select "_"."AgentFullName" as "c122",
    "_"."CarrierName" as "c134",
    "_"."DateCreated" as "c153",
    "_"."HolderName" as "c179"
from
(
    select "AgentFullName",
    "CarrierName",
    "DateCreated",
    "HolderName",
    "_"."t0_0" as "t0_0",
    "_"."t1_0" as "t1_0"
from
(
    select "_"."AgentFullName",
    "_"."CarrierName",
    "_"."DateCreated",
    "_"."HolderName",
    "_"."o0",
    "_"."t0_0",
    "_"."t1_0"
from
(
    select "_"."AgentFullName" as "AgentFullName",
    "_"."CarrierName" as "CarrierName",
    "_"."DateCreated" as "DateCreated",
    "_"."HolderName" as "HolderName",
    "_"."o0" as "o0",
case
when "_"."o0" is not null
then "_"."o0"
else  '1899-12-28 00:00:00'
end as "t0_0",
case
when "_"."o0" is null
then 0
else 1
end as "t1_0"
from
(
    select "rows"."AgentFullName" as "AgentFullName",
    "rows"."CarrierName" as "CarrierName",
    "rows"."DateCreated" as "DateCreated",
    "rows"."HolderName" as "HolderName",
    "rows"."o0" as "o0"
from
(
    select "_"."AgentFullName" as "AgentFullName",
    "_"."CarrierName" as "CarrierName",
    "_"."DateCreated" as "DateCreated",
    "_"."HolderName" as "HolderName",
    "_"."PolicyStatus" as "PolicyStatus",
    "_"."DateCreated" as "o0"
from
(
    select "AgentFullName",
    "CarrierName",
    "DateCreated",
    "HolderName",
    "PolicyStatus"
from "public"."Policies" "$Table"
) "_"
where ("_"."DateCreated" <  '2025-01-05 00:00:00' and "_"."DateCreated" >=  '2024-07-05 00:00:00') and "_"."PolicyStatus" = 'Downlines_Processed'
) "rows"
group by "AgentFullName",
    "CarrierName",
    "DateCreated",
    "HolderName",
    "o0"
) "_"
) "_"
) "_"
) "_"
order by "_"."t0_0" desc,
    "_"."t1_0" desc,
    "_"."AgentFullName",
    "_"."HolderName",
    "_"."CarrierName"
limit 501`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            AgentFullName: '$AgentFullName',
                            CarrierName: '$CarrierName',
                            DateCreated: '$DateCreated',
                            HolderName: '$HolderName',
                            PolicyStatus: '$PolicyStatus',
                        },
                    },
                    {
                        $match: {
                            $and: [
                                {
                                    $and: [
                                        {
                                            DateCreated: {
                                                $lt: '2025-01-05 00:00:00',
                                            },
                                        },
                                        {
                                            DateCreated: {
                                                $gte: '2024-07-05 00:00:00',
                                            },
                                        },
                                    ],
                                },
                                {
                                    PolicyStatus: {
                                        $eq: 'Downlines_Processed',
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $group: {
                            _id: {
                                AgentFullName: '$AgentFullName',
                                CarrierName: '$CarrierName',
                                DateCreated: '$DateCreated',
                                HolderName: '$HolderName',
                                o0: '$DateCreated',
                            },
                        },
                    },
                    {
                        $project: {
                            AgentFullName: '$_id.AgentFullName',
                            CarrierName: '$_id.CarrierName',
                            DateCreated: '$_id.DateCreated',
                            HolderName: '$_id.HolderName',
                            o0: '$_id.o0',
                            _id: 0,
                        },
                    },
                    {
                        $project: {
                            AgentFullName: '$AgentFullName',
                            CarrierName: '$CarrierName',
                            DateCreated: '$DateCreated',
                            HolderName: '$HolderName',
                            t0_0: {
                                $switch: {
                                    branches: [
                                        {
                                            case: {
                                                $ne: ['$o0', null],
                                            },
                                            then: '$o0',
                                        },
                                    ],
                                    default: {
                                        $literal: '1899-12-28 00:00:00',
                                    },
                                },
                            },
                            t1_0: {
                                $switch: {
                                    branches: [
                                        {
                                            case: {
                                                $eq: ['$o0', null],
                                            },
                                            then: 0,
                                        },
                                    ],
                                    default: {
                                        $literal: 1,
                                    },
                                },
                            },
                        },
                    },
                    {
                        $sort: {
                            t0_0: -1,
                            t1_0: -1,
                            AgentFullName: 1,
                            HolderName: 1,
                            CarrierName: 1,
                        },
                    },
                    {
                        $project: {
                            c122: '$AgentFullName',
                            c134: '$CarrierName',
                            c153: '$DateCreated',
                            c179: '$HolderName',
                        },
                    },

                    {
                        $limit: 501,
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi  query 2', function () {
            const sql = `select "$Ordered"."_id",
                                "$Ordered"."CreatedDate",
                                "$Ordered"."Field",
                                "$Ordered"."NewValue",
                                "$Ordered"."OldValue",
                                "$Ordered"."ParentId"
                         from
                             (
                                 select "_"."_id",
                                        "_"."CreatedDate",
                                        "_"."Field",
                                        "_"."NewValue",
                                        "_"."OldValue",
                                        "_"."ParentId"
                                 from
                                     (
                                         select "_id",
                                                "CreatedDate",
                                                "Field",
                                                "NewValue",
                                                "OldValue",
                                                "ParentId"
                                         from "public"."global-flat--offer-history" "$Table"
                                     ) "_"
                                 where "_"."Field" = 'Credit_Status__c' and "_"."Field" is not null
                             ) "$Ordered"
                         order by "$Ordered"."_id"`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            _id: '$_id',
                            CreatedDate: '$CreatedDate',
                            Field: '$Field',
                            NewValue: '$NewValue',
                            OldValue: '$OldValue',
                            ParentId: '$ParentId',
                        },
                    },
                    {
                        $match: {
                            $and: [
                                {
                                    Field: {
                                        $eq: 'Credit_Status__c',
                                    },
                                },
                                {
                                    Field: {
                                        $ne: null,
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $sort: {
                            _id: 1,
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi  query 2', function () {
            const sql = `select "$Ordered"."_id",
                                "$Ordered"."CreatedDate",
                                "$Ordered"."Field",
                                "$Ordered"."NewValue",
                                "$Ordered"."OldValue",
                                "$Ordered"."ParentId"
                         from
                             (
                                 select "_"."_id",
                                        "_"."CreatedDate",
                                        "_"."Field",
                                        "_"."NewValue",
                                        "_"."OldValue",
                                        "_"."ParentId"
                                 from
                                     (
                                         select "_id",
                                                "CreatedDate",
                                                "Field",
                                                "NewValue",
                                                "OldValue",
                                                "ParentId"
                                         from "public"."global-flat--offer-history" "$Table"
                                     ) "_"
                                 where "_"."Field" = 'Credit_Status__c' and "_"."Field" is not null
                             ) "$Ordered"
                         order by "$Ordered"."_id"`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            _id: '$_id',
                            CreatedDate: '$CreatedDate',
                            Field: '$Field',
                            NewValue: '$NewValue',
                            OldValue: '$OldValue',
                            ParentId: '$ParentId',
                        },
                    },
                    {
                        $match: {
                            $and: [
                                {
                                    Field: {
                                        $eq: 'Credit_Status__c',
                                    },
                                },
                                {
                                    Field: {
                                        $ne: null,
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $sort: {
                            _id: 1,
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a powerbi  query 3', function () {
            const sql = `
                select "_"."_id" as "_id",
                       "_"."Id" as "Id",
                       "_"."BudgetDealType" as "BudgetDealType",
                       "_"."BudgetLevelValueYear" as "BudgetLevelValueYear",
                       "_"."BudgetMonth" as "BudgetMonth",
                       "_"."BudgetName" as "BudgetName",
                       "_"."BudgetYear" as "BudgetYear",
                       "_"."Channel" as "Channel",
                       "_"."ChannelLevelValue" as "ChannelLevelValue",
                       "_"."GoToMarket" as "GoToMarket",
                       "_"."GoToMarketLevelValue" as "GoToMarketLevelValue",
                       "_"."Industry" as "Industry",
                       "_"."IndustryLevelValue" as "IndustryLevelValue",
                       "_"."SubChannel" as "SubChannel",
                       "_"."_dateUpdated" as "_dateUpdated",
                       'Budget' as "Split",
                       cast("_"."BudgetMonth" as varchar) as "t0_0",
                       cast("_"."BudgetYear" as varchar) as "t1_0",
                       '' as "Account",
                       '' as "AccountName",
                       '' as "AccountFundingStatus",
                       '' as "AccountLegalName",
                       '' as "AccountContractPrinciple",
                       '' as "ApplicationStatus",
                       '' as "ClosedBy",
                       '' as "CreditEngineMaxLoanAmount",
                       '' as "ClosedReasonNew",
                       '' as "CreatedDate",
                       '' as "CreditStatus",
                       '' as "DisbursedAmount",
                       '' as "Name",
                       '' as "Product",
                       '' as "SalesExecutiveUser",
                       '' as "Status",
                       '' as "SourceInitiative",
                       '' as "SalesJourney",
                       '' as "Underwriter",
                       case
                           when "_"."Channel" = 'Partnership - Medical' and "_"."Channel" is not null
                               then '1,248'
                           when "_"."BudgetDealType" = 'New' and "_"."BudgetDealType" is not null
                               then '1,24'
                           when "_"."BudgetDealType" = 'Readvance' and "_"."BudgetDealType" is not null
                               then '1,23'
                           else '0'
                           end as "FactorRate",
                       case
                           when "_"."Channel" = 'Partnerships - Medical' and "_"."Channel" is not null
                               then '13,8'
                           when "_"."BudgetDealType" = 'New' and "_"."BudgetDealType" is not null
                               then '9'
                           when "_"."BudgetDealType" = 'Readvance' and "_"."BudgetDealType" is not null
                               then '8'
                           else '0'
                           end as "LatestQuoteTerm"
                from "public"."global-flat--budget-full-set" "_"`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            _id: '$_id',
                            Id: '$Id',
                            BudgetDealType: '$BudgetDealType',
                            BudgetLevelValueYear: '$BudgetLevelValueYear',
                            BudgetMonth: '$BudgetMonth',
                            BudgetName: '$BudgetName',
                            BudgetYear: '$BudgetYear',
                            Channel: '$Channel',
                            ChannelLevelValue: '$ChannelLevelValue',
                            GoToMarket: '$GoToMarket',
                            GoToMarketLevelValue: '$GoToMarketLevelValue',
                            Industry: '$Industry',
                            IndustryLevelValue: '$IndustryLevelValue',
                            SubChannel: '$SubChannel',
                            _dateUpdated: '$_dateUpdated',
                            Split: {
                                $literal: 'Budget',
                            },
                            t0_0: {
                                $convert: {
                                    input: '$BudgetMonth',
                                    to: 'string',
                                },
                            },
                            t1_0: {
                                $convert: {
                                    input: '$BudgetYear',
                                    to: 'string',
                                },
                            },
                            Account: {
                                $literal: '',
                            },
                            AccountName: {
                                $literal: '',
                            },
                            AccountFundingStatus: {
                                $literal: '',
                            },
                            AccountLegalName: {
                                $literal: '',
                            },
                            AccountContractPrinciple: {
                                $literal: '',
                            },
                            ApplicationStatus: {
                                $literal: '',
                            },
                            ClosedBy: {
                                $literal: '',
                            },
                            CreditEngineMaxLoanAmount: {
                                $literal: '',
                            },
                            ClosedReasonNew: {
                                $literal: '',
                            },
                            CreatedDate: {
                                $literal: '',
                            },
                            CreditStatus: {
                                $literal: '',
                            },
                            DisbursedAmount: {
                                $literal: '',
                            },
                            Name: {
                                $literal: '',
                            },
                            Product: {
                                $literal: '',
                            },
                            SalesExecutiveUser: {
                                $literal: '',
                            },
                            Status: {
                                $literal: '',
                            },
                            SourceInitiative: {
                                $literal: '',
                            },
                            SalesJourney: {
                                $literal: '',
                            },
                            Underwriter: {
                                $literal: '',
                            },
                            FactorRate: {
                                $switch: {
                                    branches: [
                                        {
                                            case: {
                                                $and: [
                                                    {
                                                        $eq: [
                                                            '$Channel',
                                                            'Partnership - Medical',
                                                        ],
                                                    },
                                                    {
                                                        $ne: ['$Channel', null],
                                                    },
                                                ],
                                            },
                                            then: '1,248',
                                        },
                                        {
                                            case: {
                                                $and: [
                                                    {
                                                        $eq: [
                                                            '$BudgetDealType',
                                                            'New',
                                                        ],
                                                    },
                                                    {
                                                        $ne: [
                                                            '$BudgetDealType',
                                                            null,
                                                        ],
                                                    },
                                                ],
                                            },
                                            then: '1,24',
                                        },
                                        {
                                            case: {
                                                $and: [
                                                    {
                                                        $eq: [
                                                            '$BudgetDealType',
                                                            'Readvance',
                                                        ],
                                                    },
                                                    {
                                                        $ne: [
                                                            '$BudgetDealType',
                                                            null,
                                                        ],
                                                    },
                                                ],
                                            },
                                            then: '1,23',
                                        },
                                    ],
                                    default: {
                                        $literal: '0',
                                    },
                                },
                            },
                            LatestQuoteTerm: {
                                $switch: {
                                    branches: [
                                        {
                                            case: {
                                                $and: [
                                                    {
                                                        $eq: [
                                                            '$Channel',
                                                            'Partnerships - Medical',
                                                        ],
                                                    },
                                                    {
                                                        $ne: ['$Channel', null],
                                                    },
                                                ],
                                            },
                                            then: '13,8',
                                        },
                                        {
                                            case: {
                                                $and: [
                                                    {
                                                        $eq: [
                                                            '$BudgetDealType',
                                                            'New',
                                                        ],
                                                    },
                                                    {
                                                        $ne: [
                                                            '$BudgetDealType',
                                                            null,
                                                        ],
                                                    },
                                                ],
                                            },
                                            then: '9',
                                        },
                                        {
                                            case: {
                                                $and: [
                                                    {
                                                        $eq: [
                                                            '$BudgetDealType',
                                                            'Readvance',
                                                        ],
                                                    },
                                                    {
                                                        $ne: [
                                                            '$BudgetDealType',
                                                            null,
                                                        ],
                                                    },
                                                ],
                                            },
                                            then: '8',
                                        },
                                    ],
                                    default: {
                                        $literal: '0',
                                    },
                                },
                            },
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should not optimize a nested query with no fields', function () {
            const sql = `
                select * from (select * from (select * from (select * from (select * from "dwh-data-views-users") _) _) _) _`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(optimized, aggr.pipeline, 'did optimize');
        });

        it('should optimize a nested query fields', function () {
            const sql = `
                select _.UserID from (select _.UserID from (select _.UserID from (select _.UserID from (select UserID from "dwh-data-views-users") _) _) _) _`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            UserID: '$UserID',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize a nested query fields 2', function () {
            const sql = `
                select _.UserT as X from (select _.UserT from (select _.UserID as UserT from (select _.UserID from (select UserID from "dwh-data-views-users") _) _) _) _`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            X: '$UserID',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize with unneeded alias', function () {
            const sql = `
                select UserID from "dwh-data-views-users" users`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            UserID: '$UserID',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize with unneeded alias 2', function () {
            const sql = `
                select UserID as X from "dwh-data-views-users" users`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            X: '$UserID',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize with where function complex', function () {
            const sql = `
                select sum(cast("rows"."ValueUSD" as decimal)) as "a0"
                from
                    (
                        select "_"."ValueUSD"
                        from "public"."PLDetails2" "_"
                        where (((("_"."AccountType" = 'EXPENSE' and cast("_"."Year" as decimal) = cast(2024 as decimal)) and not strpos((case
                                                                                                                                             when "_"."AccountName" is not null
                                                                                                                                                 then "_"."AccountName"
                                                                                                                                             else ''
                            end), 'Unrealized Currency') = 1) and not strpos((case
                                                                                  when "_"."AccountName" is not null
                                                                                      then "_"."AccountName"
                                                                                  else ''
                            end), 'EE: Leave Pay') = 1) and not strpos((case
                                                                            when "_"."AccountName" is not null
                                                                                then "_"."AccountName"
                                                                            else ''
                            end), 'Inter-company') = 1) and "_"."Year" >= 2022
                    ) "rows"`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            $and: [
                                {
                                    $and: [
                                        {
                                            $and: [
                                                {
                                                    $and: [
                                                        {
                                                            $and: [
                                                                {
                                                                    AccountType:
                                                                        {
                                                                            $eq: 'EXPENSE',
                                                                        },
                                                                },
                                                                {
                                                                    $expr: {
                                                                        $eq: [
                                                                            {
                                                                                $convert:
                                                                                    {
                                                                                        input: '$Year',
                                                                                        to: 'decimal',
                                                                                    },
                                                                            },
                                                                            {
                                                                                $convert:
                                                                                    {
                                                                                        input: 2024,
                                                                                        to: 'decimal',
                                                                                    },
                                                                            },
                                                                        ],
                                                                    },
                                                                },
                                                            ],
                                                        },
                                                        {
                                                            $nor: [
                                                                {
                                                                    $expr: {
                                                                        $eq: [
                                                                            {
                                                                                $add: [
                                                                                    {
                                                                                        $indexOfCP:
                                                                                            [
                                                                                                {
                                                                                                    $switch:
                                                                                                        {
                                                                                                            branches:
                                                                                                                [
                                                                                                                    {
                                                                                                                        case: {
                                                                                                                            $ne: [
                                                                                                                                '$AccountName',
                                                                                                                                null,
                                                                                                                            ],
                                                                                                                        },
                                                                                                                        then: '$AccountName',
                                                                                                                    },
                                                                                                                ],
                                                                                                            default:
                                                                                                                {
                                                                                                                    $literal:
                                                                                                                        '',
                                                                                                                },
                                                                                                        },
                                                                                                },
                                                                                                {
                                                                                                    $literal:
                                                                                                        'Unrealized Currency',
                                                                                                },
                                                                                            ],
                                                                                    },
                                                                                    1,
                                                                                ],
                                                                            },
                                                                            1,
                                                                        ],
                                                                    },
                                                                },
                                                            ],
                                                        },
                                                    ],
                                                },
                                                {
                                                    $nor: [
                                                        {
                                                            $expr: {
                                                                $eq: [
                                                                    {
                                                                        $add: [
                                                                            {
                                                                                $indexOfCP:
                                                                                    [
                                                                                        {
                                                                                            $switch:
                                                                                                {
                                                                                                    branches:
                                                                                                        [
                                                                                                            {
                                                                                                                case: {
                                                                                                                    $ne: [
                                                                                                                        '$AccountName',
                                                                                                                        null,
                                                                                                                    ],
                                                                                                                },
                                                                                                                then: '$AccountName',
                                                                                                            },
                                                                                                        ],
                                                                                                    default:
                                                                                                        {
                                                                                                            $literal:
                                                                                                                '',
                                                                                                        },
                                                                                                },
                                                                                        },
                                                                                        {
                                                                                            $literal:
                                                                                                'EE: Leave Pay',
                                                                                        },
                                                                                    ],
                                                                            },
                                                                            1,
                                                                        ],
                                                                    },
                                                                    1,
                                                                ],
                                                            },
                                                        },
                                                    ],
                                                },
                                            ],
                                        },
                                        {
                                            $nor: [
                                                {
                                                    $expr: {
                                                        $eq: [
                                                            {
                                                                $add: [
                                                                    {
                                                                        $indexOfCP:
                                                                            [
                                                                                {
                                                                                    $switch:
                                                                                        {
                                                                                            branches:
                                                                                                [
                                                                                                    {
                                                                                                        case: {
                                                                                                            $ne: [
                                                                                                                '$AccountName',
                                                                                                                null,
                                                                                                            ],
                                                                                                        },
                                                                                                        then: '$AccountName',
                                                                                                    },
                                                                                                ],
                                                                                            default:
                                                                                                {
                                                                                                    $literal:
                                                                                                        '',
                                                                                                },
                                                                                        },
                                                                                },
                                                                                {
                                                                                    $literal:
                                                                                        'Inter-company',
                                                                                },
                                                                            ],
                                                                    },
                                                                    1,
                                                                ],
                                                            },
                                                            1,
                                                        ],
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                                {
                                    Year: {
                                        $gte: 2022,
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $group: {
                            _id: {},
                            a0: {
                                $sum: {
                                    $convert: {
                                        input: '$ValueUSD',
                                        to: 'decimal',
                                    },
                                },
                            },
                        },
                    },
                    {
                        $project: {
                            _id: 0,
                            a0: '$a0',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize aggregates recursively', function () {
            const sql = `
                select t2.name from table1 t1 inner join (select _table2.id2 from (
                                                                                      select "id2","val"
                                                                                      from "Table2" "_Table"
                                                                                  ) _table2 where _table2.val>3) t2 on t1.id1=t2.id2`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            t1: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'Table2',
                            as: 't2',
                            let: {
                                t1_id1: '$t1.id1',
                            },
                            pipeline: [
                                {
                                    $project: {
                                        id2: '$id2',
                                        val: '$val',
                                    },
                                },
                                {
                                    $match: {
                                        val: {
                                            $gt: 3,
                                        },
                                    },
                                },
                                {
                                    $project: {
                                        id2: '$id2',
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$$t1_id1', '$id2'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$t2',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $project: {
                            name: '$t2.name',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize with simple join1', function () {
            const sql = `
                select t2.name from (select * from table1 where x=1) t1 inner join (select _table2.id2 from (
                                                                                      select "id2","val"
                                                                                      from "Table2" "_Table"
                                                                                  ) _table2 where _table2.val>3) t2 on t1.id1=t2.id2`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            x: {
                                $eq: 1,
                            },
                        },
                    },
                    {
                        $project: {
                            t1: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'Table2',
                            as: 't2',
                            let: {
                                t1_id1: '$t1.id1',
                            },
                            pipeline: [
                                {
                                    $project: {
                                        id2: '$id2',
                                        val: '$val',
                                    },
                                },
                                {
                                    $match: {
                                        val: {
                                            $gt: 3,
                                        },
                                    },
                                },
                                {
                                    $project: {
                                        id2: '$id2',
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$$t1_id1', '$id2'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$t2',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $project: {
                            name: '$t2.name',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize where at end', function () {
            const sql = `
                select t2.name from table1 t1 inner join (select _table2.id2 from (
        select "id2","val"
    from "Table2" "_Table"
) _table2 where _table2.val>3) t2 on t1.id1=t2.id2 where t1.val2>100 and to_int(t1.val2)<200`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            $and: [
                                {
                                    val2: {
                                        $gt: 100,
                                    },
                                },
                                {
                                    $expr: {
                                        $lt: [
                                            {
                                                $toInt: '$val2',
                                            },
                                            200,
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $project: {
                            t1: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'Table2',
                            as: 't2',
                            let: {
                                t1_id1: '$t1.id1',
                            },
                            pipeline: [
                                {
                                    $project: {
                                        id2: '$id2',
                                        val: '$val',
                                    },
                                },
                                {
                                    $match: {
                                        val: {
                                            $gt: 3,
                                        },
                                    },
                                },
                                {
                                    $project: {
                                        id2: '$id2',
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$$t1_id1', '$id2'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$t2',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $project: {
                            name: '$t2.name',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize where at end 2', function () {
            const sql = `
                select t2.name from table1 t1 inner join "Table2" t2 on t1.id1=t2.id2 where t1.val2>100 and to_int(t1.val2)<200`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            $and: [
                                {
                                    val2: {
                                        $gt: 100,
                                    },
                                },
                                {
                                    $expr: {
                                        $lt: [
                                            {
                                                $toInt: '$val2',
                                            },
                                            200,
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $project: {
                            t1: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'Table2',
                            as: 't2',
                            localField: 't1.id1',
                            foreignField: 'id2',
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$t2',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $project: {
                            name: '$t2.name',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should not optimize when cross table', function () {
            const sql = `
                select t2.name from table1 t1 inner join "Table2" t2 on t1.id1=t2.id2 where t1.val2>100 and to_int(t2.val2)<200`;
            const aggr = SQLParser.parseSQL(sql);
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(
                aggr.pipeline,
                {}
            );
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $project: {
                            t1: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'Table2',
                            as: 't2',
                            localField: 't1.id1',
                            foreignField: 'id2',
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$t2',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $match: {
                            $and: [
                                {
                                    't1.val2': {
                                        $gt: 100,
                                    },
                                },
                                {
                                    $expr: {
                                        $lt: [
                                            {
                                                $toInt: '$t2.val2',
                                            },
                                            200,
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $project: {
                            name: '$t2.name',
                        },
                    },
                ],
                'did not optimize'
            );
        });

        it('should optimize when match isnt at end in lookup', function () {
            const pipeline = [
                {
                    $project: {
                        cust: '$$ROOT',
                    },
                },
                {
                    $lookup: {
                        from: 'buffer664390eeefc55bc633d3189b',
                        as: 'empsCsr',
                        let: {},
                        pipeline: [
                            {
                                $project: {
                                    customer: '$$ROOT',
                                },
                            },
                            {
                                $lookup: {
                                    from: 'buffer664b503510da713033caa13e',
                                    as: 'empCsr',
                                    let: {},
                                    pipeline: [
                                        {
                                            $match: {
                                                $expr: {
                                                    $eq: [
                                                        '$Id',
                                                        '0056g000003H71UAAS',
                                                    ],
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                            {
                                $set: {
                                    empCsr: {
                                        $first: '$empCsr',
                                    },
                                },
                            },
                            {
                                $match: {
                                    empCsr: {
                                        $ne: null,
                                    },
                                },
                            },
                            {
                                $lookup: {
                                    from: 'buffer66420ac72c5748d51acd2808',
                                    as: 'certiAgents',
                                    localField: 'empCsr.Id',
                                    foreignField: 'Id',
                                },
                            },
                            {
                                $set: {
                                    certiAgents: {
                                        $first: '$certiAgents',
                                    },
                                },
                            },
                            {
                                $match: {
                                    certiAgents: {
                                        $ne: null,
                                    },
                                },
                            },
                            {
                                $match: {
                                    'customer.Id': {
                                        $eq: '0016g00000RwgtpAAB',
                                    },
                                },
                            },
                            {
                                $group: {
                                    _id: {
                                        Id: '$empCsr.Id',
                                        CUST: '$cust.Id',
                                        userId: '$certiAgents.userId',
                                    },
                                },
                            },
                            {
                                $project: {
                                    Id: '$_id.Id',
                                    CUST: '$_id.CUST',
                                    userId: '$_id.userId',
                                    _id: 0,
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $eq: ['$Id', '0056g000003H71UAAS'],
                                    },
                                },
                            },
                        ],
                    },
                },
                {
                    $lookup: {
                        from: 'buffer664390eeefc55bc633d3189b',
                        as: 'empsProd',
                        let: {},
                        pipeline: [
                            {
                                $project: {
                                    customer: '$$ROOT',
                                },
                            },
                            {
                                $lookup: {
                                    from: 'buffer664b503510da713033caa13e',
                                    as: 'empProd',
                                    let: {},
                                    pipeline: [
                                        {
                                            $match: {
                                                $expr: {
                                                    $eq: [
                                                        '$Id',
                                                        '0056g000005UjGgAAK',
                                                    ],
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                            {
                                $set: {
                                    empProd: {
                                        $first: '$empProd',
                                    },
                                },
                            },
                            {
                                $match: {
                                    empProd: {
                                        $ne: null,
                                    },
                                },
                            },
                            {
                                $lookup: {
                                    from: 'buffer66420ac72c5748d51acd2808',
                                    as: 'certiAgents',
                                    localField: 'empProd.Id',
                                    foreignField: 'Id',
                                },
                            },
                            {
                                $set: {
                                    certiAgents: {
                                        $first: '$certiAgents',
                                    },
                                },
                            },
                            {
                                $match: {
                                    certiAgents: {
                                        $ne: null,
                                    },
                                },
                            },
                            {
                                $match: {
                                    'customer.Id': {
                                        $eq: '0016g00000RwgtpAAB',
                                    },
                                },
                            },
                            {
                                $group: {
                                    _id: {
                                        Id: '$empProd.Id',
                                        CUST: '$cust.Id',
                                        userId: '$certiAgents.userId',
                                    },
                                },
                            },
                            {
                                $project: {
                                    Id: '$_id.Id',
                                    CUST: '$_id.CUST',
                                    userId: '$_id.userId',
                                    _id: 0,
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $eq: ['$Id', '0056g000005UjGgAAK'],
                                    },
                                },
                            },
                        ],
                    },
                },
                {
                    $match: {
                        'cust.Id': {
                            $eq: '0016g00000RwgtpAAB',
                        },
                    },
                },
                {
                    $project: {
                        agentIds: {
                            $concatArrays: [
                                '$empsCsr.userId',
                                '$empsProd.userId',
                            ],
                        },
                    },
                },
                {
                    $limit: 1,
                },
            ];
            // console.log(JSON.stringify(aggr.pipeline, null, 4));
            const optimized = optimizer.optimizeMongoAggregate(pipeline, {});
            assert.deepStrictEqual(
                optimized,
                [
                    {
                        $match: {
                            Id: {
                                $eq: '0016g00000RwgtpAAB',
                            },
                        },
                    },
                    {
                        $project: {
                            cust: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'buffer664390eeefc55bc633d3189b',
                            as: 'empsCsr',
                            let: {},
                            pipeline: [
                                {
                                    $match: {
                                        Id: {
                                            $eq: '0016g00000RwgtpAAB',
                                        },
                                    },
                                },
                                {
                                    $project: {
                                        customer: '$$ROOT',
                                    },
                                },
                                {
                                    $lookup: {
                                        from: 'buffer664b503510da713033caa13e',
                                        as: 'empCsr',
                                        let: {},
                                        pipeline: [
                                            {
                                                $match: {
                                                    $expr: {
                                                        $eq: [
                                                            '$Id',
                                                            '0056g000003H71UAAS',
                                                        ],
                                                    },
                                                },
                                            },
                                        ],
                                    },
                                },
                                {
                                    $set: {
                                        empCsr: {
                                            $first: '$empCsr',
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        empCsr: {
                                            $ne: null,
                                        },
                                    },
                                },
                                {
                                    $lookup: {
                                        from: 'buffer66420ac72c5748d51acd2808',
                                        as: 'certiAgents',
                                        localField: 'empCsr.Id',
                                        foreignField: 'Id',
                                    },
                                },
                                {
                                    $set: {
                                        certiAgents: {
                                            $first: '$certiAgents',
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        certiAgents: {
                                            $ne: null,
                                        },
                                    },
                                },
                                {
                                    $group: {
                                        _id: {
                                            Id: '$empCsr.Id',
                                            CUST: '$cust.Id',
                                            userId: '$certiAgents.userId',
                                        },
                                    },
                                },
                                {
                                    $project: {
                                        Id: '$_id.Id',
                                        CUST: '$_id.CUST',
                                        userId: '$_id.userId',
                                        _id: 0,
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$Id', '0056g000003H71UAAS'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $lookup: {
                            from: 'buffer664390eeefc55bc633d3189b',
                            as: 'empsProd',
                            let: {},
                            pipeline: [
                                {
                                    $match: {
                                        Id: {
                                            $eq: '0016g00000RwgtpAAB',
                                        },
                                    },
                                },
                                {
                                    $project: {
                                        customer: '$$ROOT',
                                    },
                                },
                                {
                                    $lookup: {
                                        from: 'buffer664b503510da713033caa13e',
                                        as: 'empProd',
                                        let: {},
                                        pipeline: [
                                            {
                                                $match: {
                                                    $expr: {
                                                        $eq: [
                                                            '$Id',
                                                            '0056g000005UjGgAAK',
                                                        ],
                                                    },
                                                },
                                            },
                                        ],
                                    },
                                },
                                {
                                    $set: {
                                        empProd: {
                                            $first: '$empProd',
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        empProd: {
                                            $ne: null,
                                        },
                                    },
                                },
                                {
                                    $lookup: {
                                        from: 'buffer66420ac72c5748d51acd2808',
                                        as: 'certiAgents',
                                        localField: 'empProd.Id',
                                        foreignField: 'Id',
                                    },
                                },
                                {
                                    $set: {
                                        certiAgents: {
                                            $first: '$certiAgents',
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        certiAgents: {
                                            $ne: null,
                                        },
                                    },
                                },
                                {
                                    $group: {
                                        _id: {
                                            Id: '$empProd.Id',
                                            CUST: '$cust.Id',
                                            userId: '$certiAgents.userId',
                                        },
                                    },
                                },
                                {
                                    $project: {
                                        Id: '$_id.Id',
                                        CUST: '$_id.CUST',
                                        userId: '$_id.userId',
                                        _id: 0,
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$Id', '0056g000005UjGgAAK'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $project: {
                            agentIds: {
                                $concatArrays: [
                                    '$empsCsr.userId',
                                    '$empsProd.userId',
                                ],
                            },
                        },
                    },
                    {
                        $limit: 1,
                    },
                ],
                'did not optimize'
            );
        });
    });
});

//
