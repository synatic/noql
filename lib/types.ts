import type {Document, Sort} from 'mongodb';
import {JSONSchema6TypeName, JSONSchema6} from 'json-schema';

export {JSONSchema6};
export interface TableColumnAst {
    tableList?: string[];
    columnList?: string[];
    ast: AST;
}

export type ExpressionTypes =
    | 'column_ref'
    | 'aggr_func'
    | 'function'
    | 'binary_expr'
    | 'case'
    | 'select'
    | 'cast'
    | 'expr_list'
    | 'else'
    | 'when'
    | 'unary_expr'
    | 'window_func';

export interface AST extends Expression {
    _next?: AST;
    db?: string;
    with?: {
        name: string;
        stmt: any[];
        columns?: any[];
    };
    options?: any[];
    distinct?: 'DISTINCT';
    columns?: Columns;
    where?: Expression;
    groupby?: {
        type: 'column_ref';
        table?: string;
        column: string;
    };
    having?: Expression;
    orderby?: {
        type: 'ASC' | 'DESC';
        expr: Expression;
    }[];
    limit?: {
        seperator: string;
        value: {
            type: string;
            value: number;
        }[];
    };
    values?: {
        type: 'expr_list';
        value: any[];
    }[];
    set: {
        column: string;
        value: any;
        table: string | null;
    }[];
    union?: string;
}
export interface Expression {
    type: ExpressionTypes;
    table?: string;
    column?: string;
    name?: string;
    args?: Expression;
    from?: TableDefinition[];
    value?: any;
    tableList?: string[];
    columnList?: string[];
    ast?: AST;
    parenthesis?: boolean;
    expr?: Expression;
    left?: Expression;
    right?: Expression;
    operator?: string;
    cond?: Expression;
    result?: Expression;
    target?: any;
    over?: {
        as_window_specification: {
            parentheses: boolean;
            window_specification: {
                name: string | null;
                orderby: {
                    expr: Expression;
                    nulls: null;
                    type: 'ASC' | 'DESC';
                }[];
                partitionby:
                    | {
                          expr: Expression;
                          type: string;
                          as: string | null;
                      }[]
                    | null;
                window_frame_clause: any;
            };
        };
        type: 'window';
    };
}

export type Columns = '*' | Column[];
export interface Column {
    expr: Expression;
    as: string;
}

export interface TableDefinition {
    db?: string;
    table?: string;
    as?: string;
    type?: 'dual';
    expr?: Expression;
    on?: Expression;
}

/**------------end testing */

export type ParsedQueryOrAggregate = ParsedMongoQuery | ParsedMongoAggregate;
/** The result of the parser constructing a mongo query from SQL, if the result couldn't be a simple query and needed to be an aggregate function see ParsedMongoAggregate */
export interface ParsedMongoQuery {
    /** The db collection to query */
    collection: string;
    /** The projection to use for the query to get the requested fields */
    projection?: Document;
    /** The number of records to skip */
    skip?: number;
    /** The number of records to limit the result set too */
    limit?: number;
    /** The query to use in the find*/
    query?: Document;
    /** The sort to apply to the query  */
    sort?: Sort;
    /** Tells the calling system if countDocuments should be called instead of find */
    count?: boolean;
    type: 'query';
}

/** The result of the parser constructing a mongo aggregate from SQL, if the result could be a simple query see ParsedMongoQuery */
export interface ParsedMongoAggregate {
    /** The pipeline steps to execute as an aggregate function */
    pipeline: PipelineFn[];
    /** The list of collections involved in the pipeline, the first one is the collection to execute the aggregation against */
    collections: string[];
    type: 'aggregate';
}

export interface PipelineFn {
    $project?: {[key: string]: any};
    $match?: {[key: string]: any};
    $group?: {_id: any; [key: string]: any};
    $replaceRoot?: {[key: string]: any};
    $map?: {[key: string]: any};
    $sort?: {[key: string]: any};
    $limit?: number;
    $skip?: number;
    $unset?: any;
    $unwind?: any;
    $lookup?: any;
    $count?: any;
    $unionWith?: any;
    $set?: any;
    $setWindowFields?: SetWindowFields;
    $addFields?: {[key: string]: any};
}

export interface SetWindowFields {
    partitionBy?: string;
    sortBy: {[key: string]: -1 | 1};
    output: {
        [key: string]: any;
    };
}

export type ParserInput = TableColumnAst | string;
export interface ParserOptions {
    /** Only used in canQuery */
    isArray?: boolean;
    /** automatically unwind joins, by default is set to false and unwind should be done by using unwind in the select or join */
    unwindJoins?: boolean;
    /** Specifies the type that Nodejs SQL Parser will use e.g. 'table', 'column'*/
    type?: string;
    /** force the unset of the _id field if it's not in the select list */
    unsetId?: boolean;
    /** If provided, the library will use the schemas to generate better queries */
    schemas?: Schemas;
    /** If true, will optimize the join for better performance */
    optimizeJoins?: boolean;
}

export interface NoqlContext extends ParserOptions {
    /** The raw SQL statement before being cleaned up or altered */
    rawStatement?: string;
    /** The cleaned SQL statement */
    cleanedStatement?: string;
    tables: string[];
    fullAst: TableColumnAst;
    joinHints?: string[];
    projectionAlreadyAdded?: boolean;
}

export interface ParseResult {
    parsedAst: TableColumnAst;
    context: NoqlContext;
}

export interface ColumnParseResult {
    replaceRoot?: {
        $replaceRoot: {
            newRoot: string;
        };
    };
    asMapping: {column: string; as: string}[];
    groupBy: {
        $group: {
            _id: {
                [key: string]: string | {$literal: string};
            };
        };
    };
    unwind: {$unset?: string; $unwind?: string}[];
    parsedProject: {
        $project: {
            [key: string]:
                | string
                | {$literal: string}
                | {[key: string]: string};
        };
    };
    exprToMerge: (string | {[key: string]: string | {$literal: string}})[];
    count: {$count: string}[];
    unset: {$unset: string[]};
    countDistinct: string;
    groupByProject?: object;
    windowFields: SetWindowFields[];
    subQueryRootProjections: string[];
}

export interface MongoQueryFunction {
    /** The name of the function as it will be used in sql, case insensitive, e.g. abs */
    name: string;
    /** List of aliases that can also be used to call this function */
    aliases?: string[];
    /** A description of what the function does */
    description?: string;
    /** Allow the function to be used in mongo query/find and not just aggregate pipelines, default: false */
    allowQuery?: boolean;
    /** Specifies if it is an aggregate only function or a general function (for queries maybe?) */
    type?: 'function' | 'aggr_func';
    /** Doesn't seem to be used */
    parsedName?: string;
    /** function that takes in the parameters from the queries and returns the pipeline operation */
    parse: (...parameters: any) =>
        | {
              [key: string]: any;
          }
        | string;
    /** specifies if this function requires an as when it's in a query, default: true */
    requiresAs?: boolean;
    /** Specifies if this query requires a group by */
    forceGroup?: boolean;
    jsonSchemaReturnType: JSONSchemaTypeName | SchemaFn;
    /** Specifies if this function is allowed to run without parentheses. E.g. current_date */
    doesNotNeedArgs?: boolean;
    //TODO Rk, would be good to have a description here and auto generate docs
}

export type JSONSchemaTypeName =
    | 'string'
    | 'number'
    | 'integer'
    | 'boolean'
    | 'object'
    | 'date'
    | 'string[]'
    | 'number[]'
    | 'integer[]'
    | 'boolean[]'
    | 'object[]'
    | 'date[]'
    | 'null';

type SchemaFn = (params: any) => SchemaFnResult | SchemaFnResult[];

export interface SchemaFnResult {
    /** Specifies if there is a json schema type returned or the name of the field that defines the type */
    type: 'fieldName' | 'jsonSchemaValue' | 'unset';
    jsonSchemaValue?: JSONSchemaTypeName;
    fieldName?: 'string';
    /** Specifies if the result will be an array of the field type, should not apply to jsonSchemaValue */
    isArray?: boolean;
}

export type JsonSchemaTypeMap = {
    [key: string]: JSONSchemaTypeName;
};

export interface FlattenedSchemas {
    [collectionName: string]: FlattenedSchema[];
}

export interface Schemas {
    [collectionName: string]: JSONSchema6;
}

export interface FlattenedSchema {
    /** The path to the field within the document/json object */
    path: string;
    /** The JsonSchema type */
    type: JSONSchema6TypeName | JSONSchema6TypeName[];
    /** The JsonSchema format if it's a string */
    format?: string | 'date-time' | 'mongoid';
    /** Specifies if the field is an array or not */
    isArray: boolean;
    /** Specifies if it's a required field or not */
    required: boolean;
}

export interface ResultSchema extends FlattenedSchema {
    /** The order for this result, lowest should come first */
    order: number;
    /** the collection from which this column comes */
    collectionName: string;
    /** If the column has an "as" name, it will be here */
    as?: string;
}

export type GetSchemaFunction = (
    collectionName: string
) => Promise<FlattenedSchema[]>;

export type GroupByColumnParserFn = (
    expr: Expression,
    depth: number,
    aggrName: string
) => void;

export type GetTables = (subAst: AST, context: NoqlContext) => string[];

export interface FindSchemaResult {
    schema: JSONSchema6;
    required: boolean;
}

export interface OptimizationProcessResult {
    wasOptimized: boolean;
    pipelineStagesAdded: PipelineFn[];
    lookupPipelineStagesAdded: PipelineFn[];
    leftOverMatches: Record<string, unknown>[];
}
