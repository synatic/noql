import type {Document, Sort} from 'mongodb';
import type {} from 'node-sql-parser';
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
    | 'unary_expr';
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
    args?: Expression[];
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
    $group?: {_id: any};
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
}

export type ParserInput = TableColumnAst | string;
export type ParserOptions = {
    isArray?: boolean;
    /** automatically unwind joins, by default is set to false and unwind should be done by using unwind in the select */
    unwindJoins?: boolean;
    database?: string;
    type?: string;
};

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
            [key: string]: string | {$literal: string};
        };
    };
    exprToMerge: (string | {[key: string]: string | {$literal: string}})[];
    count: {$count: string}[];
    unset: string[];
}

export interface MongoQueryFunction {
    /** The name of the function as it will be used in sql, case insensitive, e.g. abs */
    name: string;
    /** A description of what the function does */
    description?: string;
    /** Allow the function to be used in mongo query/find and not just aggregate pipelines, default: false */
    allowQuery?: boolean;
    /** Specifies if it is an aggregate only function or a general function (for queries maybe?) */
    type?: 'function' | 'aggr_func';
    /** Doesn't seem to be used */
    parsedName?: string;
    /** function that takes in the parameters from the queries and returns the pipeline operation */
    parse: (...parameters: any) => {
        [key: string]: any;
    };
    /** specifies if this function requires an as when it's in a query, default: true */
    requiresAs?: boolean;
}
