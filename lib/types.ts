import {
    TableColumnAst,
    Option,
    From,
    With,
    ColumnRef,
    Dual,
    OrderBy,
    Limit,
    InsertReplaceValue,
    SetList,
    AggrFunc,
    Star,
} from 'node-sql-parser';

export interface MongoQuery {
    collection: string;
    projection?: Projection;
    skip?: number;
    limit?: number;
    query?: object;
    sort?: object;
    count?: boolean;
}

export type ParserInput = TableColumnAst | string;
export type ParserOptions = Option & {
    isArray?: boolean;
    /** automatically unwind joins, by default is set to false and unwind should be done by using unwind in the select */
    unwindJoins?: boolean;
};
export type ParserResult = TableColumnAst;
export type Columns = '*' | Column[];
export interface Column {
    expr: {
        type: 'column_ref' | 'aggr_func' | 'function' | 'binary_expr' | 'case' | 'select' | 'cast';
        table?: string | null;
        column?: string;
        name?: string;
        args?: ColumnRef | AggrFunc | Star | null;
        from?: any;
        value?: any;
    };
    as: string;
}
export interface MongoAggregate {
    pipeline: PipelineFn[];
    collections: any[];
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
}
export interface AstLike {
    type: string;
    db?: string;
    with?: With | null;
    options?: any[] | null;
    distinct?: 'DISTINCT' | null;
    columns?: Columns;
    from?: Array<From | Dual | any> | null;
    where?: any;
    groupby?: ColumnRef[] | null;
    having?: any[] | null;
    orderby?: OrderBy[] | null;
    limit?: Limit | null;
    table?: Array<From | Dual> | null;
    values?: InsertReplaceValue[];
    set: SetList[];
    expr: any;
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
            [key: string]: string | {$literal: string};
        };
    };
    exprToMerge: (string | {[key: string]: string | {$literal: string}})[];
    count: {$count: string}[];
}
export interface Projection {}

export interface MongoQueryFunction {
    /** The name of the function as it will be used in sql, case insensitive, e.g. abs */
    name: string;
    /** Allow the function to be used in mongo query/find and not just aggregate pipelines, default:false */
    allowQuery?: boolean;
    /** Specifies if it is an aggregate only function or a general function (for queries maybe?) */
    type?: 'function' | 'aggr_func';
    /** Doesn't seem to be used */
    parsedName?: string;
    /** function that takes in the parameters from the queries and returns the pipeline operation */
    parse: (parameters: any) => {
        [key: string]: any;
    };
}
