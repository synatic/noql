import {MongoClient, Document} from 'mongodb';
import {PipelineFn, ParserOptions} from '../../../lib/types';

/** Options to use when running the function to test/generate test outputs */
export interface BuildQueryResultOptions {
    /** The mongo client to use for the results */
    mongoClient: MongoClient;
    /** The path where the actual test runs from */
    dirName: string;
    /** Specifies the destination file */
    fileName: string;
    /** Specifies if it should be run in write to file mode (when making changes) or test mode where the results are checked, defaults to test */
    mode?: 'write' | 'test';
}
/** Options to use when running the function to test/generate test outputs */
export interface QueryResultOptions extends ParserOptions {
    /** The query string to run against the db */
    queryString: string;
    /** The JSON path in the target file at which to store the results */
    casePath: string;
    /** Specifies if it should be run in write to file mode (when making changes) or test mode where the results are checked, defaults to test */
    mode?: 'write' | 'test';
    /** Defaults to false, set to true if you expect no results to be returned */
    expectZeroResults?: boolean;
    /** when comparing results, will ignore the values of dates, useful when using Current_Date etc. Defaults to false */
    ignoreDateValues?: boolean;
    /** Specifies if the pipeline should be written to the file, useful for debugging */
    outputPipeline?: boolean;
    /** If true the tester won't query the db, just generate the pipeline */
    skipDbQuery?: boolean;
}

export type AllQueryResultOptions = BuildQueryResultOptions &
    QueryResultOptions;

export type QueryResultTester = (
    innerOptions: QueryResultOptions
) => Promise<QueryTesterResult>;

export type QueryTesterResult = {
    results: Document[];
    pipeline: PipelineFn[];
    collections: string[];
};
