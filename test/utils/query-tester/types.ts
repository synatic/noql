import {MongoClient, Document} from 'mongodb';

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
export interface QueryResultOptions {
    /** The query string to run against the db */
    queryString: string;
    /** The JSON path in the target file at which to store the results */
    casePath: string;
    /** Specifies if it should be run in write to file mode (when making changes) or test mode where the results are checked, defaults to test */
    mode?: 'write' | 'test';
}

export type AllQueryResultOptions = BuildQueryResultOptions &
    QueryResultOptions;

export type QueryResultTester = (
    innerOptions: QueryResultOptions
) => Promise<Document[]>;
