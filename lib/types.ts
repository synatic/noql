import {Column} from 'node-sql-parser';
export interface MongoQuery {
    collection: string;
    projection?: Projection;
    skip?: number;
    limit?: number;
    query?: object;
    sort?: object;
    count?: boolean;
}

export interface Projection {}

export type Columns = Column[] | '*';
