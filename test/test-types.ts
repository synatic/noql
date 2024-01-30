import {JSONSchema7} from 'json-schema';

export interface SchemaDoc {
    collectionName: string;
    schema: JSONSchema7;
    flattenedSchema: Record<string, unknown>;
}
