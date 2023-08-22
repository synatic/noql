const {buildConcat} = require('../../lib/make/makeCastPart');
const assert = require('assert');
const $schema = require('@synatic/schema-magic');

/** @typedef {import('../../lib/types').FlattenedSchema} FlattenedSchema */
describe('makeCastPart', () => {
    describe('buildConcat', () => {
        describe('primitives', () => {
            it('should support strings', () => {
                /** @type {FlattenedSchema[]}*/
                const schemas = getSchema({name: 'asd'});
                const result = buildConcat(schemas);
                const joined = joinConcat(result);
                assert('"name":"$$this.name"', joined);
            });
            it('should support nullable strings', () => {
                /** @type {FlattenedSchema[]}*/
                const schemas = getSchema({name: 'asd'}, {name: null});
                const result = buildConcat(schemas);
                const joined = joinConcat(result);
                assert(
                    '"name":"{ $cond: [{$eq: ["$$value", ""],},"$$this.name", "null"] }"',
                    joined
                );
            });
        });
    });
});

/**
 *
 * @param {string[]} parts
 */
function joinConcat(parts) {
    return parts.join('');
}
/**
 *
 * @param {Record<string,unknown>|Record<string,unknown>[]} obj
 * @returns {FlattenedSchema[]}
 */
function getSchema(...obj) {
    if (Array.isArray(obj)) {
        const schema = $schema.mergeSchemas(
            obj
                .slice(0, 100)
                .filter((v) => Boolean)
                .map((v) => $schema.generateSchemaFromJSON(v))
        );
        return $schema.flattenSchema(schema, {
            additionalProperties: ['displayOptions'],
        });
    }
    const schema = $schema.generateSchemaFromJSON(obj);
    return $schema.flattenSchema(schema, {
        additionalProperties: ['displayOptions'],
    });
}
/**
 *
 * @param {string} path The path to the field on the document
 * @returns {FlattenedSchema} The resultant schema
 */
function fsString(path) {
    return {
        path,
        type: 'string',
        format: null,
        isArray: false,
        required: false,
    };
}

/**
 *
 * @param {string} path The path to the field on the document
 * @returns {FlattenedSchema} The resultant schema
 */
function fsMongoId(path) {
    return {
        path,
        type: 'string',
        format: 'mongoid',
        isArray: false,
        required: false,
    };
}
/**
 *
 * @param {string} path The path to the field on the document
 * @returns {FlattenedSchema} The resultant schema
 */
function fsNumber(path) {
    return {
        path,
        type: 'number',
        format: 'mongoid',
        isArray: false,
        required: false,
    };
}
/**
 *
 * @param {string} path The path to the field on the document
 * @returns {FlattenedSchema} The resultant schema
 */
function fsInteger(path) {
    return {
        path,
        type: 'number',
        format: 'mongoid',
        isArray: false,
        required: false,
    };
}
