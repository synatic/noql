/**
 *
 * @param {import("mongodb").Db} database
 * @returns {import("../../lib/types").Schemas}
 */
async function getAllSchemas(database) {
    const collections = await database.collections();
    const collectionNames = collections
        .map((c) => c.collectionName)
        .filter((c) => c !== 'schemas');
    /** @type {import("../../lib/types").Schemas} */
    const schemas = {};
    for (const collectionName of collectionNames) {
        const searchResult = await database
            .collection('schemas')
            .findOne({collectionName}, {projection: {_id: 0, schema: 1}});
        schemas[collectionName] = searchResult.schema;
    }

    const schemaKeys = Object.keys(schemas);
    if (collectionNames.length !== schemaKeys.length) {
        const missingSchemas = collectionNames.filter(
            (cn) => schemaKeys.indexOf(cn) >= 0
        );
        throw new Error(
            `Not all schemas could be retrieved from the db, missing: ${missingSchemas}`
        );
    }
    return schemas;
}
module.exports = {getAllSchemas};
