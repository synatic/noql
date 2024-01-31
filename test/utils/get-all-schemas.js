/** @type {import("../../lib/types").Schemas |null} */
let schemas = null;
/**
 *
 * @param {import("mongodb").Db} database
 * @returns {import("../../lib/types").Schemas}
 */
async function getAllSchemas(database) {
    if (schemas) {
        return schemas;
    }

    const collections = await database.collections();
    const collectionNames = collections
        .map((c) => c.collectionName)
        .filter((c) => c !== 'schemas');
    schemas = {};
    for (const collectionName of collectionNames) {
        const searchResult = await database
            .collection('schemas')
            .findOne({collectionName}, {projection: {_id: 0, schema: 1}});
        schemas[collectionName] = searchResult.schema;
    }

    return schemas;
}
module.exports = {getAllSchemas};
