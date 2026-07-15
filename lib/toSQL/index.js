const {pipelineToAST} = require('./pipelineToAST');
const {astToSQL} = require('./astToSQL');

/**
 * Converts a MongoDB aggregation pipeline (as produced by
 * SQLParser.makeMongoAggregate) back into an equivalent noql SQL statement.
 * @param {object[]} pipeline - the aggregation pipeline stages
 * @param {string[]} collections - the base collection name(s) the pipeline runs against
 * @returns {string} the reconstructed SQL statement
 * @throws if the pipeline uses a construct reverse conversion doesn't support yet
 */
function pipelineToSQL(pipeline, collections) {
    const ast = pipelineToAST(pipeline, collections);
    return astToSQL(ast);
}

module.exports = {pipelineToSQL, pipelineToAST, astToSQL};
