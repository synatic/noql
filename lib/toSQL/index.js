const {pipelineToAST} = require('./pipelineToAST');
const {astToSQL} = require('./astToSQL');
const {normalizePipelineInput} = require('./normalizePipelineInput');

/**
 * Converts a MongoDB aggregation pipeline (as produced by
 * SQLParser.makeMongoAggregate) back into an equivalent noql SQL statement.
 *
 * Accepts either a bare pipeline array, or a full aggregate command document
 * (`{aggregate, pipeline, ...}`). When a command document is passed and
 * `collections` is omitted/empty, the command's `aggregate` field is used as
 * the base collection.
 * @param {object[]|object} pipeline - the aggregation pipeline stages, or an
 *   aggregate command document containing a `pipeline` array
 * @param {string[]} [collections] - the base collection name(s) the pipeline
 *   runs against (optional when `pipeline` is a command document with
 *   `aggregate`)
 * @param {{pretty?: boolean}} [options] - `pretty` (default `true`) formats
 *   the SQL across multiple indented lines; set to `false` for a single line
 * @returns {string} the reconstructed SQL statement
 * @throws if the pipeline uses a construct reverse conversion doesn't support yet
 */
function pipelineToSQL(pipeline, collections, options = {}) {
    const normalized = normalizePipelineInput(pipeline, collections);
    const ast = pipelineToAST(normalized.pipeline, normalized.collections);
    return astToSQL(ast, {pretty: options.pretty !== false});
}

module.exports = {pipelineToSQL, pipelineToAST, astToSQL, normalizePipelineInput};
