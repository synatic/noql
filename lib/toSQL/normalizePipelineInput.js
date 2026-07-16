/**
 * Accepts either a bare aggregation pipeline array, or a full MongoDB
 * aggregate command document (`{aggregate, pipeline, ...}` as returned by
 * Compass / the shell / driver logs), and returns a normalized
 * `{pipeline, collections}` pair ready for reverse conversion.
 *
 * When the input is a command document and `collections` is missing/empty,
 * the command's `aggregate` collection name is used.
 * @param {object[]|object} pipelineOrCommand
 * @param {string[]} [collections]
 * @returns {{pipeline: object[], collections: string[]}}
 */
function normalizePipelineInput(pipelineOrCommand, collections) {
    let pipeline = pipelineOrCommand;
    let resolvedCollections = Array.isArray(collections)
        ? collections.filter((c) => typeof c === 'string' && c.trim())
        : [];

    if (
        pipeline &&
        typeof pipeline === 'object' &&
        !Array.isArray(pipeline) &&
        Array.isArray(pipeline.pipeline)
    ) {
        if (
            resolvedCollections.length === 0 &&
            typeof pipeline.aggregate === 'string' &&
            pipeline.aggregate
        ) {
            resolvedCollections = [pipeline.aggregate];
        }
        pipeline = pipeline.pipeline;
    }

    if (!Array.isArray(pipeline)) {
        throw new Error(
            'pipelineToSQL expects a Mongo aggregation pipeline array, or an aggregate command document with a "pipeline" array (e.g. {"aggregate":"myCollection","pipeline":[...]})'
        );
    }

    return {pipeline, collections: resolvedCollections};
}

module.exports = {normalizePipelineInput};
