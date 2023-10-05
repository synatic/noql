const {Long} = require('bson');

module.exports = {formatLargeNumber};
/**
 * Takes in a numerical input and ensures that mongodb will be able to parse it
 *
 * @param {string|number} input The input value
 * @returns {number| Long} The output value
 */
function formatLargeNumber(input) {
    if (typeof input === 'string') {
        const number = Number(input);
        if (Number.isNaN(number)) {
            throw new Error(`String input: "${input}" was not a number`);
        }
        if (number > Number.MAX_SAFE_INTEGER) {
            return Long.fromString(input);
        }
        return number;
    }
    if (typeof input === 'number') {
        return input;
    }
    throw new Error(
        `Input "${input}" was of type ${typeof input} which is not supported by formatLargeNumber`
    );
}
