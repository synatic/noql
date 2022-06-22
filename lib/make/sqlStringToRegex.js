/* eslint-disable security/detect-non-literal-regexp */

/**
 * Create a regex from a sql like command
 *
 * @param {string } likeVal - the like value to turn into a regex
 * @returns {string|null} - the regex equivalent
 */
function sqlStringToRegex(likeVal) {
    if (!likeVal) {
        return null;
    }
    let regex;
    if (likeVal.startsWith('%') && likeVal.endsWith('%')) {
        regex = new RegExp(`${likeVal.substring(1, likeVal.length - 1)}`);
    } else if (likeVal.startsWith('%')) {
        regex = new RegExp(`${likeVal.substring(1)}$`);
    } else if (likeVal.endsWith('%')) {
        regex = new RegExp(`^${likeVal.substring(0, likeVal.length - 1)}`);
    } else {
        regex = new RegExp(`^${likeVal}$`);
    }
    return regex.source;
}

module.exports = {sqlStringToRegex};
