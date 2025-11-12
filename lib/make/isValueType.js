const $check = require('check-types');
const {ObjectId} = require('bson');
/**
 * Create a regex from a sql like command
 * @param {string } type - the like value to turn into a regex
 * @param {*} value - the value
 * @param {import('../types').NoqlContext} _context - The Noql context to use when generating the output
 * @returns {boolean} - the regex equivalent
 */
function isValueType(type, value, _context) {
    if (!type) {
        return false;
    }
    const isKnownValueType = [
        'single_quote_string',
        'backticks_quote_string',
        'string',
        'hex_string',
        'full_hex_string',
        'bit_string',
        'double_quote_string',
        'boolean',
        'null',
        'var_string',
        'date',
        'datetime',
        'time',
        'timestamp',
        'number',
        'bool',
    ].includes(type);
    if (isKnownValueType) {
        return true;
    }
    if (type === 'function') {
        if (!$check.assigned(value)) {
            return false;
        }
        if (value === '$$NOW') {
            return false;
        }
        return (
            $check.primitive(value) ||
            $check.date(value) ||
            ObjectId.isValid(value)
        );
    }
    if (type === 'expr_list') {
        if (!$check.array(value)) {
            return false;
        }
        return value.reduce(
            (a, v) =>
                a &&
                ($check.primitive(v) ||
                    $check.date(v) ||
                    ObjectId.isValid(value) ||
                    isValueType(v.type, v.value)),
            true
        );
    }
    return false;
}

module.exports = {isValueType};
