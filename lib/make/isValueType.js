const $check = require('check-types');
const {ObjectId} = require('bson');
/**
 * Create a regex from a sql like command
 *
 * @param {string } type - the like value to turn into a regex
 * @param {*} [value] - the value
 * @returns {boolean} - the regex equivalent
 */
function isValueType(type, value) {
    if (!type) {
        return false;
    }

    return (
        [
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
        ].includes(type) ||
        (type === 'function' &&
            $check.assigned(value) &&
            ($check.primitive(value) ||
                $check.date(value) ||
                ObjectId.isValid(value))) ||
        (type === 'expr_list' &&
            $check.array(value) &&
            value.reduce(
                (a, v) =>
                    a &&
                    ($check.primitive(v) ||
                        $check.date(v) ||
                        ObjectId.isValid(value) ||
                        isValueType(v.type, v.value)),
                true
            ))
    );
}

module.exports = {isValueType};
