const {functionByName} = require('../MongoFunctions');
module.exports = {applyPivot, applyUnpivot};

/**
 *
 * @param {string} pivotString
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 */
function applyPivot(pivotString, pipeline, context) {
    const pivot = createJSONFromPivotString(pivotString);

    pivot.fields = pivot.fields.map((field) => {
        const functionName = field.split('(')[0];
        const foundFunction = functionByName(functionName);
        if (!foundFunction) {
            throw new Error(
                `Unable to find function "${functionName}" in pivot.fields."`
            );
        }
        let argumentString = field.replace(functionName, '');
        let name = '';
        if (argumentString.indexOf(' as ') >= 0) {
            const parts = argumentString.split(' as ');
            argumentString = parts[0];
            name = parts[1];
        }
        argumentString = argumentString.substring(1, argumentString.length - 1);
        const rawArguments = argumentString.split(',');
        if (!name) {
            name = rawArguments[0];
        }
        const parsedArguments = rawArguments.map((arg) =>
            isNumeric(arg) ? parseFloat(arg) : `$${arg}`
        );
        return {
            name,
            foundFunction,
            parsedArguments,
        };
    });
    pipeline.push({
        $group: {
            _id: `$${pivot.for}`,
            ...pivot.fields.reduce((previousValue, currentValue) => {
                const res = currentValue.foundFunction.parse(
                    ...currentValue.parsedArguments
                );
                previousValue[currentValue.name] = res;
                return previousValue;
            }, {}),
        },
    });
    pipeline.push({
        $group: {
            _id: null,
            data: {
                $push: {
                    k: {
                        $toString: '$_id',
                    },
                    v: {
                        ...pivot.fields.reduce(
                            (previousValue, currentValue) => {
                                previousValue[currentValue.name] =
                                    `$${currentValue.name}`;
                                return previousValue;
                            },
                            {}
                        ),
                    },
                },
            },
        },
    });
    pipeline.push({
        $project: {
            _id: 0,
            data: {
                $arrayToObject: '$data',
            },
        },
    });
    pipeline.push({
        $project: {
            result: {
                $mergeObjects: [
                    {
                        ...pivot.columns.reduce(
                            (previousValue, currentValue) => {
                                previousValue[currentValue] = null;
                                return previousValue;
                            },
                            {}
                        ),
                    },
                    '$data',
                ],
            },
        },
    });
    pipeline.push({
        $replaceRoot: {
            newRoot: '$result',
        },
    });
}

/**
 *
 * @param {string} str
 * @returns {boolean}
 */
function isNumeric(str) {
    if (typeof str != 'string') {
        return false;
    }
    return !isNaN(str) && !isNaN(parseFloat(str));
}

/**
 *
 * @param {string} inputString
 * @returns {{columns: (*|*[]), for: (*|string), fields: *[]}}
 */
function createJSONFromPivotString(inputString) {
    // Split the string by '|'
    const parts = inputString.split('|');

    // Extract the field from the pivot function
    let pivotPart = parts[1];
    const fieldMatch = pivotPart.match(/pivot\(\[(.*?)\]/);
    const fields = fieldMatch
        ? fieldMatch[1].split(',').map((f) => f.trim())
        : [];

    pivotPart = pivotPart.replace(fieldMatch[0], '');
    // Extract the 'for' part
    const forMatch = pivotPart.match(/,(.*?),\[/);
    const forPart = forMatch ? forMatch[1] : '';

    pivotPart = pivotPart.replace(forMatch[0], '');
    // Extract the columns
    const columnsMatch = pivotPart.match(/(.*?)\]/);
    const columns = columnsMatch ? columnsMatch[1].split(',').map(Number) : [];

    // Construct the JSON object
    return {
        fields: fields,
        for: forPart,
        columns: columns,
    };
}

/**
 *
 * @param {string} pivotString
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 */
function applyUnpivot(pivotString, pipeline, context) {
    const unpivot = createJSONFromPivotString(pivotString);
    console.log(unpivot);
}
