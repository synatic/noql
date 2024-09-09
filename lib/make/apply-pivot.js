const {functionByName} = require('../MongoFunctions');
const merge = require('lodash/merge');

module.exports = {applyPivot, applyUnpivot, applyMultipleUnpivots};

/**
 *
 * @param {string} pivotString
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 */
function applyPivot(pivotString, pipeline, context) {
    const pivot = createJSONFromPivotString('pivot', pivotString);

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
                    v:
                        pivot.fields.length > 1
                            ? {
                                  ...pivot.fields.reduce(
                                      (previousValue, currentValue) => {
                                          previousValue[currentValue.name] =
                                              `$${currentValue.name}`;
                                          return previousValue;
                                      },
                                      {}
                                  ),
                              }
                            : `$${pivot.fields[0].name}`,
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
 * @param {'pivot'|'unpivot'} type
 * @param {string} inputString
 * @returns {{columns: (*|*[]), for: (*|string), fields: *[]}}
 */
function createJSONFromPivotString(type, inputString) {
    // Split the string by '|'
    const parts = inputString.split('|');

    // Extract the field from the pivot function
    let pivotPart = parts[1];
    let fieldMatch;
    if (type === 'pivot') {
        fieldMatch = pivotPart.match(/pivot\(\[(.*?)\]/);
    } else {
        fieldMatch = pivotPart.match(/unpivot\(\[(.*?)\]/);
    }

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
    const columns = columnsMatch
        ? columnsMatch[1].split(',').map((c) => c.trim())
        : [];

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
 * @param {import('../types').PipelineFn} projection
 * @param {import('../types').NoqlContext} context
 */
function applyUnpivot(pivotString, pipeline, projection, context) {
    const unpivot = createJSONFromPivotString('unpivot', pivotString);

    const columnsToExclude = [unpivot.for, '_id']
        .concat(unpivot.columns)
        .concat(unpivot.fields);
    const columns = Object.keys(projection.$project).filter(
        (val) => columnsToExclude.indexOf(val) === -1
    );
    pipeline.push({
        $project: {
            ...columns.reduce((previousValue, currentValue) => {
                previousValue[currentValue] = `$${currentValue}`;
                return previousValue;
            }, {}),
            fields: {
                $objectToArray: '$$ROOT',
            },
        },
    });
    pipeline.push({
        $project: {
            ...columns.reduce((previousValue, currentValue) => {
                previousValue[currentValue] = 1;
                return previousValue;
            }, {}),
            fields: {
                $filter: {
                    input: '$fields',
                    as: 'field',
                    cond: {
                        $and: columns
                            .map((c) => ({$ne: ['$$field.k', c]}))
                            .concat([
                                {
                                    $or: unpivot.columns.map((c) => ({
                                        $eq: ['$$field.k', c],
                                    })),
                                },
                            ]),
                    },
                },
            },
        },
    });
    pipeline.push({
        $unwind: '$fields',
    });
    pipeline.push({
        $project: {
            ...columns.reduce((previousValue, currentValue) => {
                previousValue[currentValue] = 1;
                return previousValue;
            }, {}),
            [unpivot.for]: '$fields.k',
            [unpivot.fields[0]]: '$fields.v', // Assuming 'Orders' should be numeric
        },
    });
}

/**
 *
 * @param {string[]} pivotStrings
 * @param pivotString
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').PipelineFn} projection
 * @param {import('../types').NoqlContext} context
 */
function applyMultipleUnpivots(pivotStrings, pipeline, projection, context) {
    const unpivots = pivotStrings.map((s) =>
        createJSONFromPivotString('unpivot', s)
    );
    const unpivot = unpivots.reduce(
        (previousValue, currentValue) => {
            previousValue.for.push(currentValue.for);
            previousValue.fields.push(...currentValue.fields);
            previousValue.columns.push(...currentValue.columns);
            return previousValue;
        },
        {fields: [], for: [], columns: []}
    );
    const columnsToExclude = [...unpivot.for, '_id']
        .concat(unpivot.columns)
        .concat(unpivot.fields);
    const columns = Object.entries(projection.$project)
        .filter(
            ([key, expression]) =>
                columnsToExclude.indexOf(key) === -1 &&
                expression.indexOf('.') > 0
        )
        .map(([key]) => key);
    pipeline.push({
        $project: {
            ...columns.reduce((previousValue, currentValue) => {
                previousValue[currentValue] = `$${currentValue}`;
                return previousValue;
            }, {}),
            fields: {
                $objectToArray: '$$ROOT',
            },
        },
    });
    pipeline.push({
        $project: {
            ...columns.reduce((previousValue, currentValue) => {
                previousValue[currentValue] = 1;
                return previousValue;
            }, {}),
            fields: {
                $filter: {
                    input: '$fields',
                    as: 'field',
                    cond: {
                        $and: columns
                            .map((c) => ({$ne: ['$$field.k', c]}))
                            .concat([
                                {
                                    $or: unpivot.columns.map((c) => ({
                                        $eq: ['$$field.k', c],
                                    })),
                                },
                            ]),
                    },
                },
            },
        },
    });
    pipeline.push({
        $unwind: '$fields',
    });
    const columnProjection = {
        $project: {
            ...columns.reduce((previousValue, currentValue) => {
                previousValue[currentValue] = 1;
                return previousValue;
            }, {}),
        },
    };
    for (const unpivotOp of unpivots) {
        columnProjection.$project[unpivotOp.fields[0]] = {
            $cond: {
                if: {
                    $or: unpivotOp.columns.map((c) => ({
                        $eq: ['$fields.k', c],
                    })),
                },
                then: '$fields.v',
                else: '$$REMOVE',
            },
        };
    }
    pipeline.push(columnProjection);
    pipeline.push({
        $group: {
            _id: columns.reduce((previousValue, currentValue) => {
                previousValue[currentValue] = `$${currentValue}`;
                return previousValue;
            }, {}),
            ...unpivots.reduce((previousValue, currentValue) => {
                previousValue[currentValue.for] = {
                    $push: `$${currentValue.fields[0]}`,
                };
                return previousValue;
            }, {}),
        },
    });
    pipeline.push({
        $unwind: {
            path: `$${unpivot.for[0]}`,
            includeArrayIndex: '__unwindIndex',
            preserveNullAndEmptyArrays: false,
        },
    });
    const restOfFor = unpivot.for.splice(1);
    const restOfFields = unpivot.fields.splice(1);
    pipeline.push({
        $project: {
            _id: 0,
            ...columns.reduce((previousValue, currentValue) => {
                previousValue[currentValue] = `$_id.${currentValue}`;
                return previousValue;
            }, {}),
            [unpivot.fields[0]]: `$${unpivot.for[0]}`,
            ...restOfFields.reduce(
                (previousValue, currentValue, currentIndex) => {
                    previousValue[currentValue] = {
                        $arrayElemAt: [
                            `$${restOfFor[currentIndex]}`,
                            '$__unwindIndex',
                        ],
                    };
                    return previousValue;
                },
                {}
            ),
        },
    });
    console.log('');
    /**
     *
     */
}
