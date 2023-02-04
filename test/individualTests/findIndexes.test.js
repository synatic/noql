const findIndexes = require('../../lib/findIndexes');
const assert = require('assert');

describe('Find Indexes', function () {
    describe('findIndexesOnMatch', function () {
        it('should check a simple match', () => {
            assert.deepEqual(
                findIndexes.findIndexesOnMatch({a: 'testval', b: 'testval2'}),
                [{a: 1, b: 1}]
            );
        });

        it('should check with $and', () => {
            assert.deepEqual(
                findIndexes.findIndexesOnMatch({
                    $and: [
                        {
                            $and: [
                                {
                                    name: {
                                        $eq: 'test',
                                    },
                                },
                                {
                                    title: {
                                        $eq: 'test2',
                                    },
                                },
                            ],
                        },
                        {
                            col1: {
                                $eq: 'test3',
                            },
                        },
                    ],
                }),
                [{name: 1, title: 1, col1: 1}]
            );
        });
    });
});
