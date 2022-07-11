const SQLParser = require('../../lib/SQLParser.js');
const assert = require('assert');

describe('Array Sort', function () {
    it('should be able to build an AST without an exception', () => {
        assert.doesNotThrow(() => {
            SQLParser.parseSQLtoAST(
                'SELECT id, (select * from Rentals order by `Rental Date` desc) AS OrderedRentals FROM `customers`'
            );
        });
    });
    it('should build the mongodb aggregation pipeline with a sort', () => {
        SQLParser.makeMongoAggregate(
            'SELECT id, (select * from Rentals order by `Rental Date` desc) AS OrderedRentals FROM `customers`'
        );
    });
});
