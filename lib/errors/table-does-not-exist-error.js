class TableDoesNotExistError extends Error {
    /**
     * @param {string} tableName
     * @param {string} statement
     */
    constructor(tableName, statement) {
        const message = `Table "${tableName}" does not exist.\n${statement}`;
        super(message);
    }
}
module.exports = {TableDoesNotExistError};
