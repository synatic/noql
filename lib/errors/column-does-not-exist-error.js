class ColumnDoesNotExistError extends Error {
    /**
     * @param {string} columnName
     * @param {string} tableName
     * @param {string} statement
     */
    constructor(columnName, tableName, statement) {
        const message = `Column "${columnName}" from table "${tableName}" does not exist.\n${statement}`;
        super(message);
    }
}
module.exports = {ColumnDoesNotExistError};
