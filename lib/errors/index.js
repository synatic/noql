const cModule = require('./column-does-not-exist-error');
const tModule = require('./table-does-not-exist-error');

module.exports = {...cModule, ...tModule};
