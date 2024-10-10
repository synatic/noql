const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const isoWeek = require('dayjs/plugin/isoWeek');
const localizedFormat = require('dayjs/plugin/localizedFormat');
const customParseFormat = require('dayjs/plugin/customParseFormat');

dayjs.extend(utc);
dayjs.extend(isoWeek);
dayjs.extend(localizedFormat);
dayjs.extend(customParseFormat);

module.exports = {dayjs};
