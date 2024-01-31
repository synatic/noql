const {EJSON} = require('bson');
const {MongoClient, ObjectId} = require('mongodb');
const $schema = require('@synatic/schema-magic');
const fs = require('fs/promises');
const Path = require('path');
const {Client} = require('pg');
const $check = require('check-types');

const connectionString = 'mongodb://127.0.0.1:27017';
const dbName = 'sql-to-mongo-test';
/** @type {import('mongodb').MongoClient} */
let mongoClient;
/** @type {import('mongodb').Db} */
let db;
/** @type {import('pg').Client} */
let pgClient;

const defaultConnectionOptions = {
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'sql123#',
};

const shouldAddDataToPg = true;
const maxRowsToInsert = 1;

async function connect() {
    console.log('About to connect to db');
    mongoClient = new MongoClient(connectionString);
    await mongoClient.connect();
    console.log('Connected!');
    db = mongoClient.db(dbName);
}

/**
 *
 * @param {object[]} values
 * @param {string} collectionName
 * @returns {Promise<import('../test-types').SchemaDoc>}
 */
async function generateSchema(values, collectionName) {
    if (!mongoClient || !db) {
        throw new Error('Call connect before addTestData');
    }
    const schema = $schema.mergeSchemas(
        values
            .slice(0, 100)
            .filter((v) => Boolean)
            .map((v) => $schema.generateSchemaFromJSON(v))
    );
    const flattenedSchema = $schema.flattenSchema(schema, {
        additionalProperties: ['displayOptions'],
    });
    const schemaDoc = {
        collectionName,
        schema,
        flattenedSchema,
    };
    await db.collection('schemas').insertOne(schemaDoc);
    return schemaDoc;
}

async function addTestData() {
    if (!mongoClient || !db) {
        throw new Error('Call connect before addTestData');
    }
    const dataDirectory = './test/exampleData/';
    const files = await fs.readdir(dataDirectory);
    for (const file of files) {
        const searchString = '.json';
        const jsonIndex = file.lastIndexOf(searchString);
        if (jsonIndex < 0) {
            continue;
        }
        const collectionName = file.substring(0, jsonIndex);
        const filePath = Path.join(dataDirectory, file);
        const dataString = await fs.readFile(filePath, {encoding: 'utf-8'});
        const data = EJSON.parse(dataString);
        for (const item of data) {
            if (item._id && typeof item._id === 'string') {
                item._id = new ObjectId(item._id);
            }
        }
        await db.collection(collectionName).bulkWrite(
            data.map((d) => {
                return {insertOne: {document: d}};
            })
        );
        const schemaDoc = await generateSchema(data, collectionName);
        if (shouldAddDataToPg) {
            await addDataToPg(data, schemaDoc);
        }
    }
}

async function dropTestDb() {
    if (!mongoClient || !db) {
        throw new Error('Call connect before dropTestDb');
    }
    const {databases} = await mongoClient.db().admin().listDatabases();
    if (databases.findIndex((d) => d.name === dbName) > -1) {
        await mongoClient.db(dbName).dropDatabase();
    }
}

async function setup() {
    // todo don't connect/disconnect from pg if not going to add data
    await connect();
    await dropTestDb();
    await dropPgDbAndRecreate();
    await addTestData();
    await pgClient.end();
    return {db, client: mongoClient, dbName};
}

async function disconnect() {
    const promises = [];
    if (pgClient) {
        promises.push(pgClient.end());
    }
    promises.push(mongoClient.close());
    return await Promise.all(promises);
}

async function dropPgDbAndRecreate() {
    let client = new Client({
        ...defaultConnectionOptions,
        database: `postgres`,
    });
    await client.connect();
    try {
        await client.query(`DROP DATABASE "${dbName}" WITH (FORCE);`);
    } catch (err) {
        // happens if db didn't exist
    }
    try {
        await client.query(`
            CREATE DATABASE "${dbName}"
                WITH
                OWNER = postgres
                ENCODING = 'UTF8'
                CONNECTION LIMIT = -1
                IS_TEMPLATE = False;`);
    } catch (err) {
        // if db already existed, will throw error
        if (err.code !== '42P04') {
            throw err;
        }
    }
    await client.end();
    client = new Client({
        ...defaultConnectionOptions,
        database: dbName,
    });
    await client.connect();
    pgClient = client;
}

/**
 * @param {Record<string,unknown>[]} data
 * @param {import('../test-types').SchemaDoc} schemaDoc
 * @returns {Promise<void>}
 */
async function addDataToPg(data, schemaDoc) {
    delete schemaDoc.schema.properties._id;
    const createTableStatement =
        getDropAndCreateTableStatementFromSchema(schemaDoc);

    await pgClient.query(createTableStatement);

    const tableName = schemaDoc.collectionName;

    const schemaInfo = Object.entries(schemaDoc.schema.properties).map(
        ([key, value]) => {
            /** @type {import('json-schema').JSONSchema7TypeName} */
            const type = value.type;
            return {key, type, schema: value};
        }
    );
    const columnNames = schemaInfo.map((s) => `"${s.key}"`).join(', ');
    let insertStatement = `INSERT INTO "${tableName}" (${columnNames}) VALUES\n`;
    const dataToInsert = data.slice(0, maxRowsToInsert).map((d) => {
        const data = {...d};
        delete data._id;
        return data;
    });
    const rows = dataToInsert.map((row) => {
        const values = schemaInfo.map((column) => {
            const value = row[column.key];
            return convertValue(
                value,
                column.type,
                column.schema,
                tableName,
                column.key
            );
        });
        return `(${values.join(', ')})`;
    });
    insertStatement += `${rows.join(',\n')}`;
    insertStatement += ';';
    try {
        await pgClient.query(insertStatement);
    } catch (err) {
        console.log(insertStatement);
        console.error(err);
        throw err;
    }
}
/**
 * @param {import('../test-types').SchemaDoc}schemaDoc
 * @returns {string}
 */
function getDropAndCreateTableStatementFromSchema(schemaDoc) {
    const hasUnderscoreId = !!schemaDoc.schema.properties._id;
    const tableName = schemaDoc.collectionName;
    return `
        DROP TABLE IF EXISTS public."${tableName}";

        CREATE TABLE IF NOT EXISTS public."${tableName}"
        (
            ${
                hasUnderscoreId
                    ? '"_id" text COLLATE pg_catalog."default" NOT NULL,'
                    : ''
            }
            ${Object.entries(schemaDoc.schema.properties)
                .map((prop) =>
                    getColumnStatement(prop, schemaDoc.schema.required)
                )
                .filter(Boolean)
                .join(',\n\t\t\t')}${hasUnderscoreId ? ',' : ''}
            ${
                hasUnderscoreId
                    ? `CONSTRAINT ${tableName.replace(
                          /-/g,
                          '_'
                      )}_pkey PRIMARY KEY ("_id")`
                    : ''
            }
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS public."${tableName}"
            OWNER to postgres;
    `;
}
/**
 * @param {[string,import('json-schema').JSONSchema7Definition]} property
 * @param {string[]} requiredProps
 * @returns {string}
 */
function getColumnStatement([name, property], requiredProps) {
    if (typeof property === 'boolean') {
        return '';
    }
    if (name === '_id') {
        return '';
    }
    const nullable = true; // requiredProps.indexOf(name) >= 0;
    const dataType = getColumnFromSchema(property);
    return `"${name}" ${dataType}${nullable ? '' : ' NOT NULL'}${
        dataType.indexOf('text') >= 0 ||
        dataType.indexOf('character varying') >= 0
            ? ' COLLATE pg_catalog."default"'
            : ''
    }`;
}
/**
 * @param {import('json-schema').JSONSchema7Definition} schema
 * @returns {string}
 */
function getColumnFromSchema(schema) {
    const defaultType = 'text';
    if (typeof schema === 'boolean') {
        return defaultType;
    }
    let schemaPartType = schema.type;
    if ($check.array(schemaPartType)) {
        schemaPartType = schemaPartType.filter((p) => p !== 'null')[0];
    }

    if (schemaPartType === 'integer') {
        return 'numeric';
    } else if (schemaPartType === 'number') {
        return 'numeric';
    } else if (schemaPartType === 'string' && schema.format === 'date-time') {
        return 'timestamp with time zone';
    } else if (schemaPartType === 'boolean') {
        return 'boolean';
    } else if (schemaPartType === 'string') {
        // const stringLen = Math.max(schemaPart.stringLength || 0, defaultStringLength);
        return 'text';
        // if (stringLen > 8000) {
        //     tempColumn.dataType = 'text';
        // } else {
        //     tempColumn.dataType = 'character varying';
        //     tempColumn.length = stringLen;
        // }
    } else if (schemaPartType === 'object') {
        return 'json';
    } else if (
        schemaPartType === 'array' &&
        schema.items &&
        schema.items.type
    ) {
        if (schema.items.type === 'integer') {
            return 'numeric[]';
        } else if (schema.items.type === 'number') {
            return 'numeric[]';
        } else if (schema.items.type === 'boolean') {
            return 'boolean[]';
        } else if (
            schema.items.type === 'string' &&
            schema.items.format === 'date-time'
        ) {
            return 'timestamp with time zone[]';
        } else if (schema.items.type === 'string') {
            return 'text[]';
        } else if (schema.items.type === 'object') {
            return 'json[]';
        } else if (schema.items.type === 'array') {
            return 'json[]';
        } else {
            return 'text[]';
        }
    } else if (schemaPartType === 'array') {
        return 'json[]';
    }

    return defaultType;
}

/**
 *
 * @param {unknown} value
 * @param {import('json-schema').JSONSchema7Type} type
 * @param {import('json-schema').JSONSchema7Definition} schema
 * @param {string} tableName
 * @param {string} columnName
 * @returns {string}
 */
function convertValue(value, type, schema, tableName, columnName) {
    if (Array.isArray(type)) {
        const filteredType = type.filter((t) => t !== 'null');
        if (filteredType.length > 1) {
            if (type.indexOf('string') >= 0) {
                type = 'string';
            } else {
                throw new Error(
                    `Array types "[${type.join(
                        ', '
                    )}]" not yet supported, from table "${tableName}", column "${columnName}", schema:\n${JSON.stringify(
                        schema,
                        null,
                        4
                    )}`
                );
            }
        } else {
            type = filteredType[0];
        }
    }
    if ($check.not.assigned(value)) {
        return 'NULL';
    }
    if (type === 'string') {
        if (schema.format === 'date-time') {
            return `'${
                value.toISOString().replace('T', ' ').substring(0, 22) + '+00'
            }'`;
        }
        return `'${value}'`;
    }
    if (type === 'integer') {
        return `${value}`;
    }
    if (type === 'number') {
        return `${value}`;
    }
    if (type === 'boolean') {
        return `${value}`;
    }
    if (type === 'object') {
        return `'${JSON.stringify(value, null, 0)}'`;
    }
    if (type === 'array') {
        if (!Array.isArray(value)) {
            throw new Error(
                `JSON schema types is array but value was not, from table "${tableName}", column "${columnName}", schema:\n${JSON.stringify(
                    schema,
                    null,
                    4
                )}`
            );
        }

        if (schema.items) {
            if (schema.items.type === 'object') {
                const convertedValues = value.map((v) =>
                    convertValue(v, 'object', schema, tableName, columnName)
                );
                return `array[${convertedValues.join(',')}]::json[]`;
            }
            if (
                schema.items.type === 'integer' ||
                schema.items.type === 'number'
            ) {
                return `array[${value.join(',')}]`;
            }
            if (schema.items.type === 'string') {
                return `array[${value.map((v) => `'${v}'`).join(',')}]`;
            }
        }
    }

    throw new Error(
        `Unsupported type "${type}" from table "${tableName}", column "${columnName}", schema:\n${JSON.stringify(
            schema,
            null,
            4
        )}`
    );
}

module.exports = {connect, addTestData, dropTestDb, setup, disconnect, dbName};
