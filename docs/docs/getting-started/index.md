---
hide:
  - navigation
---

# Getting Started


## Installation

Install NoQL using the [npm install command](https://docs.npmjs.com/downloading-and-installing-packages-locally):

```bash
npm i @synatic/noql --save
```

## Usage

NoQL outputs an object with the type, either `query` or `aggregate`, along with the components of the Mongo query. To use the output object, construct a query with `MongoClient` from the [MongoDB NodeJS Driver](https://www.npmjs.com/package/mongodb): 

???+ example "Example usage with `MongoClient`"

    Node.js:

    ```js
    const SQLParser = require('@synatic/noql');
    const {MongoClient} = require('mongodb');

    (async () => {
        try {
            client = new MongoClient('mongodb://127.0.0.1:27017');
            await client.connect();
            const db = client.db('noql-test');

            const parsedSQL = SQLParser.parseSQL(
                "select id from `films` limit 10", 
                { database: 'postgresql' /* or 'mysql' */ } });
            if (parsedSQL.type === 'query') {
                console.log(
                    await db
                        .collection(parsedSQL.collection)
                        .find(parsedSQL.query || {}, parsedSQL.projection || {})
                        .limit(parsedSQL.limit || 50)
                        .toArray()
                );
            } else if (parsedSQL.type === 'aggregate') {
                console.log(
                    await db
                        .collection(parsedSQL.collections[0])
                        .aggregate(parsedSQL.pipeline)
                        .toArray()
                );
            }
        } catch (exp) {
            console.error(exp);
        }
    })();
    ```
