
function constructShellQuery(parsedSQL) {
    var nodeQuery = '';
    if (parsedSQL.type === 'query') {
        nodeQuery = 
`db.${parsedSQL.collection}
  .find(${JSON.stringify(parsedSQL.query || {})} , ${JSON.stringify(parsedSQL.projection || {})})
  .sort(${JSON.stringify(parsedSQL.sort || {})})
  .limit(${parsedSQL.limit || 50}));
`;
    }
    else if (parsedSQL.type === 'aggregate') {
        nodeQuery =
`db.${parsedSQL.collections[0]}
    .aggregate(
        ${JSON.stringify(parsedSQL.pipeline || {}, null, 4)}
    )
    .toArray()
`;
    }
    return nodeQuery;
}

