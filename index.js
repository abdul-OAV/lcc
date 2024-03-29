/* eslint no-undefined: 0 */
/* eslint prefer-reflect: 0 */
/* eslint max-params: 0 */
/* eslint no-param-reassign: 0 */
/* eslint no-plusplus: 0 */
/* eslint guard-for-in: 0 */
/* eslint complexity: 0 */
const Connector = require('loopback-connector').Connector;
const Promise = require('bluebird');
const {
  CosmosClient,
  PartitionKey,
//   ConnectionPolicy,
//   RetryOptions,
} = require("@azure/cosmos");
const util = require('util');
const debug = require('debug')('oav:ss:connectors:cosmosdb');
exports.initialize = function initialize(dataSource, callback) {
    if (!CosmosClient) {
        return;
    }
    const settings = dataSource.settings;
    dataSource.connector = new CosmosDB(settings);
    if (callback) {
        dataSource.connector.connect(callback);
    }
};

function CosmosDB(settings) {
    Connector.call(this, 'cosmosdb', settings);
    this.host = settings.host;
    this.masterKey = settings.masterKey;
    this.databaseName = settings.databaseName || undefined;
    // this.collectionName = settings.collectionName || undefined;
    this.enableCrossPartitionQuery = settings.enableCrossPartitionQueries || false;
    this.partitionKey = settings.partitionKey || undefined;
    this.client = null;
    this.queryOptions = {
        disableDefaultOrdering: settings.disableDefaultOrdering,
        chargeWarningThreshold: settings.chargeWarningThreshold
    };

    if (this.databaseName !== undefined) {
        debug('Selected %j database', this.databaseName);
    }
    // if (this.databaseName !== undefined && this.collectionName !== undefined) {
    //     debug('Selected %j collection in %j database', this.collectionName, this.databaseName);
    // }

    // Initialize a retry policy.
    if (settings.retry !== null && settings.retry !== undefined) {
        this.retryPolicy = {
            max: settings.retry.max
        };
    }
}

util.inherits(CosmosDB, Connector);

CosmosDB.prototype.connect = function connect(callback) {
    if (this.databaseName === undefined) {
        return callback(new Error('No database selected, it must be provided in dataSource.settings'));
    }

    // if (this.collectionName === undefined) {
    //     return callback(new Error('No collection selected, it must be provided in dataSource.settings'));
    // }

    debug('Connecting to %j with retry policy %j', this.host, this.retryPolicy);

    const connectionPolicy = {};
    if (this.retryPolicy && this.retryPolicy.max) {
        connectionPolicy.retryOptions = {maxRetryAttemptCount: this.retryPolicy.max};
    }

    this.client = new CosmosClient({ endpoint: this.host, key: this.masterKey, connectionPolicy: {} });
    return callback(null);
};

CosmosDB.prototype.disconnect = function disconnect(callback) {
    debug('Disconnecting from %j', this.host);
    this.client = null;
    return callback(null);
};

CosmosDB.prototype.ping = async function ping(cb) {
    try {
      // console.log(ping);
    //   const { result } = await this.client.database(this.databaseName).read();
    await this.client.database(this.databaseName).read();
      // console.log(result);
      return cb(null, 'ok');
        // const querySpec = { query: 'SELECT TOP 1 * FROM c', parameters: [] };
        // const { result: items, headers } = await this.client.database(this.databaseName).container(this.collectionName)
        //     .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).executeNext();
        // checkRequestCharge(headers, querySpec, this.queryOptions, this.collectionName);
        //
        // items && items[0] ? cb(null, 'ok') : cb(error);
    } catch (error) {
      return cb(error);
    }
};

CosmosDB.prototype.create = async function create(model, data, options, callback) {
    try {
        const { item, headers } = await this.client.database(this.databaseName).container(model).items.create(data);
        // const { item } = await this.client.database(this.databaseName).container(this.collectionName).items.create(data);
        checkRequestCharge(headers, {query: `create ${JSON.stringify(data)}`}, this.queryOptions, model);

        // debug('%j document created in %j (%j)', item.id, model, this.databaseName);
        // debug('%j document created in %j (%j)', item.id, this.collectionName, this.databaseName);
        return callback(null, item.id);
    } catch (error) {
        return callback(error);
    }
};

CosmosDB.prototype.updateOrCreate = async function updateOrCreate(model, data, options, callback) {
    const modelDefinition = this.getModelDefinition(model);
    try {
        const result = await this.client.database(this.databaseName).container(model).items.upsert(data);
        const { resource, headers } = result;
        checkRequestCharge(headers, {query: `create/update/replace/save ${JSON.stringify(data)}`}, this.queryOptions, model);

        if (!resource) {
          // console.log(result);
        }
        // debug('%j document updated in %j (%j)', resource.id, model, this.databaseName);
        // const { resource } = await this.client.database(this.databaseName).container(this.collectionName).items.upsert(data);
        //
        // debug('%j document updated in %j (%j)', resource.id, this.collectionName, this.databaseName);
        return callback(null, dropNonViewProperties(modelDefinition, resource));
    } catch (error) {
        return callback(error);
    }
};

CosmosDB.prototype.replaceOrCreate = CosmosDB.prototype.updateOrCreate;
CosmosDB.prototype.save = CosmosDB.prototype.updateOrCreate;

CosmosDB.prototype.replaceById = async function replaceById(model, id, data, options, callback) {
  // console.log('replaceById');
  if (id === null || id === undefined) {
    return callback(new Error('ID value is required'));
  }

  const modelDefinition = this.getModelDefinition(model);
  try {
    // console.log('replcaeById');
    // console.log({id, data});
    const { resource, headers } = await this.client.database(this.databaseName).container(model)
      .item(id).replace(data);
    // debug('%j document replaced in %j (%j)', resource.id, model, this.databaseName);
    checkRequestCharge(headers, {query: `replaceById ${id} ${JSON.stringify(data)}`}, this.queryOptions, model);
    // const { resource } = await this.client.database(this.databaseName).container(this.collectionName)
    //     .item(id).replace(data);
    //
    // debug('%j document replaced in %j (%j)', resource.id, this.collectionName, this.databaseName);
    return callback(null, dropNonViewProperties(modelDefinition, resource));
  } catch (error) {
    return callback(error);
  }
};

CosmosDB.prototype.all = async function all(modelName, filter, options, callback) {
    // debug('all', modelName, filter, options);
    const self = this;
    const modelDefinition = this.getModelDefinition(modelName);
    try {
        const querySpec = buildQuerySpecForModel(this.queryOptions, modelDefinition, filter);
        // debug(JSON.stringify(querySpec));
        // debug(querySpec);
        const container = this.client.database(this.databaseName).container(modelName);
        const output = await container.items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).fetchAll();
        // debug({output});
        const { resources: items, headers } = output;
        // debug({items});
        // debug(items[0]);
        // if (items[0]) {
        //   debug(items[0]._etag);
        // }
        checkRequestCharge(headers, querySpec, this.queryOptions, modelName);
        // const output = await this.client.database(this.databaseName).container(this.collectionName)
        //     .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).fetchAll();
        // // console.log(output);
        // const { resources: items, headers } = output;
        // checkRequestCharge(headers, querySpec, this.queryOptions, this.collectionName);
        const objs = items.map(x => dropNonViewProperties(modelDefinition, x));
        if (filter && filter.include) {
          return self._models[modelName].model.include(
            objs,
            filter.include,
            options,
            callback,
          );
        }
        return callback(null, objs);
    } catch (error) {
        return callback(error);
    }
};

CosmosDB.prototype.count = async function count(model, where, options, callback) {
  try {
    const querySpec = buildQuerySpecForModel(this.queryOptions, this.getModelDefinition(model), { where }, ['VALUE COUNT(1)']);
    const { resources: items, headers } = await this.client.database(this.databaseName).container(model)
    .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).fetchAll();
    // debug({items});
    checkRequestCharge(headers, querySpec, this.queryOptions, model);
    // const { result: items, headers } = await this.client.database(this.databaseName).container(this.collectionName)
    //     .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).toArray();
    //
    // checkRequestCharge(headers, querySpec, this.queryOptions, this.collectionName);
    if (!items) {
        return callback(null, null);
    }
    return items && items[0] && callback(null, items[0]);
  } catch (error) {
    return callback(error);
  }
};

CosmosDB.prototype.update = async function update(model, where, data, options, callback) {
    // console.log('update');
    const modelDefinition = this.getModelDefinition(model);
    try {
        // Translate data properties from view to DB.
        data = translateDBObjectFromView(modelDefinition, data);

        const querySpec = buildQuerySpecForModel(this.queryOptions, modelDefinition, { where });
        // debug(querySpec);
        const iterator = this.client.database(this.databaseName).container(model)
            .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).getAsyncIterator();
        // const iterator = this.client.database(this.databaseName).container(this.collectionName)
            // .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery });
        let totalItems = 0;
        const failures = [];
        // Recursively apply update operations to all matching documents.
        // Underlying client library makes sure that all documents are
        // fetched. We just travel through the query iterator and execute
        // replace for each document in sequence. Because CosmosDB has no
        // multi-document transactions, we do not stop on individual
        // errors but rather execute the whole query iterator until end.
        // At the end, we report the ratio of how many operations succeeded
        // to caller. So that they can decide what to do.
        // console.log({iterator});
        for await (const output of iterator) {
            // console.log(output);
            const {resources: items, headers} = output;
            // Execute replace to next item in iteration.
            if (items && items[0]) {
                checkRequestCharge(headers, querySpec, this.queryOptions, model);
                // checkRequestCharge(headers, querySpec, this.queryOptions, this.collectionName);
                const item = items[0];
                // console.log({item})
                totalItems += 1;

                // Set updated properties to item.
                Object.assign(item, data);

                await new Promise((resolve, reject) => {

                    this.replaceById(model, item.id, item, options, (error, output) => {
                        // console.log({error, output});
                        if (error) {
                            // console.log('Individual replace operator failed: %j', error);
                            failures.push(item.id);
                            return reject(error);
                        }
                        return resolve(output);
                            // Just move to next item, ignore possible errors.
                        //   next();
                    });
                });
            } else {
              // Finish up the operation and report the caller.
              // do nothing
            }
        }
        // console.log('done');
        // console.log('else');
        const numberOfSuccesses = totalItems - failures.length;
        // debug('Batch update success rate is %j', numberOfSuccesses / totalItems);

        return callback(null, {
            count: numberOfSuccesses,
            successRate: numberOfSuccesses / totalItems,
            failures
        });
        // const next = async () => {
        //     console.log('next');
        //     const { result: items, headers } = await iterator.next();
        //     console.log({items});

        //     // Execute replace to next item in iteration.
        //     if (items && items[0]) {
        //         checkRequestCharge(headers, querySpec, this.queryOptions, model);
        //         // checkRequestCharge(headers, querySpec, this.queryOptions, this.collectionName);
        //         const item = items[0];
        //         debug({item});
        //         Object.assign(item, data);
        //         debug({item});

        //         this.replaceById(model, item.id, item, options, function(error, result) {
        //             console.log({error, result});
        //             if (error) {
        //                 // console.log('Individual replace operator failed: %j', error);
        //                 failures.push(item.id);
        //             }

        //             // Just move to next item, ignore possible errors.
        //             next();
        //         });
        //     }

        //     // Finish up the operation and report the caller.
        //     else {
        //         const numberOfSuccesses = totalItems - failures.length;

        //         debug('Batch update success rate is %j', numberOfSuccesses / totalItems);

        //         return callback(null, {
        //             count: numberOfSuccesses,
        //             successRate: numberOfSuccesses / totalItems,
        //             failures: failures
        //         });
        //     }
        // };

        // next();
    } catch (error) {
        return callback(error);
    }
    // } catch (error) {
    //     console.log(error);
    //     return callback(error);
    // }
};

// CosmosDB.prototype.destroyAll = async function destroyAll(model, where, options, callback) {
//     debug('destroyAll', where, options);
//     const self = this;
//     const properties = ['c.id'];
//     const partitionKey = this.partitionKey;
//         // if (this.partitionKey) {
//     //     properties.push(`c.${this.partitionKey}`);
//     // }

//     try {
//         const querySpec = buildQuerySpecForModel(this.queryOptions, this.getModelDefinition(model), { where }, properties);
//         const container = this.client.database(this.databaseName).container(model);
//         // const iterator = container.items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).getAsyncIterator();
//         const {resources: items} = await this.client.database(this.databaseName).container(model)
//         .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).fetchAll();
//         const operations = items.map((item) => ({
//             operationType: 'Delete',
//             resourceBody: item
//         }));
//         debug(operations);
//         const result = await container.items.bulk(operations);
//         debug(result);
//         return result;
//         // debug({iterator});
//         // debug('iterator.fetchNext', iterator.fetchNext);
//         // const iterator = this.client.database(this.databaseName).container(this.collectionName)
//         //     .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery });

//         let totalItems = 0;
//         const failures = [];
//         for await (const {resources} of iterator) {
//             for await (const {id} of resources) {
//                 totalItems++;
//                 try {
//                     const item = await container.item(id, partitionKey);
//                     // debug(item);
//                     await item.delete();
//                 } catch (error) {
//                     debug(error);
//                     failures.push(id);
//                 }
//             }
//         }
//         const numberOfSuccesses = totalItems - failures.length;
//         debug('Batch destroy success rate is %j', numberOfSuccesses / totalItems);

//         return callback(null, {
//             count: numberOfSuccesses,
//             successRate: numberOfSuccesses / totalItems,
//             failures
//         });

//         return null;
//         // Recursively apply delete operations to all matching documents.
//         // Underlying client library makes sure that all documents are
//         // fetched. We just travel through the query iterator and execute
//         // delete for each document in sequence. Because CosmosDB has no
//         // multi-document transactions, we do not stop on individual
//         // errors but rather execute the whole query iterator until end.
//         // At the end, we report the ratio of how many operations succeeded
//         // to caller. So that they can decide what to do.
//         const next = async () => {
//             debug('start');
//             const { result: items, headers } = await iterator.fetchNext();
//             debug({items});
//             // Execute delete to next item in iteration.
//             if (items && items[0]) {
//                 checkRequestCharge(headers, querySpec, this.queryOptions, model);
//                 // checkRequestCharge(headers, querySpec, this.queryOptions, this.collectionName);
//                 const document = items[0];
//                 totalItems += 1;

//                 return self.destroyOne(model, document, options, (error) => {
//                     if (error) {
//                         debug('Individual delete operator failed: %j', error);
//                         failures.push(document.id);
//                     }

//                     // Just move to next item, ignore possible errors.
//                     next();
//                 });
//             }
//             // Finish up the operation and report the caller.
//             const numberOfSuccesses = totalItems - failures.length;
//             debug('Batch destroy success rate is %j', numberOfSuccesses / totalItems);

//             return callback(null, {
//                 count: numberOfSuccesses,
//                 successRate: numberOfSuccesses / totalItems,
//                 failures
//             });
//         };
//         return next();
//     } catch (error) {
//         return callback(error);
//     }
// };

CosmosDB.prototype.destroyAll = async function destroyAll(modelName, where, options, callback) {
    // debug('destroyAll', where, options);
    const self = this;
    const modelDefinition = this.getModelDefinition(modelName);
    const partitionKey = this.partitionKey;
    // const properties = ['c.id'];
    // if (this.partitionKey) {
    //     properties.push(`c.${this.partitionKey}`);
    // }

    try {
        // const querySpec = buildQuerySpecForModel(this.queryOptions, modelDefinition, { where }, properties);
        const querySpec = buildQuerySpecForModel(this.queryOptions, modelDefinition, { where });
        // debug(JSON.stringify(querySpec));
        const container = this.client.database(this.databaseName).container(modelName);
        const output = await container.items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery }).fetchAll();
        // const iterator = this.client.database(this.databaseName).container(model)
        //     .items.query(querySpec, { enableCrossPartitionQuery: this.enableCrossPartitionQuery });
        const { resources: items, headers } = output;
        // debug(output);
        checkRequestCharge(headers, querySpec, this.queryOptions, modelName);
        let totalItems = 0;
        const failures = [];

        // Recursively apply delete operations to all matching documents.
        // Underlying client library makes sure that all documents are
        // fetched. We just travel through the query iterator and execute
        // delete for each document in sequence. Because CosmosDB has no
        // multi-document transactions, we do not stop on individual
        // errors but rather execute the whole query iterator until end.
        // At the end, we report the ratio of how many operations succeeded
        // to caller. So that they can decide what to do.
        for (let index = 0; index < items.length; index++) {
            const item = items[index];
            // debug({item});
            // debug(item.id, partitionKey);
            try {
                await container.item(String(item.id), String(item.id)).delete();
            } catch (error) {
                console.log(error);
                failures.push({error, item});
            }
            ++totalItems;
        }
        // Finish up the operation and report the caller.
        const numberOfSuccesses = totalItems - failures.length;
        // debug({numberOfSuccesses, totalItems})
        debug('Batch destroy success rate is %j', numberOfSuccesses / totalItems);
        return callback(null, {
            count: numberOfSuccesses,
            successRate: numberOfSuccesses / totalItems,
            failures
        });

    } catch (error) {
        return callback(error);
    }
};

CosmosDB.prototype.destroyOne = async function destroyOne(model, document, options, callback) {
    debug('destroyOne', document, options);
    try {
        const partitionKey = this.partitionKey ? document[this.partitionKey] : undefined;
        await this.client.database(this.databaseName).container(model)
            .item(document.id, partitionKey).delete();
        // await this.client.database(this.databaseName).container(this.collectionName)
        //     .item(document.id, partitionKey).delete();

        debug('%j document deleted', document.id);
        return callback(null);
    } catch (error) {
        return callback(error);
    }
};

CosmosDB.prototype.updateAttributes = function updateAttributes(model, id, data, options, cb) {
    // console.log('updateAttributes');
    // CosmosDB has no native support for individual attribute updates.
    // Instead we just update documents with given ID and replace them
    // with newer versions.
    this.update(model, { id }, data, options, cb);
};

CosmosDB.prototype.autoupdate = function autoupdate(models, cb) {
  // console.log('autoupdate');
  const self = this;
  if (!cb && typeof models === 'function') {
    cb = models;
    models = undefined;
  }
  // First argument is a model name
  if (typeof models === 'string') {
    models = [models];
  }

  models = models || Object.keys(this._models);
  return Promise.map(models, async (model) => {
    if (!(model in self._models)) {
        const error = new Error(`Model not found: ${model}`);
        throw error;
    }
    const { container } = await this.client.database(this.databaseName).containers.createIfNotExists({ id: model, partitionKey: '/id', maxThroughput: 1000});
    // console.log(container);
    return container;
  })
  .then((containers) => cb(null, containers))
  .catch((error) => cb(error));
};

// function buildCollectionUri(databaseName, collectionName) {
//     return `dbs/${databaseName}/colls/${collectionName}`;
// }

// function buildDocumentUri(databaseName, collectionName, documentId) {
//     return `dbs/${databaseName}/colls/${collectionName}/docs/${documentId}`;
// }

function dropNonViewProperties(modelDefinition, object) {
    const viewObject = {};
    for (const dbProperty in object) {
        try {
            const viewProperty = translateViewPropertyFromDB(modelDefinition, dbProperty);
            viewObject[viewProperty] = object[dbProperty];
        } catch (error) {
            // Property is dropped because it cannot be translated to view property.
        }
    }
    if (object._etag) {
      viewObject._etag = object._etag;
    }
    return viewObject;
}

function translateDBPropertyFromView(modelDefinition, viewProperty) {
    if (modelDefinition.properties[viewProperty] !== undefined && modelDefinition.properties[viewProperty].cosmosdb !== undefined && modelDefinition.properties[viewProperty].cosmosdb.propertyName !== undefined) {
        return modelDefinition.properties[viewProperty].cosmosdb.propertyName;
    }

    if (modelDefinition.properties[viewProperty] !== undefined) {
        return viewProperty;
    }

    throw new Error(`'${viewProperty}' is not any of available model properties: ${Object.keys(modelDefinition.properties).join(', ')}, or it doesn't have a valid 'cosmosdb.propertyName' configuration.`);
}

function translateViewPropertyFromDB(modelDefinition, dbProperty) {
    for (const property in modelDefinition.properties) {
        if (modelDefinition.properties[property].cosmosdb !== undefined && modelDefinition.properties[property].cosmosdb.propertyName === dbProperty) {
            return property;
        }
    }

    if (modelDefinition.properties[dbProperty] !== undefined) {
        return dbProperty;
    }

    throw new Error(`'${dbProperty}' is not any of available model properties: ${Object.keys(modelDefinition.properties).join(', ')}, or it doesn't have a valid 'cosmosdb.propertyName' configuration.`);
}

function translateDBObjectFromView(modelDefinition, object) {
    const dbObject = {};
    for (const viewProperty in object) {
        const dbProperty = translateDBPropertyFromView(modelDefinition, viewProperty);
        dbObject[dbProperty] = object[viewProperty];
    }

    return dbObject;
}

function isCaseInsensitive(modelDefinition, property) {
    return modelDefinition.properties[property] !== undefined && modelDefinition.properties[property].cosmosdb !== undefined && Boolean(modelDefinition.properties[property].cosmosdb.caseInsensitive);
}

function buildWhereClauses(modelDefinition, params, where) {
    return Object.keys(where).map(x => {
        const normalizedKey = x.toUpperCase().trim();
        // Build a top-level logical operator.
        if (['AND', 'OR'].indexOf(normalizedKey) >= 0) {
            // All sub-level logical operators are AND if nothing else is specified.
            const logicalClause = where[x]
                .map(y => buildWhereClauses(modelDefinition, params, y))
                .filter(y => Boolean(y))
                .join(` ${normalizedKey} `);
            if (logicalClause.length > 0) {
                return `(${logicalClause})`;
            }

            // No logical clause.
            return undefined;
        }

        const dbProperty = translateDBPropertyFromView(modelDefinition, x);
        // Use CONTAINS() function to build LIKE operator since CosmosDB does not
        // support LIKEs.
        if (where[x].like !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].like));

            return `CONTAINS(${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))}, @_${params.length.toString()})`;
        }

        // Use CONTAINS() function to build NOT LIKE operator since CosmosDB does not
        // support LIKEs.
        if (where[x].nlike !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].nlike));

            return `NOT CONTAINS(${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))}, @_${params.length.toString()})`;
        }

        // Use CONTAINS() and LOWER() functions to build case-insensitive LIKE
        // operator since CosmosDB does not support case-insensitive LIKEs.
        if (where[x].ilike !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, (where[x].ilike || '').toLowerCase()));

            return `CONTAINS(LOWER(${escapeColumn('c', dbProperty)}), @_${params.length})`;
        }

        // Use CONTAINS() and LOWER() functions to build case-insensitive NOT LIKE
        // operator since CosmosDB does not support case-insensitive LIKEs.
        if (where[x].nilike !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, (where[x].nilike || '').toLowerCase()));

            return `NOT CONTAINS(LOWER(${escapeColumn('c', dbProperty)}), @_${params.length})`;
        }

        // Build greater than operator.
        if (where[x].gt !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].gt));

            return `${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} > @_${params.length.toString()}`;
        }

        // Build greater than or equal operator.
        if (where[x].gte !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].gte));

            return `${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} >= @_${params.length.toString()}`;
        }

        // Build less than operator.
        if (where[x].lt !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].lt));

            return `${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} < @_${params.length.toString()}`;
        }

        // Build less than or equal operator.
        if (where[x].lte !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].lte));

            return `${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} <= @_${params.length.toString()}`;
        }

        // Build IN operator.
        if (where[x].inq !== undefined) {
            const positions = [];
            for (let i = 0; i < where[x].inq.length; ++i) {
                params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].inq[i]));
                positions.push(params.length);
            }

            const inParams = positions.map(i => `@_${i}`);
            return `${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} IN (${inParams.join(',')})`;
        }

        // Build NOT IN operator.
        if (where[x].nin !== undefined) {
            const positions = [];
            for (let i = 0; i < where[x].nin.length; ++i) {
                params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].nin[i]));
                positions.push(params.length);
            }

            const inParams = positions.map(i => `@_${i}`);
            return `${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} NOT IN (${inParams.join(',')})`;
        }

        // Build non-equality operator.
        if (where[x].neq !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].neq));

            return `${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} <> @_${params.length.toString()}`;
        }

        // Build BETWEEN operator.
        if (where[x].between !== undefined) {
            if (where[x].between.length !== 2) {
                throw new Error(`'between' operator has incorrect number of parameters. It should have exactly 2, but has ${where[x].between.length}.`);
            }

            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].between[0]));
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].between[1]));

            return `(${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} BETWEEN @_${params.length - 1} AND @_${params.length.toString()})`;
        }

        // Use ARRAY_CONTAINS to build any operator.
        if (where[x].any !== undefined) {
            params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x].any));

            return `ARRAY_CONTAINS(${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))}, @_${params.length.toString()})`;
        }

        // By default, assume equality operator.
        params.push(decorateQueryParameter(modelDefinition, dbProperty, where[x]));

        return `${decoreateDBQueryValue(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} = @_${params.length.toString()}`;
    }).filter(x => x !== undefined)

      // All clauses are AND if nothing else is specified.
      .join(' AND ');
}

function decoreateDBQueryValue(modelDefinition, dbProperty, value) {
    if (isCaseInsensitive(modelDefinition, dbProperty)) {
        return `LOWER(${value})`;
    }

    return value;
}

function decorateQueryParameter(modelDefinition, dbProperty, value) {
    if (isCaseInsensitive(modelDefinition, dbProperty)) {
        return value.toLowerCase();
    }

    return value;
}

function escapeColumn(row, dbProperty) {
    return `${row}["${dbProperty}"]`;
}

function buildQuerySpecForModel(queryOptions, modelDefinition, filter, select, orderBy) {
    filter = filter || {};

    const modelProperties = Object.keys(modelDefinition.properties);
    const querySelect = select || modelProperties.map(x => escapeColumn('c', translateDBPropertyFromView(modelDefinition, x)));

    // Build ordering if it is set in filters.
    let queryOrderBy = orderBy || (queryOptions.disableDefaultOrdering ? [] : ['c._ts']);
    if (filter.order) {
        filter.order = Array.isArray(filter.order) ? filter.order : [filter.order];

        queryOrderBy = (filter.order || []).map(x => {
            const order = x.split(' ', 2);
            const dbProperty = translateDBPropertyFromView(modelDefinition, order[0]);
            // Normalize order by type if given.
            if (order.length > 1) {
                order[1] = order[1].toUpperCase().trim();
            } else {
              // Set default order by type.
                order.push('ASC');
            }

            if (['ASC', 'DESC'].indexOf(order[1]) < 0) {
                throw new Error(`Order by '${order[1]}' is not allowed for the field '${order[0]}'.`);
            }

            return `${escapeColumn('c', dbProperty)} ${order[1]}`;
        });
    }

    const queryParams = [];
    const queryWhere = buildWhereClauses(modelDefinition, queryParams, filter.where || {});

    const querySpec = {
        query: [
            `SELECT${isFinite(filter.limit) ? ` TOP ${filter.limit}` : ''} ${querySelect.join(',')} FROM c`,
            queryWhere && `WHERE ${queryWhere}`,
            queryOrderBy.length && `ORDER BY ${queryOrderBy.join(',')}`,
        ].filter(part => part).join(' '),
        parameters: queryParams.map((x, i) => ({
            name: `@_${i + 1}`,
            value: x === true ? 1 : x,
        }))
    };
    // debug(`SQL: ${querySpec.query}, params: ${JSON.stringify(querySpec.parameters)}`);

    return querySpec;
}

function checkRequestCharge(headers, querySpec, queryOptions, collectionName) {
  // console.log(headers);
    if (!queryOptions.chargeWarningThreshold || !headers) {
        return;
    }

    const charge = headers['x-ms-request-charge'];
    if (charge > queryOptions.chargeWarningThreshold) {
        debug([
            `WARN: Query charge exceeded configured threshold: ${charge} RU`,
            ` -> Collection: ${collectionName}`,
            ` -> Query: ${textEllipsis(querySpec.query, 100)}`
        ].join('\n'));
    }
}

function textEllipsis(str, maxLength, { side = "end", ellipsis = "..." } = {}) {
  if (str.length > maxLength) {
    switch (side) {
      case "start":
        return ellipsis + str.slice(-(maxLength - ellipsis.length));
      case "end":
      default:
        return str.slice(0, maxLength - ellipsis.length) + ellipsis;
    }
  }
  return str;
}
