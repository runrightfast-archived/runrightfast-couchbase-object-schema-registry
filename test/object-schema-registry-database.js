/**
 * Copyright [2013] [runrightfast.co]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

'use strict';
var expect = require('chai').expect;
var cbConnManager = require('runrightfast-couchbase').couchbaseConnectionManager;
var ObjectSchemaRegistryDatabase = require('..').ObjectSchemaRegistryDatabase;
var ObjectSchema = require('runrightfast-validator').validatorDomain.ObjectSchema;
var when = require('when');
var uuid = require('uuid');
var lodash = require('lodash');

var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId;

describe('database', function() {
	var database = null;

	var idsToDelete = [];

	before(function(done) {

		var options = {
			couchbase : {
				"host" : [ "localhost:8091" ],
				buckets : [ {
					"bucket" : "default"
				} ]
			},
			logLevel : 'DEBUG',
			startCallback : function(result) {
				console.log('before::startCallback');
				console.log(result);

				database = new ObjectSchemaRegistryDatabase({
					couchbaseConn : cbConnManager.getBucketConnection('default'),
					logLevel : 'DEBUG'
				});

				done();
			}
		};

		cbConnManager.registerConnection(options);

	});

	afterEach(function(done) {
		database.deleteObjectSchemas(idsToDelete).then(function(result) {
			idsToDelete = [];
			done();
		}, function(error) {
			console.error(JSON.stringify(error, undefined, 2));
			done(error.error);
		});

	});

	after(function(done) {
		database.deleteObjectSchemas(idsToDelete).then(function(result) {
			cbConnManager.stop(function() {
				cbConnManager.clear();
				idsToDelete = [];
				done();
			});
		}, function(error) {
			done(error);
		});

	});

	it('can create a new ObjectSchema in the database', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema.namespace, schema.version));
		database.createObjectSchema(schema).then(function(result) {
			console.log('(can create a new ObjectSchema in the database) result : ' + JSON.stringify(result, undefined, 2));
			try {
				expect(result.cas).to.exist;
				expect(result.value instanceof ObjectSchema).to.equal(true);
				done();
			} catch (err) {
				done(err);
			}

		}, function(err) {
			done(error);
		});
	});

	it('#createObjectSchema validates ObjectSchema arg', function(done) {
		database.createObjectSchema().then(function(result) {
			done(new Error('expected validation error'));
		}, function(err) {
			console.log(err);
			done();
		});
	});

	it('create will fail if ObjectSchema fails validation', function(done) {
		database.createObjectSchema({}).then(function(result) {
			done(new Error('expected create to fail because of validation error'));
		}, function(err) {
			console.log(err);
			done();
		});
	});

	it('create a new ObjectSchema in the database with the same id will fail', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema.namespace, schema.version));
		database.createObjectSchema(schema).then(function(result) {
			console.log('(can create a new ObjectSchema in the database) result : ' + JSON.stringify(result, undefined, 2));
			try {
				expect(result.cas).to.exist;
				expect(result.value instanceof ObjectSchema).to.equal(true);

				database.createObjectSchema(schema).otherwise(function(error) {
					done();
				});

			} catch (err) {
				done(err);
			}

		}, function(err) {
			done(error);
		});
	});

	it('can get an ObjectSchema from the database', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema.namespace, schema.version));
		database.createObjectSchema(schema).then(function(result) {
			console.log('(can create a new ObjectSchema in the database) result : ' + JSON.stringify(result, undefined, 2));
			try {
				expect(result.cas).to.exist;
				expect(result.value instanceof ObjectSchema).to.equal(true);

				when(database.getObjectSchema(result.value.namespace, result.value.version), function(result) {
					console.log('(getObjectSchema() result : ' + JSON.stringify(result, undefined, 2));
					var retrievedSchema = result.value;
					var retrievedSchemaCAS = result.cas;
					expect(retrievedSchema).to.exist;
					expect(retrievedSchemaCAS).to.exist;
					expect(retrievedSchema.id).to.equal(result.value.id);
					done();
				}, function(err) {
					done(err);
				});

			} catch (err) {
				done(err);
			}

		}, function(err) {
			done(error);
		});
	});

	it('getting an ObjectSchema from the database for an invalid id will return an error', function(done) {
		when(database.getObjectSchema('ns://' + uuid.v4(), '0.0.1'), function(result) {
			done(new Error('expected an error because the doc should not exist'));
		}, function(err) {
			expect(err.code).to.equal('NOT_FOUND');
			done();
		});
	});

	it('#getObjectSchema validates its args', function(done) {
		when(database.getObjectSchema('invalid namespace format', '0.0.1'), function(result) {
			done(new Error('expected validation error'));
		}, function(err) {
			console.log(err);
			done();
		});
	});

	it('#updateObjectSchema validates its args', function(done) {
		when(database.updateObjectSchema(null, null, 'azappala'), function(result) {
			done(new Error('expected validation error'));
		}, function(err) {
			console.log(err);
			done();
		});
	});

	it('#deleteObjectSchema validates its args', function(done) {
		when(database.deleteObjectSchema(), function(result) {
			done(new Error('expected validation error'));
		}, function(err) {
			console.log(err);
			done();
		});
	});

	it('#getObjectSchemaVersions validates its args', function(done) {
		when(database.getObjectSchemaVersions(), function(result) {
			done(new Error('expected validation error'));
		}, function(err) {
			console.log(err);
			done();
		});
	});

	it('can update an ObjectSchema from the database', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema.namespace, schema.version));
		database.createObjectSchema(schema).then(function(result) {
			console.log('(can create a new ObjectSchema in the database) result : ' + JSON.stringify(result, undefined, 2));
			try {
				expect(result.cas).to.exist;
				expect(result.value instanceof ObjectSchema).to.equal(true);

				when(database.getObjectSchema(result.value.namespace, result.value.version), function(result) {
					console.log('(getObjectSchema() result:\n' + JSON.stringify(result, undefined, 2));
					var retrievedSchema = result.value;
					var retrievedSchemaCAS = result.cas;
					expect(retrievedSchema).to.exist;
					expect(retrievedSchemaCAS).to.exist;
					expect(retrievedSchema.id).to.equal(result.value.id);

					result.value.description = 'test : can update an ObjectSchema from the database';
					when(database.updateObjectSchema(result.value, result.cas, 'azappala'), function(result) {
						console.log('(getObjectSchema() result after update:\n' + JSON.stringify(result, undefined, 2));
						var retrievedSchema2 = result.value;
						var retrievedSchemaCAS2 = result.cas;

						try {
							expect(retrievedSchemaCAS).to.not.equal(retrievedSchemaCAS2);
							expect(retrievedSchema2.updatedOn.getTime() > retrievedSchema.updatedOn.getTime()).to.equal(true);
							expect(retrievedSchema2.updatedBy).to.equal('azappala');
							done();
						} catch (err) {
							done(err);
						}
					}, function(err) {
						done(err);
					});

				}, function(err) {
					done(err);
				});

			} catch (err) {
				done(err);
			}

		}, function(err) {
			done(error);
		});
	});

	it('updating will fail for an invalid ObjectSchema', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema.namespace, schema.version));
		database.createObjectSchema(schema).then(function(result) {
			console.log('(can create a new ObjectSchema in the database) result : ' + JSON.stringify(result, undefined, 2));
			try {
				expect(result.cas).to.exist;
				expect(result.value instanceof ObjectSchema).to.equal(true);

				when(database.getObjectSchema(result.value.namespace, result.value.version), function(result) {
					console.log('(getObjectSchema() result:\n' + JSON.stringify(result, undefined, 2));
					var retrievedSchema = result.value;
					var retrievedSchemaCAS = result.cas;
					expect(retrievedSchema).to.exist;
					expect(retrievedSchemaCAS).to.exist;
					expect(retrievedSchema.id).to.equal(result.value.id);

					result.value.description = 'test : can update an ObjectSchema from the database';
					when(database.updateObjectSchema({}, result.cas, 'azappala'), function(result) {
						done(new Error('Expected update to fail'));
					}, function(err) {
						console.log(err);
						done();
					});

				}, function(err) {
					done(err);
				});

			} catch (err) {
				done(err);
			}

		}, function(err) {
			done(error);
		});
	});

	it('updating an ObjectSchema from the database with an expired CAS should fail', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema.namespace, schema.version));
		database.createObjectSchema(schema).then(function(result) {
			console.log('(can create a new ObjectSchema in the database) result : ' + JSON.stringify(result, undefined, 2));
			try {
				expect(result.cas).to.exist;
				expect(result.value instanceof ObjectSchema).to.equal(true);

				when(database.getObjectSchema(result.value.namespace, result.value.version), function(result) {
					console.log('(getObjectSchema() result:\n' + JSON.stringify(result, undefined, 2));
					var retrievedSchema = result.value;
					var retrievedSchemaCAS = result.cas;
					expect(retrievedSchema).to.exist;
					expect(retrievedSchemaCAS).to.exist;
					expect(retrievedSchema.id).to.equal(result.value.id);

					result.value.description = 'test : can update an ObjectSchema from the database';
					return when(database.updateObjectSchema(retrievedSchema, retrievedSchemaCAS, 'azappala'), function(result) {
						console.log('(getObjectSchema() result after update:\n' + JSON.stringify(result, undefined, 2));
						var retrievedSchema2 = result.value;
						var retrievedSchemaCAS2 = result.cas;

						expect(retrievedSchemaCAS).to.not.equal(retrievedSchemaCAS2);
						expect(retrievedSchema2.updatedOn.getTime() > retrievedSchema.updatedOn.getTime()).to.equal(true);
						expect(retrievedSchema2.updatedBy).to.equal('azappala');

						return result;
					}, function(err) {
						return err;
					}).then(function(result) {
						when(database.updateObjectSchema(retrievedSchema, retrievedSchemaCAS, 'azappala'), function(result) {
							done(new Error('Expected an error because the CAS should be expired'));
						}, function(error) {
							console.log(error);
							done();
						});

					}, function(error) {
						done(error);
					});

				}, function(err) {
					done(err);
				});

			} catch (err) {
				done(err);
			}

		}, function(err) {
			done(error);
		});
	});

	it('can delete an ObjectSchema from the database', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema.namespace, schema.version));
		database.createObjectSchema(schema).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));

			expect(result.cas).to.exist;
			expect(result.value instanceof ObjectSchema).to.equal(true);

			var namespace = result.value.namespace;
			var version = result.value.version;

			var removePromise_1 = when(database.deleteObjectSchema(namespace, version), function(result) {
				return result;
			}, function(err) {
				return err;
			});

			var removePromise_2 = when(removePromise_1, function(result) {
				console.log('delete result #1 : ' + JSON.stringify(result, undefined, 2));

				return when(database.deleteObjectSchema(namespace, version), function(result) {
					console.log('delete result2 : ' + JSON.stringify(result, undefined, 2));
					return result;
				}, function(error) {
					return error;
				});
			}, function(error) {
				return error;
			});

			when(removePromise_2, function(result) {
				done();
			}, function(error) {
				done(error);
			});

		}, function(err) {
			done(error);
		});
	});

	it('can retrieve multiple ObjectSchemas from the database at once', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema1 = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema1.namespace, schema1.version));

		options.namespace = 'ns://runrightfast.co/couchbase/config';
		options.version = '1.0.1';
		var schema2 = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema2.namespace, schema2.version));

		var creates = [];

		creates.push(when(database.createObjectSchema(schema1), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			return result;
		}, function(error) {
			return error;
		}));

		creates.push(when(database.createObjectSchema(schema2), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			return result;
		}, function(error) {
			return error;
		}));

		when(when.all(creates), function(results) {
			console.log(JSON.stringify(results, undefined, 2));

			try {
				var ids = lodash.map(results, function(result) {
					return objectSchemaId(result.value.namespace, result.value.version);
				});

				console.log(JSON.stringify(ids, undefined, 2));

				database.getObjectSchemas(ids).then(function(result) {
					console.log(JSON.stringify(result, undefined, 2));
					done();
				}, function(error) {
					done(error);
				});
			} catch (error) {
				done(error);
			}
		}, function(error) {
			done(error);
		});

	});

	it('can retrieve multiple ObjectSchemas from the database at once', function(done) {
		var options = {
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		};

		var schema1 = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema1.namespace, schema1.version));

		options.namespace = 'ns://runrightfast.co/couchbase/config';
		options.version = '1.0.1';
		var schema2 = new ObjectSchema(options);
		idsToDelete.push(objectSchemaId(schema2.namespace, schema2.version));

		var creates = [];

		creates.push(when(database.createObjectSchema(schema1), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			return result;
		}, function(error) {
			return error;
		}));

		creates.push(when(database.createObjectSchema(schema2), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			return result;
		}, function(error) {
			return error;
		}));

		when(when.all(creates), function(results) {
			console.log(JSON.stringify(results, undefined, 2));

			try {
				var ids = lodash.map(results, function(result) {
					return objectSchemaId(result.value.namespace, result.value.version);
				});

				ids.push(objectSchemaId('ns://non-existent', '0.0.0'));

				console.log(JSON.stringify(ids, undefined, 2));

				database.getObjectSchemas(ids).then(function(result) {
					console.log(JSON.stringify(result, undefined, 2));
					expect(lodash.keys(result).length).to.equal(2);
					done();
				}, function(error) {
					done(error);
				});
			} catch (error) {
				done(error);
			}
		}, function(error) {
			done(error);
		});

	});

	it('#getObjectSchemas validates that ids is an Array of Strings', function(done) {
		database.getObjectSchemas([ 1, 2 ]).then(function(result) {
			done(new Error('expected validation error'));
		}, function(error) {
			console.log(error);
			done();
		});

	});

	it('can check if the Design Documents have been defined', function(done) {
		when(database.checkDesignDocs(), function(results) {
			console.log(JSON.stringify(results, undefined, 2));
			done();
		}, function(error) {
			done(error);
		});
	});

	it('can check if the Design Documents have been defined and create them if they do not exist', function(done) {
		when(database.checkDesignDocs(true), function(results) {
			console.log(JSON.stringify(results, undefined, 2));
			done();
		}, function(error) {
			done(error);
		});
	});

	describe('has query functionality', function() {
		var idsToDelete = [];
		var schemas = [];

		before(function(done) {
			this.timeout(1000 * 10);

			var now = Date.now();
			for ( var i = 0; i < 10; i++) {
				schemas.push(new ObjectSchema({
					namespace : 'ns://runrightfast.co/schema1',
					version : '1.0.' + i,
					description : 'Schema #1'
				}));

				schemas.push(new ObjectSchema({
					namespace : 'ns://runrightfast.co/schema2',
					version : '2.0.' + i,
					description : 'Schema #2'
				}));

				schemas.push(new ObjectSchema({
					namespace : 'ns://runrightfast.co/schema3',
					version : i + '.0.' + i,
					description : 'Schema #1',
					createdOn : new Date(now + (1000 * 60 * 60 * 24 * i)),
					updatedOn : new Date(now + (1000 * 60 * 60 * 24 * i))
				}));
			}

			lodash.forEach(schemas, function(schema) {
				idsToDelete.push(objectSchemaId(schema.namespace, schema.version));
			});

			var promises = lodash.map(schemas, function(schema) {
				return when(database.createObjectSchema(schema), function(result) {
					return result;
				}, function(error) {
					return error;
				});
			});

			// ensure design docs have been created
			promises.push(when(database.checkDesignDocs(true, true), function(results) {
				console.log(JSON.stringify(results, undefined, 2));
			}, function(error) {
				done(error);
			}));

			when(when.all(promises), function(results) {
				console.log('All schemas have been created: ' + schemas.length);
				/*
				 * By default, docs get persisted to disk every 5 seconds. Docs
				 * will not be available to query until they have been written
				 * to disk.
				 */
				setTimeout(done, 5200);
			});

		});

		after(function(done) {
			database.deleteObjectSchemas(idsToDelete).then(function(result) {
				idsToDelete = [];
				done();
			}, function(error) {
				console.error(JSON.stringify(error, undefined, 2));
				done(error.error);
			});
		});

		it('#checkDesignDocs validates create arg is a Boolean if specified', function(done) {
			database.checkDesignDocs('', true).then(function(result) {
				done(new Error('expected validation error'));
			}, function(err) {
				console.log(err);
				done();
			});
		});

		it('#checkDesignDocs validates replace arg is a Boolean if specified', function(done) {
			database.checkDesignDocs(true, '').then(function(result) {
				done(new Error('expected validation error'));
			}, function(err) {
				console.log(err);
				done();
			});
		});

		it('can query for namespace versions', function(done) {
			when(database.getObjectSchemaVersions('ns://runrightfast.co/schema1'), function(result) {
				console.log('query results: ' + JSON.stringify(result, undefined, 2));
				try {
					expect(result.length).to.equal(10);

					var expectedVersions = lodash.filter(schemas, function(schema) {
						return schema.namespace === 'ns://runrightfast.co/schema1';
					}).map(function(schema) {
						return schema.version;
					});

					expect(lodash.difference(expectedVersions, result).length).to.equal(0);

					done();
				} catch (err) {
					done(err);
				}
			}, function(error) {
				done(error);
			});

		});

		it('can query for namespaces and version counts', function(done) {
			when(database.getObjectSchemaNamespaces(), function(result) {
				console.log('query results: ' + JSON.stringify(result, undefined, 2));
				done();
			}, function(error) {
				done(error);
			});
		});

		describe('#getObjectSchemasByCreatedOn enables querying on createdOn', function() {

			it('can query by createdOn and retrieves the first 20 records by default', function(done) {
				when(database.getObjectSchemasByCreatedOn(), function(result) {
					console.log('query results: ' + JSON.stringify(result, undefined, 2));
					try {
						expect(result.length).to.equal(20);
						done();
					} catch (err) {
						done(err);
					}

				}, function(error) {
					done(error);
				});
			});

			it('can query by createdOn and retrieves the first 20 ObjectSchemas by default', function(done) {
				when(database.getObjectSchemasByCreatedOn({
					returnDocs : true
				}), function(result) {
					console.log('query results: ' + JSON.stringify(result, undefined, 2));
					try {
						try {
							expect(result.length).to.equal(20);
							result.forEach(function(record) {
								expect(record.value instanceof ObjectSchema).to.equal(true);
							});
							done();
						} catch (err) {
							done(err);
						}
					} catch (err) {
						done(err);
					}

				}, function(error) {
					done(error);
				});
			});

			it('can query by createdOn and filter on a date range', function(done) {
				var now = Date.now();
				var queryOptions = {
					from : new Date(now + (1000 * 60 * 60 * 24)),
					to : new Date(now + (1000 * 60 * 60 * 24 * 5))
				};
				when(database.getObjectSchemasByCreatedOn(queryOptions), function(result) {
					console.log('result.length = ' + result.length);
					console.log('query results: ' + JSON.stringify(result, undefined, 2));
					try {
						expect(result.length).to.equal(4);
						done();
					} catch (err) {
						done(err);
					}

				}, function(error) {
					done(error);
				});
			});

			it('#getObjectSchemasByCreatedOn validates that that date range values are Date objects', function(done) {
				var now = Date.now();
				var queryOptions = {
					from : 'NOT A DATE OBJECT',
					to : {}
				};
				when(database.getObjectSchemasByCreatedOn(queryOptions), function(result) {
					done(new Error('Expected validation error'));
				}, function(error) {
					console.log(error);
					done();
				});
			});

			it('can query by createdOn and page through results using skip', function(done) {
				var now = Date.now();
				var queryOptions = {
					limit : 5,
					skip : 0
				};

				var docCount = 0;

				var nextPage = function(queryOptions) {
					return when(database.getObjectSchemasByCreatedOn(queryOptions), function(result) {
						docCount += result.length;
						console.log('docCount = ' + docCount);
						console.log('result.length = ' + result.length);
						console.log('query results: ' + JSON.stringify(result, undefined, 2));
						return result;
					}, function(error) {
						done(error);
					});
				};

				var pages = [];
				for ( var i = 0; i < 5; i++) {
					queryOptions.skip += 5;
					pages.push(nextPage(queryOptions));
				}

				when(when.all(pages), function(result) {
					done();
				}, function(error) {
					done(error);
				});

			});

			it('can query by createdOn and page through results using skip and startkey_docid', function(done) {
				var now = Date.now();
				var queryOptions = {
					limit : 5,
					skip : 0
				};

				var docCount = 0;

				var nextPage = function(queryOptions) {
					return when(database.getObjectSchemasByCreatedOn(queryOptions), function(result) {
						docCount += result.length;
						console.log('docCount = ' + docCount);
						console.log('result.length = ' + result.length);
						console.log('query results: ' + JSON.stringify(result, undefined, 2));
						return result;
					}, function(error) {
						done(error);
					});
				};

				var firstPage = nextPage(queryOptions);

				when(firstPage, function(result) {
					queryOptions.startDocId = result[result.length - 1].id;
					return nextPage(queryOptions);
				}, function(err) {
					done(err);
				}).then(function(result) {
					done();
				}, function(err) {
					done(err);
				});

			});
		});

		describe('#getObjectSchemasByUpdatedOn enable querying on updatedOn', function() {

			it('#getObjectSchemasByUpdatedOn validates that that date range values are Date objects', function(done) {
				var now = Date.now();
				var queryOptions = {
					from : {},
					to : ''
				};
				when(database.getObjectSchemasByUpdatedOn(queryOptions), function(result) {
					done(new Error('Expected validation error'));
				}, function(error) {
					console.log(error);
					done();
				});
			});

			it('can query by updatedOn and retrieves the first 20 records by default', function(done) {
				when(database.getObjectSchemasByUpdatedOn(), function(result) {
					console.log('query results: ' + JSON.stringify(result, undefined, 2));
					try {
						expect(result.length).to.equal(20);
						done();
					} catch (err) {
						done(err);
					}

				}, function(error) {
					done(error);
				});
			});

			it('can query by updatedOn and retrieves the first 20 ObjectSchemas by default', function(done) {
				when(database.getObjectSchemasByUpdatedOn({
					returnDocs : true
				}), function(result) {
					console.log('query results: ' + JSON.stringify(result, undefined, 2));
					try {
						try {
							expect(result.length).to.equal(20);
							result.forEach(function(record) {
								expect(record.value instanceof ObjectSchema).to.equal(true);
							});
							done();
						} catch (err) {
							done(err);
						}
					} catch (err) {
						done(err);
					}

				}, function(error) {
					done(error);
				});
			});

			it('can query by updatedOn and filter on a date range', function(done) {
				var now = Date.now();
				var queryOptions = {
					from : new Date(now + (1000 * 60 * 60 * 24)),
					to : new Date(now + (1000 * 60 * 60 * 24 * 5))
				};
				when(database.getObjectSchemasByUpdatedOn(queryOptions), function(result) {
					console.log('result.length = ' + result.length);
					console.log('query results: ' + JSON.stringify(result, undefined, 2));
					try {
						expect(result.length).to.equal(4);
						done();
					} catch (err) {
						done(err);
					}

				}, function(error) {
					done(error);
				});
			});

			it('can query by updatedOn and page through results using skip', function(done) {
				var now = Date.now();
				var queryOptions = {
					limit : 5,
					skip : 0
				};

				var docCount = 0;

				var nextPage = function(queryOptions) {
					return when(database.getObjectSchemasByUpdatedOn(queryOptions), function(result) {
						docCount += result.length;
						console.log('docCount = ' + docCount);
						console.log('result.length = ' + result.length);
						console.log('query results: ' + JSON.stringify(result, undefined, 2));
						return result;
					}, function(error) {
						done(error);
					});
				};

				var pages = [];
				for ( var i = 0; i < 5; i++) {
					queryOptions.skip += 5;
					pages.push(nextPage(queryOptions));
				}

				when(when.all(pages), function(result) {
					done();
				}, function(error) {
					done(error);
				});

			});

			it('can query by updatedOn and page through results using skip and startkey_docid', function(done) {
				var now = Date.now();
				var queryOptions = {
					limit : 5,
					skip : 0
				};

				var docCount = 0;

				var nextPage = function(queryOptions) {
					return when(database.getObjectSchemasByUpdatedOn(queryOptions), function(result) {
						docCount += result.length;
						console.log('docCount = ' + docCount);
						console.log('result.length = ' + result.length);
						console.log('query results: ' + JSON.stringify(result, undefined, 2));
						return result;
					}, function(error) {
						done(error);
					});
				};

				var firstPage = nextPage(queryOptions);

				when(firstPage, function(result) {
					queryOptions.startDocId = result[result.length - 1].id;
					return nextPage(queryOptions);
				}, function(err) {
					done(err);
				}).then(function(result) {
					done();
				}, function(err) {
					done(err);
				});

			});
		});

	});

});