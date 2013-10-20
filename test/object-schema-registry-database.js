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
			console.log(JSON.stringify(result, undefined, 2));
			idsToDelete = [];
			done();
		}, function(error) {
			console.error(JSON.stringify(error, undefined, 2));
			done(error.error);
		});

	});

	after(function(done) {
		database.deleteObjectSchemas(idsToDelete).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
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
				expect(result.schema instanceof ObjectSchema).to.equal(true);
				done();
			} catch (err) {
				done(err);
			}

		}, function(err) {
			done(error);
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
				expect(result.schema instanceof ObjectSchema).to.equal(true);

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
				expect(result.schema instanceof ObjectSchema).to.equal(true);

				when(database.getObjectSchema(result.schema.namespace, result.schema.version), function(result) {
					console.log('(getObjectSchema() result : ' + JSON.stringify(result, undefined, 2));
					var retrievedSchema = result.schema;
					var retrievedSchemaCAS = result.cas;
					expect(retrievedSchema).to.exist;
					expect(retrievedSchemaCAS).to.exist;
					expect(retrievedSchema.id).to.equal(result.schema.id);
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
				expect(result.schema instanceof ObjectSchema).to.equal(true);

				when(database.getObjectSchema(result.schema.namespace, result.schema.version), function(result) {
					console.log('(getObjectSchema() result:\n' + JSON.stringify(result, undefined, 2));
					var retrievedSchema = result.schema;
					var retrievedSchemaCAS = result.cas;
					expect(retrievedSchema).to.exist;
					expect(retrievedSchemaCAS).to.exist;
					expect(retrievedSchema.id).to.equal(result.schema.id);

					result.schema.description = 'test : can update an ObjectSchema from the database';
					when(database.updateObjectSchema(result.schema, result.cas, 'azappala'), function(result) {
						console.log('(getObjectSchema() result after update:\n' + JSON.stringify(result, undefined, 2));
						var retrievedSchema2 = result.schema;
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
				expect(result.schema instanceof ObjectSchema).to.equal(true);

				when(database.getObjectSchema(result.schema.namespace, result.schema.version), function(result) {
					console.log('(getObjectSchema() result:\n' + JSON.stringify(result, undefined, 2));
					var retrievedSchema = result.schema;
					var retrievedSchemaCAS = result.cas;
					expect(retrievedSchema).to.exist;
					expect(retrievedSchemaCAS).to.exist;
					expect(retrievedSchema.id).to.equal(result.schema.id);

					result.schema.description = 'test : can update an ObjectSchema from the database';
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
				expect(result.schema instanceof ObjectSchema).to.equal(true);

				when(database.getObjectSchema(result.schema.namespace, result.schema.version), function(result) {
					console.log('(getObjectSchema() result:\n' + JSON.stringify(result, undefined, 2));
					var retrievedSchema = result.schema;
					var retrievedSchemaCAS = result.cas;
					expect(retrievedSchema).to.exist;
					expect(retrievedSchemaCAS).to.exist;
					expect(retrievedSchema.id).to.equal(result.schema.id);

					result.schema.description = 'test : can update an ObjectSchema from the database';
					return when(database.updateObjectSchema(retrievedSchema, retrievedSchemaCAS, 'azappala'), function(result) {
						console.log('(getObjectSchema() result after update:\n' + JSON.stringify(result, undefined, 2));
						var retrievedSchema2 = result.schema;
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
			expect(result.schema instanceof ObjectSchema).to.equal(true);

			var namespace = result.schema.namespace;
			var version = result.schema.version;

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
					return objectSchemaId(result.schema.namespace, result.schema.version);
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

	it('can check if the Design Documents have been defined', function(done) {
		when(database.checkDesignDocs(), function(results) {
			console.log(JSON.stringify(results, undefined, 2));
			done();
		}, function(error) {
			done(error);
		});
	});

	it.only('can check if the Design Documents have been defined and create them if they do not exist', function(done) {
		when(database.checkDesignDocs(true), function(results) {
			console.log(JSON.stringify(results, undefined, 2));
			done();
		}, function(error) {
			done(error);
		});
	});

});