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

describe('database', function() {
	var database = null;

	var idsToDelete = {};

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

	after(function(done) {
		database.deleteMultiObjectSchema(idsToDelete).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			cbConnManager.stop(function() {
				cbConnManager.clear();
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
		idsToDelete[schema.namespace] = schema.version;
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

});