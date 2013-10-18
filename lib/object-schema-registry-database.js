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

/**
 * <code>
 * options = { 
 * 	 couchbaseConn: conn								// REQUIRED - Couchbase.Connection
 *   logLevel : 'WARN' 									// OPTIONAL - Default is 'WARN'
 * }
 * </code>
 */
(function() {
	'use strict';

	var logging = require('runrightfast-commons').logging;
	var log = logging.getLogger('hawk-credentials-database');
	var lodash = require('lodash');
	var Hoek = require('hoek');
	var assert = Hoek.assert;
	var Couchbase = require('couchbase');

	var ObjectSchemaRegistryDatabase = function(options) {
		var validateOptions = function() {
			assert(lodash.isObject(options), 'options is required');
			assert(lodash.isObject(options.couchbaseConn), 'options.couchbaseConn is required');
		};

		var logOptions = function() {
			var logLevel = options.logLevel || 'WARN';
			logging.setLogLevel(log, logLevel);
			if (log.isLevelEnabled('DEBUG')) {
				log.debug(options);
			}
		};

		validateOptions();
		logOptions();

		this.cb = options.couchbaseConn;
	};

	/**
	 * 
	 * 
	 * @param objectSchema
	 * @return Promise that returns the created ObjectSchema. If an Error
	 *         occurs, it will have an error code - defined by errorCodes keys
	 */
	ObjectSchemaRegistryDatabase.prototype.createObjectSchema = function(objectSchema) {
		// TODO
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * 
	 * @param namespace
	 *            REQUIRED
	 * @param version
	 *            REQUIRED
	 * @return Promise that returns the ObjectSchema if found. If the
	 *         ObjectSchema does not exist, then an Error with code NOT_FOUND is
	 *         returned
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchema = function(namespace, version) {
		// TODO
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * 
	 * @param ids
	 *            REQUIRED - map of namespace -> version
	 * @return Promise that returns an Array of ObjectSchemas that were found.
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemas = function(ids) {
		// TODO
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * 
	 * 
	 * @param objectSchema
	 * @return Promise that returns the deleted ObjectSchema. If the
	 *         ObjectSchema does not exist then nothing is returned, i.e.,
	 *         undefined
	 */
	ObjectSchemaRegistryDatabase.prototype.deleteObjectSchema = function(namespace, version) {
		// TODO
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * 
	 * @param namespace
	 * @return Promise that returns the existing versions for the specified
	 *         ObjectSchema namespace
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemaVersions = function(namespace) {
		// TODO
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * 
	 * @param namespace
	 * @return Promise that returns a listing of the existing ObjectSchema
	 *         namespaces as an Array
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemaNamespaces = function() {
		// TODO
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * @param params
	 *            where params is an objet with the following properties:
	 * 
	 * <code> 
	 * from			REQUIRED - Date - the start date to search from
	 * to           OPTIONAL - Date - defaults to now
	 * page			OPTIONAL - Integer - 0 based page index - defaults to 0
	 * pageSize     OPTIONAL - Integer - defaults to 20
	 * </code>
	 * 
	 * @return Promise that returns the following search results with the
	 *         following properties:
	 * 
	 * <code>
	 * namespace
	 * version
	 * createdOn
	 * </code>
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemasByCreatedOn = function(params) {
		// TODO
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * @param params
	 *            where params is an objet with the following properties:
	 * 
	 * <code> 
	 * from			REQUIRED - Date - the start date to search from
	 * to           OPTIONAL - Date - defaults to now
	 * page			OPTIONAL - Integer - 0 based page index - defaults to 0
	 * pageSize     OPTIONAL - Integer - defaults to 20
	 * </code>
	 * 
	 * @return Promise that returns the following search results with the
	 *         following properties:
	 * 
	 * <code>
	 * namespace
	 * version
	 * updatedOn
	 * </code>
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemasByUpdatedOn = function(params) {
		// TODO
		throw new Error('NOT IMPLEMENTED');
	};

	ObjectSchemaRegistryDatabase.prototype.errorCodes = {
		DUP_NS_VER : 'An ObjectSchema with the same name id (namespace/version) already exists.',
		INVALID_OBJ_SCHEMA : 'The object schema is invalid.',
		CB_ERR : 'Couchbase error',
		UNEXPECTED_ERR : 'Unexpected error.',
		NOT_FOUND : 'Not found'
	};

	module.exports = ObjectSchemaRegistryDatabase;
}());
