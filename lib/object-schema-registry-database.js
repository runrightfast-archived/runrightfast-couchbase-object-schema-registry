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
	var log = logging.getLogger('object-schema-registry-database');
	var lodash = require('lodash');
	var Hoek = require('hoek');
	var assert = Hoek.assert;
	var Couchbase = require('couchbase');
	var ObjectSchema = require('runrightfast-validator').validatorDomain.ObjectSchema;
	var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId;
	var when = require('when');

	var ObjectSchemaRegistryDatabase = function(options) {
		var validateOptions = function() {
			assert(lodash.isObject(options), 'options is required');
			assert(lodash.isObject(options.couchbaseConn), 'options.couchbaseConn is required');
		};

		var logOptions = function() {
			var logLevel = options.logLevel || 'WARN';
			logging.setLogLevel(log, logLevel);
			if (log.isDebugEnabled()) {
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
	 * @return Promise that returns the created ObjectSchema and cas - as an
	 *         object with 'schema' and 'cas' properties. If an Error occurs, it
	 *         will have an error code - defined by errorCodes keys
	 */
	ObjectSchemaRegistryDatabase.prototype.createObjectSchema = function(objectSchema) {
		assert(lodash.isObject(objectSchema), 'objectSchema is required');
		var self = this;
		return when.promise(function(resolve, reject) {
			var newSchema;
			try {
				newSchema = new ObjectSchema(objectSchema);
			} catch (err) {
				err.code = 'INVALID_OBJ_SCHEMA';
				reject(err);
				return;
			}

			self.cb.add(objectSchema.id, objectSchema, undefined, function(error, result) {
				if (error) {
					log.error('createObjectSchema() : ' + error);
					if (error.code === Couchbase.errors.keyNotFound) {
						error.code = 'DUP_NS_VER';
					}
					reject(error);
				} else {
					if (log.isDebugEnabled) {
						log.debug('createObjectSchema() : ' + JSON.stringify(result));
					}
					resolve({
						schema : newSchema,
						cas : result.cas
					});
				}
			});
		});
	};

	/**
	 * 
	 * @param namespace
	 *            REQUIRED
	 * @param version
	 *            REQUIRED
	 * @return Promise that returns the ObjectSchema and cas if found - as an
	 *         object with 'schema' and 'cas' properties. If the ObjectSchema
	 *         does not exist, then an Error with code NOT_FOUND is returned
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchema = function(namespace, version) {
		var self = this;
		var id = objectSchemaId(namespace, version);
		return when.promise(function(resolve, reject) {
			self.cb.get(id, function(error, result) {
				if (error) {
					if (error.code === Couchbase.errors.keyNotFound) {
						error.code = 'NOT_FOUND';
					} else {
						log.error('getCredentials() : ' + error);
					}

					reject(error);
				} else {
					if (log.isDebugEnabled) {
						log.debug('getCredentials() : ' + JSON.stringify(result));
					}
					resolve({
						schema : new ObjectSchema(result.value),
						cas : result.cas
					});
				}
			});
		});
	};

	/**
	 * 
	 * @param objectSchema
	 * @param cas
	 *            REQUIRED used to ensure that no one else has updated the
	 *            schema since it was retrieved
	 * @return Promise
	 */
	ObjectSchemaRegistryDatabase.prototype.setObjectSchema = function(objectSchema, cas) {
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
	 * @param namespace
	 * @param version
	 * @return Promise
	 */
	ObjectSchemaRegistryDatabase.prototype.deleteObjectSchema = function(namespace, version) {
		var self = this;
		var id = objectSchemaId(namespace, version);
		return when.promise(function(resolve, reject) {
			self.cb.remove(id, undefined, function(error, result) {
				if (error) {
					log.error('deleteObjectSchema() : ' + error);
					reject(error);
				} else {
					if (log.isLevelEnabled('DEBUG')) {
						log.debug('deleteCredentials() : ' + JSON.stringify(result));
					}
					resolve();
				}
			});
		});

	};

	/**
	 * 
	 * 
	 * @param ids
	 *            REQUIRED - map of namespace -> version
	 * @return Promise where the result is the Couchbase cas for each deleted
	 *         document
	 */
	ObjectSchemaRegistryDatabase.prototype.deleteMultiObjectSchema = function(ids) {
		assert(lodash.isObject(ids), 'ids is required');
		var self = this;
		var idsToDelete = lodash.foldl(lodash.keys(ids), function(result, namespace) {
			result[objectSchemaId(namespace, ids[namespace])] = null;
			return result;
		}, {});

		return when.promise(function(resolve, reject) {
			if (log.isDebugEnabled()) {
				log.debug('idsToDelete : ' + JSON.stringify(lodash.keys(idsToDelete)));
			}
			if (lodash.keys(idsToDelete).length === 0) {
				resolve();
				return;
			}
			self.cb.removeMulti(idsToDelete, undefined, function(error, result) {
				if (error) {
					log.error('deleteObjectSchema() : ' + error);
					reject(error);
				} else {
					if (log.isDebugEnabled) {
						log.debug('deleteCredentials() : ' + JSON.stringify(result));
					}
					resolve();
				}
			});
		});

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
		UNEXPECTED_ERR : 'Unexpected error.',
		NOT_FOUND : 'Not found'
	};

	module.exports = ObjectSchemaRegistryDatabase;
}());
