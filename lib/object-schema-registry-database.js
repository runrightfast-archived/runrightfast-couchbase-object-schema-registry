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

	var lodash = require('lodash');
	var EntityDatabase = require('runrightfast-couchbase').EntityDatabase;
	var ObjectSchema = require('runrightfast-validator').validatorDomain.ObjectSchema;
	var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId;
	var when = require('when');

	var ObjectSchemaRegistryDatabase = function(options) {
		options.entityConstructor = ObjectSchema;
		this.database = new EntityDatabase(options);
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
		return this.database.createEntity(objectSchema);
	};

	/**
	 * 
	 * @param namespace
	 *            REQUIRED
	 * @param version
	 *            REQUIRED
	 * @return Promise that returns the ObjectSchema and cas if found - as an
	 *         object with 'value' and 'cas' properties. If the ObjectSchema
	 *         does not exist, then an Error with code NOT_FOUND is returned
	 * 
	 * returned object has the following properties: <code>
	 * cas			Couchbase CAS
	 * value		ObjectSchema object
	 * </code>
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchema = function(namespace, version) {
		var self = this;
		return when.promise(function(resolve, reject) {
			var id;
			try {
				id = objectSchemaId(namespace, version);
			} catch (err) {
				reject(err);
				return;
			}

			when(self.database.getEntity(id), function(result) {
				resolve(result);
			}, function(error) {
				reject(error);
			});
		});
	};

	/**
	 * The updatedOn will be set to the current time
	 * 
	 * @param objectSchema
	 * @param cas
	 *            REQUIRED used to ensure that no one else has updated the
	 *            schema since it was retrieved
	 * @param updatedBy
	 *            OPTIONAL
	 * @return Promise - If successful, the returned object has the following
	 *         properties:
	 * 
	 * <code>
	 * cas			Couchbase CAS
	 * value		ObjectSchema object
	 * </code>
	 */
	ObjectSchemaRegistryDatabase.prototype.updateObjectSchema = function(objectSchema, cas, updatedBy) {
		return this.database.updateEntity(objectSchema, cas, updatedBy);
	};

	/**
	 * 
	 * @param ids
	 *            REQUIRED - Array of object schema ids. An ObjectSchema id can
	 *            be created using :
	 * 
	 * <code>
	 * var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId
	 * var id = objectSchemaId(namespace,version);
	 * </code>
	 * @return Promise that returns an dictionary of :
	 * 
	 * <code>
	 * objectSchemaId -> {
	 * 						cas 			// Couchbase CAS
	 * 						value			// ObjectShema
	 *  				 }
	 * <code>
	 * ObjectSchemas that were found.
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemas = function(ids) {
		return this.database.getEntities(ids);
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
		return when.promise(function(resolve, reject) {
			var id;
			try {
				id = objectSchemaId(namespace, version);
			} catch (err) {
				reject(err);
				return;
			}
			when(self.database.deleteEntity(id), function(result) {
				resolve(result);
			}, function(error) {
				reject(error);
			});
		});

	};

	/**
	 * 
	 * 
	 * @param ids
	 *            REQUIRED - REQUIRED - Array of object schema ids. An
	 *            ObjectSchema id can be created using :
	 * 
	 * <code>
	 * var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId
	 * var id = objectSchemaId(namespace,version);
	 * </code>
	 * 
	 * @return Promise where the result is the Couchbase cas for each deleted
	 *         document. If an error occurs, then an object containing both the
	 *         error and result is returned in order to inspect what went wrong.
	 */
	ObjectSchemaRegistryDatabase.prototype.deleteObjectSchemas = function(ids) {
		return this.database.deleteEntities(ids);
	};

	/**
	 * 
	 * @param namespace
	 * @return Promise that returns the existing versions for the specified
	 *         ObjectSchema namespace - as an Array of strings
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemaVersions = function(namespace) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(namespace)) {
				reject(new Error('namespace is required'));
				return;
			}

			var q = self.database.cb.view('namespace_version', 'namespace_version', {
				stale : false,
				startkey : [ namespace ],
				endkey : [ namespace + '/' ],
				inclusive_end : true,
				reduce : false
			});

			q.query(function(err, results) {
				if (err) {
					reject(err);
				} else {
					var versions = lodash.map(results, function(result) {
						return result.key[1];
					});
					resolve(versions);
				}
			});
		});

	};

	/**
	 * 
	 * @param namespace
	 * @return Promise that returns a listing of the existing ObjectSchema
	 *         namespaces along with the number of schema versions as an Object
	 *         hash (namespace -> count of versions)
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemaNamespaces = function() {
		var q = this.database.cb.view('namespace_version', 'namespace_version', {
			stale : false,
			group_level : 1,
			reduce : true
		});
		return when.promise(function(resolve, reject) {
			q.query(function(err, results) {
				if (err) {
					reject(err);
				} else {
					var namespaceCounts = lodash.map(results, function(result) {
						var namespaceCount = {};
						namespaceCount[result.key[0]] = result.value;
						return namespaceCount;
					});
					resolve(namespaceCounts);
				}
			});
		});
	};

	/**
	 * delegates to queryByDateField() with params.dateField = 'createdOn'
	 * 
	 * @see queryByDateField
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemasByCreatedOn = function(params) {
		return this.database.getEntitiesByCreatedOn(params);
	};

	/**
	 * delegates to queryByDateField() with params.dateField = 'updatedOn'
	 * 
	 * @see queryByDateField
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemasByUpdatedOn = function(params) {
		return this.database.getEntitiesByUpdatedOn(params);
	};

	/**
	 * Checks if each of the design docs exist.
	 * 
	 * @returns Promise where the result is an object consisting of 2
	 *          properties:
	 * 
	 * <code>
	 * designDocs 		Object - map of design doc name -> design doc
	 * errors			Object - map of design doc name -> Error
	 * </code>
	 */
	ObjectSchemaRegistryDatabase.prototype.checkDesignDocs = function(create, replace) {
		var designDocs = require('./couchbase-views');
		return this.database.checkDesignDocs(create, replace, designDocs);
	};

	module.exports = ObjectSchemaRegistryDatabase;
}());
