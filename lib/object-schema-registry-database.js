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
	var joi = require('joi');
	var couchbaseViews = require('./couchbase-views');

	var validateObjectSchemaIdsArray = function validateObjectSchemaIdsArray(ids) {
		var schema = {
			ids : joi.types.Array().required().includes(joi.types.String())
		};
		var validationError = joi.validate({
			ids : ids
		}, schema);
		if (validationError) {
			throw validationError;
		}
	};

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
					if (error.code === Couchbase.errors.keyAlreadyExists) {
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
						log.error('getObjectSchema() : ' + error);
					}

					reject(error);
				} else {
					if (log.isDebugEnabled) {
						log.debug('getObjectSchema() : ' + JSON.stringify(result, undefined, 2));
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
	 * The updatedOn will be set to the current time
	 * 
	 * @param objectSchema
	 * @param cas
	 *            REQUIRED used to ensure that no one else has updated the
	 *            schema since it was retrieved
	 * @param updatedBy
	 *            OPTIONAL
	 * @return Promise
	 */
	ObjectSchemaRegistryDatabase.prototype.updateObjectSchema = function(objectSchema, cas, updatedBy) {
		assert(lodash.isObject(objectSchema), 'objectSchema is required');
		assert(lodash.isObject(cas), 'cas is required');
		if (!lodash.isUndefined(updatedBy)) {
			assert(lodash.isString(updatedBy), 'updatedBy must be a String');
		}
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

			newSchema.updated(updatedBy);

			var replaceOptions = {
				cas : cas
			};

			self.cb.replace(objectSchema.id, objectSchema, replaceOptions, function(error, result) {
				if (error) {
					if (error.code === Couchbase.errors.keyAlreadyExists) {
						error.code = 'STALE_OBJ';
						if (log.isInfoEnabled()) {
							log.info('updateObjectSchema() : ' + error);
						}
					} else {
						log.error('updateObjectSchema() : ' + error);
					}
					reject(error);
				} else {
					if (log.isDebugEnabled) {
						log.debug('createObjectSchema() : ' + JSON.stringify(result, undefined, 2));
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
	 * @param ids
	 *            REQUIRED - Array of object schema ids. An ObjectSchema id can
	 *            be created using :
	 * 
	 * <code>
	 * var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId
	 * var id = objectSchemaId(namespace,version);
	 * </code>
	 * @return Promise that returns an Array of ObjectSchemas that were found.
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemas = function(ids) {
		validateObjectSchemaIdsArray(ids);
		var self = this;

		return when.promise(function(resolve, reject) {
			self.cb.getMulti(ids, null, function(error, result) {
				if (error) {
					var errors = lodash.foldl(lodash.keys(result), function(errorsNot_KeyNotFound, id) {
						if (result[id].error && result[id].error.code !== Couchbase.errors.keyNotFound) {
							errorsNot_KeyNotFound.push(result[id]);
						}
						return errorsNot_KeyNotFound;
					}, []);

					if (errors.length > 0) {
						log.error('deleteObjectSchema() : ' + error);
						reject({
							error : error,
							result : result
						});
					} else {
						resolve(result);
					}
				} else {
					resolve(result);
				}
			});
		});

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
					if (error.code === Couchbase.errors.keyNotFound) {
						resolve();
					} else {
						log.error('deleteObjectSchema() : ' + error);
						reject(error);
					}
				} else {
					if (log.isLevelEnabled('DEBUG')) {
						log.debug('deleteObjectSchema() : ' + JSON.stringify(result));
					}
					resolve(result);
				}
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
		validateObjectSchemaIdsArray(ids);
		var self = this;

		return when.promise(function(resolve, reject) {
			if (log.isDebugEnabled()) {
				log.debug('deleteObjectSchemas() : ids = ' + JSON.stringify(lodash.keys(ids)));
			}
			if (lodash.keys(ids).length === 0) {
				resolve();
				return;
			}
			self.cb.removeMulti(ids, undefined, function(error, result) {
				if (error) {
					var errors = lodash.foldl(lodash.keys(result), function(errorsNot_KeyNotFound, id) {
						if (result[id].error && result[id].error.code !== Couchbase.errors.keyNotFound) {
							errorsNot_KeyNotFound.push(result[id]);
						}
						return errorsNot_KeyNotFound;
					}, []);

					if (errors.length > 0) {
						log.error('deleteObjectSchema() : ' + error);
						reject({
							error : error,
							result : result
						});
					} else {
						resolve(result);
					}
				} else {
					if (log.isDebugEnabled) {
						log.debug('deleteObjectSchemas() : ' + JSON.stringify(result, undefined, 2));
					}
					resolve(result);
				}
			});
		});

	};

	/**
	 * 
	 * @param namespace
	 * @return Promise that returns the existing versions for the specified
	 *         ObjectSchema namespace - as an Array of strings
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemaVersions = function(namespace) {
		assert(lodash.isString(namespace), 'namespace is required');
		var q = this.cb.view('namespace_version', 'namespace_version', {
			stale : false,
			startkey : [ namespace ],
			endkey : [ namespace + '/' ],
			inclusive_end : true,
			reduce : false
		});
		return when.promise(function(resolve, reject) {
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
		var q = this.cb.view('namespace_version', 'namespace_version', {
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
	 *            where params is an object with the following properties:
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
		NOT_FOUND : 'Not found',
		STALE_OBJ : 'ObjectSchema is stale - an newer version is available'
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
	ObjectSchemaRegistryDatabase.prototype.checkDesignDocs = function(create) {
		var self = this;
		var results = {};
		var errors = {};
		var getDesignDocsPromises = [];
		var setDesignDocsPromises = [];
		var createDesignDocsPromises = [];
		return when.promise(function(resolve, reject) {
			lodash.forEach(couchbaseViews, function(view) {
				if (log.isDebugEnabled()) {
					log.debug('checkDesignDocs() : view :\n' + JSON.stringify(view, undefined, 2));
				}

				getDesignDocsPromises.push(when.promise(function(resolve, reject) {
					self.cb.getDesignDoc(view.name, function(err, data) {
						if (err) {
							if (create) {
								var designDoc = {
									views : {}
								};

								designDoc.views[view.name] = {
									map : view.map.toString()
								};
								if (view.reduce) {
									designDoc.views[view.name].reduce = view.reduce.toString();
								}

								if (log.isDebugEnabled()) {
									log.debug('creating design doc:\n' + JSON.stringify(designDoc, undefined, 2));
								}

								setDesignDocsPromises.push(when.promise(function(resolve, reject) {
									self.cb.setDesignDoc(view.name, designDoc, function(err, data) {
										if (err) {
											errors[view.name] = err;
										} else {
											createDesignDocsPromises.push(when.promise(function(resolve, reject) {
												self.cb.getDesignDoc(view.name, function(err, data) {
													if (err) {
														errors[view.name] = err;
													} else {
														results[view.name] = data;
													}
													resolve();
												});
											}));
										}
										resolve();
									});
								}));

							} else {
								errors[view.name] = err;
							}
						} else {
							results[view.name] = data;
						}
						resolve();
					});
				}));

			});

			when.all(getDesignDocsPromises).then(function(result) {
				when.all(setDesignDocsPromises).then(function(result) {
					when.all(createDesignDocsPromises).then(function(result) {
						resolve({
							designDocs : results,
							errors : errors
						});
					}, function(error) {
						reject(error);
					});
				}, function(error) {
					reject(error);
				});

			}, function(error) {
				reject(error);
			});

		});

	};

	module.exports = ObjectSchemaRegistryDatabase;
}());
