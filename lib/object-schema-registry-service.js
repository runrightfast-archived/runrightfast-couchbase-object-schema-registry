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

	var when = require('when');

	var ObjectSchemaRegistryService = function(options) {
		var ObjectSchemaRegistryDatabase = require('./object-schema-registry-database');
		this.db = new ObjectSchemaRegistryDatabase(options);
	};

	/**
	 * 
	 * @param params
	 *            an object with the following properties:
	 * 
	 * <code>
	 * namespace			REQUIRED - String - e.g., ns://runrightfast.co/security
	 * version				REQUIRED - String - follows semver semantics, e.g., 1.0.0
	 * type					REQUIRED - String - the type name 
	 * </code>
	 * 
	 * @returns Promise
	 */
	ObjectSchemaRegistryService.prototype.getSchemaType = function(params) {
		return when(this.db.getObjectSchema(params.namespace, params.version), function(result) {
			var objectSchema = result.value;
			var type = objectSchema.getType(params.type);
			if (!type) {
				throw new Error('Type is not defined for : ' + JSON.stringify(params));
			}
		}, function(error) {
			return error;
		});
	};

	module.exports = ObjectSchemaRegistryService;
}());