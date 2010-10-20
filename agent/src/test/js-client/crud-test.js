/*
 * Copyright (C) 2010 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

$(document).ready(function() {	
	var _handle_errors_globally = true,
		CFG_URL = 'http://localhost:19093/diffa-agent/rest/config/';

	function with_no_error_handling(x) {
		_handle_errors_globally = false;
		x();
		_handle_errors_globally = true;
	}
	
	function deepEqualFunc(expected) {
		return function(result) {
			deepEqual(result, expected);
		};
	}

	$.ajaxSetup({
		async: false
	});
	
	$(document).ajaxError(function(event, xhr, settings, ex) {
		if (_handle_errors_globally) {
			ok(false, "AJAX error while running tests! " + xhr.status + ": " + xhr.responseText);
		}
	});
	
	test("crud_all", function() {
		var group_def = {
				key: "TEST_group"
			},
			upstream_def = {
				name: "TEST_upstream",
				url: "http://127.0.0.1/1",
				online: false
			},
			downstream_def = {
				name: "TEST_downstream",
				url: "http://127.0.0.1/2",
				online: false
			},
			pair_def = {
				pairKey: "TEST_pair",
				versionPolicyName: "same",
				matchingTimeout: 0,
				upstreamName: upstream_def.name,
				downstreamName: downstream_def.name,
				groupKey: group_def.key
			},
			expected_pair_def = {
				key: pair_def.pairKey,
				group: group_def,
				matchingTimeout: pair_def.matchingTimeout,
				versionPolicyName: pair_def.versionPolicyName,
				downstream: downstream_def,
				upstream: upstream_def
			},
			not_exists_handler = {
				error: function(xhr, statusText, error) {
					equal(xhr.status, 404);
				}
			};
		
		// Test creating a group
		FD.create(CFG_URL, 'groups', group_def);
		FD.get(CFG_URL, 'groups', group_def.key, deepEqualFunc(group_def));
		
		// Test creating endpoints
		FD.create(CFG_URL, 'endpoints', upstream_def);
		FD.create(CFG_URL, 'endpoints', downstream_def);
		FD.get(CFG_URL, 'endpoints', upstream_def.name, deepEqualFunc(upstream_def));
		FD.get(CFG_URL, 'endpoints', downstream_def.name, deepEqualFunc(downstream_def));
		
		// Test creating a pair
//		FD.create(CFG_URL, 'pairs', pair_def);
//		FD.get(CFG_URL, 'pairs', pair_def.pairKey, deepEqualFunc(expected_pair_def));
//
//		// Delete the test group and check its and its pairs' nonexistence
//		FD.delete(CFG_URL, 'groups', group_def.key);
//		with_no_error_handling(function() {
//			FD.get(
//				CFG_URL,
//				'groups',
//				group_def.key,
//				function() {
//					ok(false, "Deleted group shouldn't exist.");
//				},
//				not_exists_handler
//			);
//			FD.get(
//				CFG_URL,
//				'pairs',
//				pair_def.pairKey,
//				function() {
//					ok(false, "Pair shouldn't exist after group deletion.");
//				},
//				not_exists_handler
//			);
//		});
	
		// Delete endpoints
		FD.delete(CFG_URL, 'endpoints', upstream_def.name);
		FD.delete(CFG_URL, 'endpoints', downstream_def.name);
		stop();
		setTimeout(function() {
			start();
			with_no_error_handling(function() {
				FD.get(
					CFG_URL,
					'endpoints',
					upstream_def.name,
					function() {
						ok(false, "Deleted upstream endpoint shouldn't exist.");
					},
					not_exists_handler
				);
				FD.get(
					CFG_URL,
					'endpoints',
					downstream_def.name,
					function() {
						ok(false, "Deleted downstream endpoint shouldn't exist.");
					},
					not_exists_handler
				);
			});
		}, 0);                                      
	});
}); // $(document).ready(...)
