/**
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

var test_data = [{
	"state": "UNMATCHED",
	"downstreamVsn": "version2",
	"upstreamVsn": "version",
	"detectedAt": 1285897102000,
	"objId": {
		"id": "id",
		"pairKey": "WEB-1"
	},
	"seqId": "1"
},
{
	"state": "UNMATCHED",
	"downstreamVsn": null,
	"upstreamVsn": "version2",
	"detectedAt": 1285897002000,
	"objId": {
		"id": "id2",
		"pairKey": "WEB-2"
	},
	"seqId": "2"
},
{
	"state": "UNMATCHED",
	"downstreamVsn": "versionB",
	"upstreamVsn": "version2",
	"detectedAt": 1285896902000,
	"objId": {
		"id": "id2",
		"pairKey": "WEB-1"
	},
	"seqId": "3"
},
{
	"state": "UNMATCHED",
	"downstreamVsn": null,
	"upstreamVsn": "version3",
	"detectedAt": 1285896802000,
	"objId": {
		"id": "id3",
		"pairKey": "WEB-1"
	},
	"seqId": "4"
},
{
	"state": "UNMATCHED",
	"downstreamVsn": "versionC",
	"upstreamVsn": "version3",
	"detectedAt": 1285896702000,
	"objId": {
		"id": "id3",
		"pairKey": "WEB-1"
	},
	"seqId": "5"
}];

var test_action_list = [{
	"name":"Resend Source",
	"method":"POST",
	"id":"resend",
	"type":"rpc",
	"action":"/actions/WEB-1/resend/${id}"
}];