/**
Copyright (c) yellowcar ltd 2010

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or other
materials provided with the distribution.

Neither the name of yellowcar ltd nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 'AS IS' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
DAMAGE.

**/

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