#!/bin/sh
#
# Copyright (C) 2010 LShift Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


bin/declare.sh -agent http://localhost:19093/diffa-agent -pairKey WEB-2 -pairGroup mygroup -upstreamName a -downstreamName b -versionPolicy same -upstreamUrl http://localhost:19293/diffa-participants/p/upstream -downstreamUrl http://localhost:19293/diffa-participants/p/downstream -matchTimeout 5
