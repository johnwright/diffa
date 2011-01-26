/*
 * Copyright (C) 2010-2011 LShift Ltd.
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

function partEl(id, lastUpdated, body) {
  return {'id':id, 'lastUpdated':lastUpdated.toString("yyyy-MM-ddTHH:mm:ss"), 'body':body };
}

var now = new Date();
var scenario1 = {
  'upstream': [
    partEl('id1', now, 'Lorem ipsum dolor sit amet'),

    partEl('idU1', now.add(-3).hours(), 'Mauris vel dolor ante'),
    partEl('idU2', now.add(-4).hours(), 'Aenean vehicula quam a velit fermentum')

  ],
  'downstream': [
    partEl('id1', now, 'Suspendisse blandit libero'),

    partEl('idD1', now.add(-1).hours(),'velit sollicitudin ut adipiscing metus imperdiet. Duis'),
    partEl('idD2', now.add(-1).days(),'consequat sit amet')
  ]
};

var scenarios = {
  '1':scenario1
};