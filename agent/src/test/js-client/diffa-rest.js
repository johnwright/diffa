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

// diffa-rest.js
// Wrappers for rest calls

function _rest_call(method, url, data, callback, extra) {
  settings = {
    type: method,
    url: url,
    data: data,
    success: callback,
    contentType: 'application/json',
    dataType: 'json',
    processData: false
  };
  $.extend(settings, extra); // merge in extra settings
  return $.ajax(settings);
}

fd_create = function(url, collection, obj, callback, extra) {
  return _rest_call('POST', url + collection, JSON.stringify(obj), callback, extra);
};

fd_list = function(url, collection, callback, extra) {
  return _rest_call('GET', url + collection, null, callback, extra);
};

fd_get = function(url, collection, key, callback, extra) {
  return _rest_call('GET', url + collection + '/' + key, null, callback, extra);
};

fd_update = function(url, collection, key, obj, callback, extra) {
  return _rest_call('PUT', url + collection + '/' + key, JSON.stringify(obj), callback, extra);
};

fd_delete = function(url, collection, key, callback, extra) {
  return _rest_call('DELETE', url + collection + '/' + key, null, callback, extra);
};