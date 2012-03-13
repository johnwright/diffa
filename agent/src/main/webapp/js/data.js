/**
 * Copyright (C) 2012 LShift Ltd.
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

$(function() {

Diffa.Views.InventoryUploader = Backbone.View.extend({
  events: {
    'submit form': 'uploadInventory'
  },

  initialize: function() {
    var self = this;

    _.bindAll(this, "render", "addOne", "updateNoneMessage");

    this.collection.bind('reset', this.render);
    this.collection.bind('add', this.addOne);
    this.collection.bind('remove', this.updateNoneMessage);

    this.selectList = this.$('select[name=endpoint]');

    this.render();
  },

  render: function() {
    var self = this;

    this.selectList.empty();
    this.collection.each(this.addOne);

    this.updateNoneMessage();

    return this;
  },

  addOne: function(e) {
    $('<option value="' + e.id + '">' + e.get('name') + '</option>').appendTo(this.selectList);
    this.updateNoneMessage();
  },

  updateNoneMessage: function() {
  },

  uploadInventory: function(e) {
    e.preventDefault();

    var endpoint = this.collection.get(this.selectList.val());
    var inventoryFiles = this.$('input[type=file]')[0].files;
    if (inventoryFiles.length < 1) {
      alert("No inventory files selected for upload");
      return false;
    }
    var inventoryFile = inventoryFiles[0];

    var submitButton = this.$('input[type=submit]');
    submitButton.attr('disabled', 'disabled');
    var statusPanel = this.$('.status');
    var applyStatus = function(text, clazz) {
      statusPanel.
        removeClass('info success error').
        addClass(clazz).
        text(text).
        show();
    };

    applyStatus('Uploading...', 'info');
    endpoint.uploadInventory(inventoryFile, {
      success: function() {
        submitButton.removeAttr('disabled');
        applyStatus('Inventory Submitted', 'success');
      },
      error: function(jqXHR, textStatus) {
        submitButton.removeAttr('disabled');

        var message = "Unknown Cause";
        if (jqXHR.status == 0) {
          message = textStatus;
        } else if (jqXHR.status == 400) {
          message = jqXHR.responseText;
        } else if (jqXHR.status == 403) {
          message = "Access Denied";
        } else {
          message = "Server Error (" + jxXHR.status + ")";
        }

        applyStatus('Inventory Submission Failed: ' + message, 'error');
      }
    });

    return false;
  }
});

Diffa.currentDomain = currentDiffaDomain;
Diffa.EndpointsCollection = new Diffa.Collections.Endpoints();
Diffa.InventoryUploaderView =  new Diffa.Views.InventoryUploader({
  el: $('#inventory-uploader'),
  collection: Diffa.EndpointsCollection
});

Diffa.EndpointsCollection.fetch();
});
