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

var Diffa = {
  Routers: {},
  Views: {},
  Collections: {},
  Models: {},
  Config: {}
};

$(function() {

Diffa.Routers.Config = Backbone.Router.extend({
  routes: {
    "":                   "index",          // #
    "endpoint":           "createEndpoint", // #endpoint
    "endpoint/:endpoint": "manageEndpoint"  // #endpoint/ep1
  },

  index: function() {
    this.updateEditor(null);
  },

  createEndpoint: function() {
    var newEndpoint = new Diffa.Models.Endpoint({name: 'untitled'});
    this.updateEditor(function() { return new Diffa.Views.EndpointEditor({model: newEndpoint}) });
  },

  manageEndpoint: function(endpointName) {
    var endpoint = Diffa.EndpointsCollection.get(endpointName);
    if (endpoint == null) {
      endpoint = new Diffa.Models.Endpoint({id: endpointName, name: endpointName});
      endpoint.fetch();
    }

    this.updateEditor(function() { return new Diffa.Views.EndpointEditor({model: endpoint}) });
  },

  updateEditor: function(newEditorBuilder) {
    if (this.currentEditor) {
      this.currentEditor.close();
    }
    if (newEditorBuilder) {
      this.currentEditor = newEditorBuilder();
    } else {
      this.currentEditor = null;
    }
  }
});

Diffa.Models.Endpoint = Backbone.Model.extend({
  url: function() {
    if (this.isNew()) {
      return API_BASE + "/" + Diffa.currentDomain + "/config/endpoints";
    } else {
      return API_BASE + "/" + Diffa.currentDomain + "/config/endpoints/" + this.get('name');
    }
  },
  parse: function(response) {
    // If we've just been created on the server, then no response will be returned. Simulate a response containing
    // the new id (which just matches the name).
    if (this.isNew() && response == null) {
      return {id: this.get('name')};
    }

    // Alias the name as the id of the object
    response.id = response.name;
    return response;
  }
});

Diffa.Collections.Endpoints = Backbone.Collection.extend({
  model: Diffa.Models.Endpoint,
  url: function() { return API_BASE + "/" + Diffa.currentDomain + "/config/endpoints"; },
  comparator: function(endpoint) { return endpoint.get('name'); }
});

Diffa.Views.ElementList = Backbone.View.extend({
  initialize: function() {
    var self = this;

    _.bindAll(this, "render", "addOne", "updateNoneMessage");

    this.collection.bind('reset', this.render);
    this.collection.bind('add', function(m) { self.addOne(m, false); });
    this.collection.bind('remove', this.updateNoneMessage);
  },

  render: function() {
    var self = this;

    this.collection.each(function(m) {
      self.addOne(m, true);
    });

    this.updateNoneMessage();

    return this;
  },

  addOne: function(m, initialRender) {
    var self = this;
    var newView = new Diffa.Views.ElementListItem({model: m, elementType: self.options.elementType});

    this.$('.element-list').append(newView.render());
    this.updateNoneMessage();

    if (!initialRender) newView.flash();
  },

  updateNoneMessage: function() {
    this.$('.none-message').toggle(this.collection.length == 0);
  }
});
Diffa.Views.ElementListItem = Backbone.View.extend({
  tagName: 'li',

  initialize: function() {
    _.bindAll(this, 'render', 'flash', 'remove');

    this.model.bind("change:name", this.render);
    this.model.bind("destroy", this.remove);
  },

  render: function() {
    $(this.el).html(
      '<a href="#' + this.options.elementType + '/' + this.model.get('name') + '">' +
        this.model.get('name') +
      '</a>');

    return this.el;
  },

  flash: function() {
    $('a', this.el).css('background-color', '#ffff99').animate({'background-color': '#FFFFFF'});
  }
});
Diffa.Views.EndpointEditor = Backbone.View.extend({
  el: $('#endpoint-editor'),

  events: {
    "click .save": 'saveChanges',
    "click .delete": 'deleteEndpoint'
  },

  initialize: function() {
    _.bindAll(this, 'render', 'close');

    this.model.bind('fetch', this.render);

    // We can bind here since the view elements already exist
    $('input[data-key]', this.el).val('');    // Clear the contents of all bound fields
    Backbone.ModelBinding.bind(this, {text: "data-key"});

    this.render();
  },

  render: function() {
    var nameContainer = $('.endpoint-name', this.el);

    $('input', nameContainer).toggle(this.model.isNew());
    $('span', nameContainer).toggle(!this.model.isNew());

    $(this.el).show();
  },

  close: function() {
    $(this.el).hide();

    this.undelegateEvents();
    Backbone.ModelBinding.unbind(this);
  },

  saveChanges: function() {
    var self = this;

    this.model.save({}, {
      success: function() {
        Diffa.EndpointsCollection.add(self.model);
        Diffa.SettingsApp.navigate("endpoint/" + self.model.id, {replace: true, trigger: true});
      }
    });
  },

  deleteEndpoint: function() {
    this.model.destroy({
      success: function() {
        Diffa.SettingsApp.navigate("", {trigger: true});
      }
    });
  }
});

Diffa.currentDomain = "diffa";    // TODO: Allow user to change this
Diffa.SettingsApp = new Diffa.Routers.Config();
Diffa.EndpointsCollection = new Diffa.Collections.Endpoints();
Diffa.EndpointElementListView = new Diffa.Views.ElementList({
  el: $('#endpoints-list'),
  collection: Diffa.EndpointsCollection,
  elementType: 'endpoint'
});
Backbone.history.start();

Diffa.EndpointsCollection.fetch();

});