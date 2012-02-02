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
    "endpoint/:endpoint": "manageEndpoint", // #endpoint/ep1
    "pair":               "createPair",     // #pair
    "pair/:pair":         "managePair"      // #pair/p1
  },

  index: function() {
    this.updateEditor(null);
  },

  createEndpoint: function() {
    var newEndpoint = new Diffa.Models.Endpoint();
    this.updateEditor(function() { return new Diffa.Views.EndpointEditor({model: newEndpoint, collection: Diffa.EndpointsCollection}) });
  },
  createPair: function() {
    var newPair = new Diffa.Models.Pair();
    this.updateEditor(function() { return new Diffa.Views.PairEditor({model: newPair, collection: Diffa.PairsCollection}) });
  },

  manageEndpoint: function(endpointName) {
    var endpoint = Diffa.EndpointsCollection.get(endpointName);
    this.updateEditor(function() { return new Diffa.Views.EndpointEditor({model: endpoint, collection: Diffa.EndpointsCollection}) });
  },

  managePair: function(pairName) {
    var pair = Diffa.PairsCollection.get(pairName);
    this.updateEditor(function() { return new Diffa.Views.PairEditor({model: pair, collection: Diffa.PairsCollection}) });
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
  idAttribute: 'name',
  initialize: function() {
    _.bindAll(this, 'updateCategories');
    this.bind('change:categories', this.updateCategories);

    this.rangeCategories = new Diffa.Collections.CategoryCollection([], {categoryType: 'range'});
    this.setCategories = new Diffa.Collections.CategoryCollection([], {categoryType: 'set'});
    this.prefixCategories = new Diffa.Collections.CategoryCollection([], {categoryType: 'prefix'});

    this.updateCategories();
  },
  urlRoot: function() { return API_BASE + "/" + Diffa.currentDomain + "/config/endpoints"; },
  prepareForSave: function() {
    var categories = {};

    this.rangeCategories.pack(categories);
    this.setCategories.pack(categories);
    this.prefixCategories.pack(categories);

    // Pack the categories back into their own fields
    this.set({categories: categories}, {silent: true});
  },
  updateCategories: function() {
    this.rangeCategories.unpack(this.get('categories'));
    this.setCategories.unpack(this.get('categories'));
    this.prefixCategories.unpack(this.get('categories'));
  }
});
Diffa.Models.Pair = Backbone.Model.extend({
  idAttribute: "key",
  urlRoot: function() { return API_BASE + "/" + Diffa.currentDomain + "/config/pairs"; },
  prepareForSave: function() {
      // Remove properties artifacts from the databinding library
    this.unset('versionPolicyName_text', {silent: true});
    this.unset('upstreamName_text', {silent: true});
    this.unset('downstreamName_text', {silent: true});
  }
});

Diffa.Collections.Endpoints = Backbone.Collection.extend({
  model: Diffa.Models.Endpoint,
  url: function() { return API_BASE + "/" + Diffa.currentDomain + "/config/endpoints"; },
  comparator: function(endpoint) { return endpoint.get('name'); }
});
Diffa.Collections.Pairs = Backbone.Collection.extend({
  model: Diffa.Models.Pair,
  url: function() { return API_BASE + "/" + Diffa.currentDomain + "/config/pairs"; },
  comparator: function(endpoint) { return endpoint.get('name'); }
});
Diffa.Collections.CategoryCollection = Backbone.Collection.extend({
  model: Backbone.Model,
  initialize: function(models, options) {
    Diffa.Collections.CategoryCollection.__super__.initialize.call(this, arguments);
    this.categoryType = options.categoryType;
  },
  pack: function(target) {
    var self = this;

    this.each(function(cat) {
      target[cat.get('name')] = $.extend({}, cat.attributes, {'@type': self.categoryType});
      delete target[cat.get('name')].name;
    });
  },
  unpack: function(source) {
    var self = this;

    this.reset([]);
    _.each(source, function(value, name) {
      if (value['@type'] == self.categoryType) {
        var attrs = $.extend({}, value, {name: name});
        delete attrs['@type']

        self.add(new self.model(attrs));
      }
    });
  }
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

    this.model.bind("change:" + this.model.idField, this.render);
    this.model.bind("destroy", this.remove);
  },

  render: function() {
    $(this.el).html(
      '<a href="#' + this.options.elementType + '/' + this.model.id + '">' +
        this.model.id +
      '</a>');

    return this.el;
  },

  flash: function() {
    $('a', this.el).css('background-color', '#ffff99').animate({'background-color': '#FFFFFF'});
  }
});
Diffa.Views.FormEditor = Backbone.View.extend({
  events: {
    "click .save": 'saveChanges',
    "click .delete": 'deleteObject'
  },

  initialize: function() {
    _.bindAll(this, 'render', 'close');

    this.model.bind('fetch', this.render);

    this.render();
  },

  render: function() {
    this.preBind();

    $('input[data-key]', this.el).val('');    // Clear the contents of all bound fields
    Backbone.ModelBinding.bind(this, {all: "data-key"});

    var nameContainer = $('.name-container', this.el);

    $('input', nameContainer).toggle(this.model.isNew());
    $('span', nameContainer).toggle(!this.model.isNew());

    $(this.el).show();
  },

  // Callback function to be implemented by subclasses that need to add field values before binding.
  preBind: function() {},
  postClose: function() {},

  close: function() {
    $(this.el).hide();

    this.undelegateEvents();
    Backbone.ModelBinding.unbind(this);

    this.postClose();
  },

  saveChanges: function() {
    var self = this;

    this.model.prepareForSave();
    this.model.save({}, {
      success: function() {
        if (!self.collection.get(self.model.id)) {
          self.collection.add(self.model);
          Diffa.SettingsApp.navigate(self.elementType + "/" + self.model.id, {replace: true, trigger: true});
        }
      }
    });
  },

  deleteObject: function() {
    this.model.destroy({
      success: function() {
        Diffa.SettingsApp.navigate("", {trigger: true});
      }
    });
  }
});
Diffa.Views.EndpointEditor = Diffa.Views.FormEditor.extend({
  el: $('#endpoint-editor'),
  elementType: "endpoint",

  preBind: function() {
    // Attach categories
    this.categoryEditors = [
      new Diffa.Views.CategoriesEditor({collection: this.model.rangeCategories, el: this.$('.range-categories')}),
      new Diffa.Views.CategoriesEditor({collection: this.model.prefixCategories, el: this.$('.prefix-categories')})
    ];
  },
  postClose: function() {
    if (this.categoryEditors) _.each(this.categoryEditors, function(editor) { editor.close(); });
  }
});
Diffa.Views.PairEditor = Diffa.Views.FormEditor.extend({
  el: $('#pair-editor'),
  elementType: "pair",

  preBind: function() {
    var selections = this.$('select.endpoint-selection');

    selections.empty();
    Diffa.EndpointsCollection.each(function(ep) {
      selections.append('<option value="' + ep.get('name') + '">' + ep.get('name') + '</option>');
    });
  }
});

Diffa.Views.CategoriesEditor = Backbone.View.extend({
  events: {
    "click .add-category-link": "createCategory"
  },

  initialize: function() {
    _.bindAll(this, "addOne", "render");
    this.collection.bind("add", this.addOne);
    this.collection.bind("reset", this.render);

    this.render();
  },

  render: function() {
    // Remove all category rows
    this.$('table tr.category-row').remove();

    this.collection.each(this.addOne);
  },

  addOne: function(added) {
    // Retrieve the field names
    var keys = this.$('table thead td').map(function(idx, el) { return $(el).data('key'); });

    // Generate a row, with values for each of the header cells
    var row = $('<tr class="category-row"></tr>');
    _.each(keys, function(k) { row.append('<td><input type="text" data-el-key="' + k + '"></td>'); });
    this.$('table').append(row);

    // Bind the model to the row
    var rowView = new Diffa.Views.CategoryEditor({el: row, model: added});
  },

  createCategory: function(e) {
    e.preventDefault();

    this.collection.add(new this.collection.model({name: 'Untitled'}));
  },

  close: function() {
    this.undelegateEvents();
    
    this.collection.unbind("add", this.addOne);
    this.collection.unbind("reset", this.render);
  }
});
Diffa.Views.CategoryEditor = Backbone.View.extend({
  initialize: function() {
    Backbone.ModelBinding.bind(this, {all: "data-el-key"});
  }
});

Diffa.currentDomain = "diffa";    // TODO: Allow user to change this
Diffa.SettingsApp = new Diffa.Routers.Config();
Diffa.EndpointsCollection = new Diffa.Collections.Endpoints();
Diffa.PairsCollection = new Diffa.Collections.Pairs();
Diffa.EndpointElementListView = new Diffa.Views.ElementList({
  el: $('#endpoints-list'),
  collection: Diffa.EndpointsCollection,
  elementType: 'endpoint'
});
Diffa.EndpointElementListView = new Diffa.Views.ElementList({
  el: $('#pairs-list'),
  collection: Diffa.PairsCollection,
  elementType: 'pair'
});

var preloadCollections = function(colls, callback) {
  var remaining = colls.length;
  _.each(colls, function(preload) {
    preload.fetch({
      success: function() {
        remaining -= 1;

        if (remaining == 0) {
          callback();
        }
      }
    })
  });
};

// Preload useful collections, and then start processing history
preloadCollections([Diffa.EndpointsCollection, Diffa.PairsCollection], function() {
  Backbone.history.start();
});
});