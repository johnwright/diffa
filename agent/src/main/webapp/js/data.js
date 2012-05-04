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
    'submit form': 'uploadInventory',
  },
  setListColumnThreshold: 10,
  templates: {
    rangeConstraint: _.template('<div class="category" data-constraint="<%= name %>" data-constraint-type="<%= dataType %>">' +
                      '<h5 class="name"><%= name %> (<%= dataType %> range)</h5>' +
                      '<div>' +
                        '<label for="<%= name %>_range_start">Start:</label>' +
                        '<span class="clearable_input">' +
                          '<input id="<%= name %>_range_start" type="text" name="start" placeholder="<%= placeholder %>">' +
                          '<span class="clear"></span>' +
                        '</span>' +
                      '</div>' +
                      '<div>' +
                        '<label for="<%= name %>_range_end">End:</label>' +
                        '<span class="clearable_input">' +
                          '<input id="<%= name %>_range_end" type="text" name="end" placeholder="<%= placeholder %>">' +
                          '<span class="clear"></span>' +
                        '</span>' +
                      '</div>' +
                     '</div>'),
    prefixConstraint: _.template('<div class="category" data-constraint="<%= name %>">' +
                        '<h5 class="name"><%= name %> (prefix)</h5>' +
                        '<span class="clearable_input">' +
                          '<input type="text" name="prefix">' +
                          '<span class="clear"></span>' +
                        '</span>' +
                      '</div>'),
    setConstraint: _.template('<div class="category" data-constraint="<%= name %>">' +
                        '<h5 class="name"><%= name %> (set)</h5>' +
                        '<% _.each(values, function(value) { %>' +
                          '<input type="checkbox" value="<%= value %>" id="constraint_<%= name %>_<%= value %>">' +
                          '<label for="constraint_<%= name %>_<%= value %>"><%= value %></label>' +
                          '<br>' +
                        '<% }); %>' +
                      '</div>'),
    setConstraintWithColumns: _.template('<div class="category" data-constraint="<%= name %>">' +
                        '<h5 class="name"><%= name %> (set)</h5>' +
                        '<div class="column_1">' +
                        '<% _.each(valuesFirst, function(value) { %>' +
                          '<input type="checkbox" value="<%= value %>" id="constraint_<%= name %>_<%= value %>">' +
                          '<label for="constraint_<%= name %>_<%= value %>"><%= value %></label>' +
                          '<br>' +
                        '<% }); %>' +
                        '</div>' +
                        '<div class="column_2">' +
                        '<% _.each(valuesSecond, function(value) { %>' +
                          '<input type="checkbox" value="<%= value %>" id="constraint_<%= name %>_<%= value %>">' +
                          '<label for="constraint_<%= name %>_<%= value %>"><%= value %></label>' +
                          '<br>' +
                        '<% }); %>' +
                        '</div>' +
                      '</div>')
  },

  initialize: function() {
    var self = this;

    _.bindAll(this, "render", "onEndpointListUpdate");

    this.el.view = this;

    this.model.bind('reset', this.render);
    this.model.bind('remove', this.onEndpointListUpdate);

    var inventoryUploadTemplate = window.JST['data/inventory-upload'];

    var name = this.model.get("name");

    if ($(this.el).data("pair-half")) {
      name = name + " (" + $(this.el).data("pair-half") + ")";
    }

    $(this.el).append(inventoryUploadTemplate({name: name }));
    this.delegateEvents(this.events);

    this.endpoint = this.model;

    this.render();
    this.model.fetch();
  },

  render: function() {
    this.onEndpointListUpdate();

    return this;
  },

  onEndpointListUpdate: function() {
    this.loadEndpoint();    // Trigger an endpoint selection to ensure the attributes are shown
  },

  loadEndpoint: function(e) {
    var self = this;

    var selectedEndpoint = this.model;

    if (selectedEndpoint && this.currentEndpoint && selectedEndpoint.id == this.currentEndpoint.id) return;

    this.currentEndpoint = selectedEndpoint;

    var constraints = this.$('.constraints');
    constraints.empty();

    if (!selectedEndpoint) return;

    selectedEndpoint.rangeCategories.each(function(rangeCat) {
      var templateVars = rangeCat.toJSON();
      var type = rangeCat.get("dataType");
      var placeholder = "";

      switch(type) {
        case "date":     placeholder = "Unbounded date";     break;
        case "datetime": placeholder = "Unbounded datetime"; break;
        case "int":      placeholder = "Unbounded";     break;
      }

      templateVars["placeholder"] = placeholder;

      constraints.append(self.templates.rangeConstraint(templateVars));

      // add date picking to the range inputs

      var lowerBound = rangeCat.get("lower");
      var upperBound = rangeCat.get("upper");

      if (lowerBound) {
        var allowOld = false;
        var startDate = new Date(lowerBound);
      } else {
        var allowOld = true;
        var startDate = new Date();
      }

      if (upperBound) {
        var endDate = new Date(upperBound);
      } else {
        var endDate = -1;
      }

      // if the category we're adding is a date range, apply datepickers
      if (type == "date" || type == "datetime") {
        $(".category:last-child input[type=text]").glDatePicker({
          allowOld: allowOld, // as far back as possible or not
          startDate: startDate,
          endDate: endDate, // latest selectable date, days since start date (int), or no limit (-1)
          selectedDate: -1, // default select date, or nothing set (-1)
          onChange: function(target, newDate) {
            var result, year, month, day;

            // helper to pad our month/day values if needed
            var pad = function(s) { s = s.toString(); if (s.length < 2) { s = "0" + s; }; return s };
            year = newDate.getFullYear();
            month = pad(newDate.getMonth() + 1);
            day = pad(newDate.getDate());
            result = year + "-" + month + "-" + day;

            if (type == "datetime") {
              result = result + " 00:00:00";
            }

            target.val(result);
            target.change(); // fire the event as though we just typed it in
          }
        });
      }
    });

    selectedEndpoint.setCategories.each(function(setCat) {
      var values = setCat.get("values");
      var threshold = self.setListColumnThreshold; // only split the values array when its size is > this number
      var template = self.templates.setConstraint;

      if (values.length >= threshold) {
        var first, second;

        // cut the list of items into two, ensuring X = [A, B] has |A| >= |B|
        var splitPoint = Math.ceil(values.length/2);
        first = values.slice(0, splitPoint);
        second = values.slice(splitPoint);

        setCat.set("valuesFirst", first);
        setCat.set("valuesSecond", second);

        template = self.templates.setConstraintWithColumns;
      }

      constraints.append(template(setCat.toJSON()));
    });

    selectedEndpoint.prefixCategories.each(function(prefixCat) {
      constraints.append(self.templates.prefixConstraint(prefixCat.toJSON()));
    });

    $(".category input").bind("change keydown focus blur", function() {
      if ($(this).val().length > 0) {
        $(this).addClass("nonempty");
      } else {
        $(this).removeClass("nonempty");
      }
    });

    $(".clearable_input .clear").click(function() {
      $(this).siblings("input").val("");
      $(this).siblings(".nonempty").removeClass("nonempty");
    });
  },

  calculateConstraints: function(e) {
    var result = [];
    var constraints = this.$('.constraints');
    var findContainer = function(name) { return constraints.find('[data-constraint=' + name + ']'); };

    var addConstraint = function(key, value) {
      result.push(encodeURIComponent(key) + "=" + encodeURIComponent(value));
    };

    this.currentEndpoint.rangeCategories.each(function(rangeCat) {
      var start = findContainer(rangeCat.get('name')).find('input[name=start]').val();
      var end = findContainer(rangeCat.get('name')).find('input[name=end]').val();

      if (start.length > 0) addConstraint(rangeCat.get('name') + "-start", start);
      if (end.length > 0) addConstraint(rangeCat.get('name') + "-end", end);
    });
    this.currentEndpoint.setCategories.each(function(setCat) {
      var selected = findContainer(setCat.get('name')).
        find('input[type=checkbox]:checked').
        each(function(idx, c) {
          addConstraint(setCat.get('name'), $(c).val());
        });
    });
    this.currentEndpoint.prefixCategories.each(function(prefixCat) {
      var prefix = findContainer(prefixCat.get('name')).find('input[name=prefix]').val();

      if (prefix.length > 0) addConstraint(prefixCat.get('name') + "-prefix", prefix);
    });

    return result.join('&');
  },

  uploadInventory: function(e) {
    e.preventDefault();

    var endpoint = this.endpoint;
    var inventoryFiles = this.$('input[type=file]')[0].files;
    if (inventoryFiles.length < 1) {
      alert("No inventory files selected for upload");
      return false;
    }
    var inventoryFile = inventoryFiles[0];

    var constraints = this.calculateConstraints();
    
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
    endpoint.uploadInventory(inventoryFile, constraints, {
      global: false,        // Don't invoke global event handlers - we'll deal with errors here locally
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


(function() {

var pairSelectTemplate = _.template('<label>Pair:</label>' +
  '<select class="pair" data-placeholder="Select pair">' +
    '<option value="" selected="selected">Select pair</option>' +
  '<% _.each(pairs.models, function(pair) { %>' +
    '<option value="<%= pair.get("key") %>"><%= pair.get("key") %></option>' +
  '<% }); %> ' +
  '</select>');

var endpointSelectTemplate = _.template('<label>Endpoint:</label>' +
  '<select class="endpoint" data-placeholder="Select endpoint">' +
    '<option value="" selected="selected">Select endpoint</option>' +
  '<% _.each(endpoints.models, function(endpoint) { %>' +
    '<option value="<%= endpoint.get("name") %>"><%= endpoint.get("name") %></option>' +
  '<% }); %>' +
  '</select>');

var panel = $('.inventory-panel');
var domain = Diffa.DomainManager.get(panel.data("domain"));

var emptyAndUnbindUploaders = function() {
  $(".diffa-inventory-uploader").each(function(i, e) {
    var v = this.view;

    // if there's an associated view, make sure we remove
    // its events so that forms which are used on the same
    // DOM element don't fire more than one inventory
    // upload event from the backbone view.
    if (v) {
      v.unbind();
      $(e).unbind();
    }

    $(e).empty();
  });
};

var pairChanged = function(pairName) {
  panel.find("select.endpoint").val("");

  emptyAndUnbindUploaders();

  // abort if there's nothing to do
  if (pairName.length == 0) { return; }

  var pair = domain.pairs.get(pairName);
  var downstream = domain.endpoints.get(pair.get("downstreamName"));
  var upstream   = domain.endpoints.get(pair.get("upstreamName"));

  $('#inventory-uploader-upstream, #inventory-uploader-downstream').each(function() {
    var pairHalf = $(this).data("pair-half");

    if (pairHalf == "upstream") {
      var m = upstream;
    } else if (pairHalf == "downstream") {
      var m = downstream;
    } else {
      throw 'Unknown half of pair "' + pairHalf + '", expected either "upstream" or "downstream"';
    }

    new Diffa.Views.InventoryUploader({
      el: $(this),
      model: m
    });
  });
};

var endpointChanged = function(endpointName) {
  $("select.pair").val("");

  emptyAndUnbindUploaders();

  // abort if there's nothing to do
  if (endpointName.length == 0) { return; }

  var endpoint = domain.endpoints.get(endpointName);

  $('#inventory-uploader').each(function() {
    var domain = Diffa.DomainManager.get($(this).data('domain'));

    new Diffa.Views.InventoryUploader({
      el: $(this),
      model: endpoint
    });
  });
}


domain.loadAll(["endpoints", "pairs"], function() {
  panel.find("#inventory-selection").prepend(endpointSelectTemplate({endpoints: domain.endpoints, domain: domain}));
  panel.find("#inventory-selection").prepend(pairSelectTemplate({pairs: domain.pairs, domain: domain}));
  panel.find("select.pair").change(function() { pairChanged(panel.find("select.pair option:selected").val()); });
  panel.find("select.endpoint").change(function() { endpointChanged(panel.find("select.endpoint option:selected").val()); });

  $("#inventory-selection select").each(function(i, el) {
    // remove the empty-value <option> which is fine without Select2, but
    // with Select2 it causes problems because it's not a real choice which
    // should be listed in the Select2 dropdown; Select2 gives us what we need
    // with a placeholder.
    $(el).find("option:first-child").replaceWith("<option></option>");
    $(el).css("width", "20em");
    $(el).select2({
      allowClear: true
    });
  });
});

})();

});