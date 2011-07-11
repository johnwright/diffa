
/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may   obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var gLastStates = [];

var Diffa = {
  Routers: {},
  Views: {},
  Collections: {},
  Models: {}
};

$(function() {

Diffa.Routers.Pairs = Backbone.Router.extend({
  routes: {
    "pair/:pair":        "managePair"  // #pair/WEB-1
  },

  managePair: function(pairKey) {
    Diffa.PairListView.select(pairKey);

    managePair(pairKey);
  }
});

Diffa.Models.Pair = Backbone.Model.extend({
  initialize: function() {
    _.bindAll(this, "remove");
  },

  remove: function() {
    this.trigger('remove');
  }
});

Diffa.Collections.Pairs = Backbone.Collection.extend({
  model: Diffa.Models.Pair,
  url: API_BASE + "/diffs/sessions/all_scan_states",

  initialize: function() {
    _.bindAll(this, "sync", "scanAll");
  },

  sync: function() {
    var self = this;

    $.getJSON(this.url, function(states) {
      var toRemove = [];

      // Update any pairs we've already got
      self.each(function(currentPair) {
        var newState = states[currentPair.id];

        if (newState) {
          // We're updating an existing pair
          currentPair.set({state: newState});
        } else {
          toRemove.push(currentPair);
        }
      });

      // Remove removable pairs
      _.each(toRemove, function(r) { self.remove(r); });

      // Add any pairs we haven't seen
      for (var key in states) {
        if (!self.get(key)) {
          self.add([{id: key, state: states[key]}]);
        }
      }
    });
  },

  scanAll: function() {
    var self = this;

    this.each(function(pair) {
      pair.set({state: 'REQUESTING'});
    });

    $.ajax({
      url: API_BASE + "/diffs/sessions/scan_all",
      type: "POST",
      success: function() {
        self.each(function(pair) {
          pair.set({state: 'SYNCHRONIZING'});
        });
      },
      error: function(jqXHR, textStatus, errorThrown) {
        alert("Error in scan request: " + errorThrown);
      }
    });
  }
});

Diffa.Views.PairList = Backbone.View.extend({
  el: $('#pair-list'),

  initialize: function() {
    _.bindAll(this, "addPair", "removePair", "select");

    this.model.bind('add', this.addPair);
    this.model.bind('remove', this.removePair);
  },

  select: function(pairKey) {
    this.selectedPair = pairKey;
    this.model.each(function(pair) {
      pair.set({selected: pair.id == pairKey});
    });
  },

  addPair: function(pair) {
    pair.set({selected: pair.id == this.selectedPair});

    var view = new Diffa.Views.PairSelector({model: pair});
    this.el.append(view.render().el);
  },

  removePair: function(pair) {
    pair.remove();
  }
});

Diffa.Views.PairSelector = Backbone.View.extend({
  tagName: "div",
  className: "pair",
  template: _.template($('#pair-selector-template').html()),

  events: {
    "click":  "select"
  },

  initialize: function() {
    _.bindAll(this, "render", "select", "close");

    this.model.bind('change', this.render);
    this.model.bind('remove', this.close);
  },

  render: function() {
    $(this.el).html(this.template({name: this.model.id, state: this.renderState(this.model.get('state'))}));
    $(this.el).attr('pair-key', this.model.id);
    $(this.el).toggleClass('selected-pair', this.model.get('selected'));
    return this;
  },

  close: function() {
    $(this.el).remove();
  },

  select: function() {
    Diffa.SettingsApp.navigate("pair/" + this.model.id, true);
  },

  renderState: function(state) {
    // TODO: When #216 changes SYNCHRONIZING to SCANNING, this should be updated
    switch (state) {
      case "REQUESTING":    return "Requesting Scan";
      case "UNKNOWN":       return "Scan not run";
      case "FAILED":        return "Last Scan Failed";
      case "UP_TO_DATE":    return "Up to Date";
      case "SYNCHRONIZING": return "Scan In Progress";
    }

    return null;
  }
});

});

function renderPairScopedActions(pairKey, actionListContainer, repairStatus) {
  var actionListCallback = function(actionList, status, xhr) {
    if (!actionList) return;

    $.each(actionList, function(i, action) {
      appendActionButtonToContainer(actionListContainer, action, pairKey, null, repairStatus);
    });
  };

  $.ajax({ url: API_BASE + '/actions/' + pairKey + '?scope=pair', success: actionListCallback });
}

function removeAllPairs() {
  $('#pairs').find('div').remove();
}

function addPair(name, state) {
  var view = new Diffa.Views.PairSelector({model: new Diffa.Models.Pair({name: name, state: state})});
  $("#pairs").append(view.render().el);
}

function managePair(name) {
  if ($('#pair-log').is(':visible')) {
    $('#pair-log').smartupdaterStop();
  } else {
    $('#pair-log').show();
    $('#pair-actions').show();
  }

  $('#pair-log').smartupdater({
      url : API_BASE + "/diagnostics/" + name + "/log",
      dataType: "json",
      minTimeout: 2000
    }, function(logEntries) {
      $('#pair-log div').remove();

      if (logEntries.length == 0) {
        $('#pair-log').append('<div class="status">No recent activity</div>');
      } else {
        $.each(logEntries, function(idx, entry) {
          var time = new Date(entry.timestamp).toString("dd/MM/yyyy HH:mm:ss");
          var text = '<span class="timestamp">' + time + '</span><span class="msg">' + entry.msg + '</span>';

          $('#pair-log').append('<div class="entry entry-' + entry.level.toLowerCase() + '">' + text + '</div>')
        })
      }
    });

  $('#pair-actions div,button').remove();
  renderPairScopedActions(name, $('#pair-actions'), null);
}

$(document).ready(function() {
  $('#scan_all').click(function(e) {
    Diffa.PairsCollection.scanAll();
  });

  Diffa.SettingsApp = new Diffa.Routers.Pairs();
  Diffa.PairsCollection = new Diffa.Collections.Pairs();
  Diffa.PairListView = new Diffa.Views.PairList({model: Diffa.PairsCollection});
  Backbone.history.start();

  Diffa.PairsCollection.sync();
  setInterval('Diffa.PairsCollection.sync()',5000);
});
