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

Diffa.Views.Rings = Backbone.View.extend(Diffa.Helpers.Viz).extend({
  initialize: function() {
    var self = this;

    _.bindAll(this, 'update');

    // Subscribe to aggregates for the various rings
    this.model.subscribeAggregate('currentHour', function() {
      return {startTime: self.nearestMinute().addHours(-1)};
    });
    this.model.subscribeAggregate('previousHour', function() {
      return {startTime: self.nearestMinute().addHours(-2), endTime: self.nearestMinute().addHours(-1)};
    });
    this.model.subscribeAggregate('before', function() {
      return {endTime: self.nearestMinute().addHours(-2)};
    });

    // Render the basic widget
    this.setupWidget();

    // Listen for changes to the various properties
    this.model.on('change:currentHour',   this.update);
    this.model.on('change:previousHour',  this.update);
    this.model.on('change:before',        this.update);

    // Request that the aggregates periodically update
    this.model.watch(this.el);
  },

  nearestMinute: function() {
    var now = new Date();
    var advance = (now.getSeconds() > 0) ? 1 : 0;

    return Date.today().add({hours: now.getHours(), minutes: now.getMinutes() + advance});
  },

  setupWidget: function() {
    $(this.el).html(JST['rings/ring']());
    this.resize();
  },

  resize: function() {
    this.layer = this.$('canvas.content')[0];
    this.width = $(this.el).innerWidth();
    
    this.layer.width = this.width;
    this.layer.height = this.width;

    this.context = this.layer.getContext("2d");
  },

  update: function() {
    var singleValue = function(l) { return (l || [0])[0]; };

    var currentHour = singleValue(this.model.get('currentHour'));
    var previousHour = singleValue(this.model.get('previousHour'));
    var before = singleValue(this.model.get('before'));

    var currentSize = this.limit(this.transformBucketSize(currentHour + previousHour + before, 20), 20);
    var previousSize = this.limit(this.transformBucketSize(previousHour + before, 20), 20);
    var beforeSize = this.limit(this.transformBucketSize(before, 20), 20);

    this.drawCircle(currentSize.value, "#ff1f0d");
    this.drawCircle(previousSize.value, "#ffba40");
    this.drawCircle(beforeSize.value, "#f4fb3f");
  },

  drawCircle: function(value, colour) {
    this.context.lineWidth = 2;
    this.context.strokeStyle = colour;
    this.context.fillStyle = colour;
    this.context.beginPath();
    this.context.arc(this.width / 2, this.width / 2, value, 0, Math.PI * 2, false);
    this.context.closePath();
    this.context.fill();
  }
});

Diffa.Views.RingSet = Backbone.View.extend({
  initialize: function() {
    _.bindAll(this, "addPair", "addAllPairs");

    this.model.pairs.bind("add", this.addPair);
    this.model.pairs.bind("reset", this.addAllPairs);
    this.addAllPairs();
  },

  addAllPairs: function() {
    this.model.pairs.each(this.addPair);
  },

  addPair: function(pair) {
    var pairEl = $('<div class="diffa-ringset"></div>').appendTo($(this.el));
    new Diffa.Views.Rings({el: pairEl, model: this.model.aggregates.forPair(pair.id)});
  }
});

$('.diffa-rings').each(function() {
  var domain = Diffa.DomainManager.get($(this).data('domain'));
  var pairKey = $(this).data('pair');

  new Diffa.Views.Rings({el: $(this), model: domain.aggregates.forPair(pairKey)});
});

$('.diffa-ringset').each(function() {
  var domain = Diffa.DomainManager.get($(this).data('domain'));

  new Diffa.Views.RingSet({el: $(this), model: domain});
  
  domain.loadAll(['pairs'], function() {});
});


});