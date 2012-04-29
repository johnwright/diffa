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
  colours: {
    current: {          // Orange
      line: '#ff9440',
      fill: '#ffb073'
    },
    previous: {         // Yellow
      line: '#f4fb40',
      fill: '#fafda4'
    },
    before: {           // Blue
      line: '#43adde',
      fill: '#99d3ed'
    }
  },

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

    this.clearCanvas();

    var currentHour = singleValue(this.model.get('currentHour'));
    var previousHour = singleValue(this.model.get('previousHour'));
    var before = singleValue(this.model.get('before'));

    var bucketOpts = {
      minSize: 10,
      maxSize: (this.width / 2) - 5,
      maxValue: 100
    };

    var currentSize = this.transformBucketSize(currentHour + previousHour + before, bucketOpts);
    var previousSize = this.transformBucketSize(previousHour + before, bucketOpts);
    var beforeSize = this.transformBucketSize(before, bucketOpts);

    this.drawCircle(currentSize.value, this.colours.current);
    this.drawCircle(previousSize.value, this.colours.previous);
    this.drawCircle(beforeSize.value, this.colours.before);
  },

  clearCanvas: function() {
    this.layer.width = this.layer.width;
  },

  drawCircle: function(value, colours) {
    this.context.lineWidth = 2;
    this.context.strokeStyle = colours.line;
    this.context.fillStyle = colours.fill;
    this.context.beginPath();
    this.context.arc(this.width / 2, this.width / 2, value, 0, Math.PI * 2, false);
    this.context.closePath();
    this.context.fill();
    this.context.stroke();
  }
});

Diffa.Views.RingSet = Backbone.View.extend({
  template: window.JST['rings/ringset-entry'],

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
    var pairEl = $(this.template({key: pair.id})).appendTo($(this.el));
    new Diffa.Views.Rings({el: $('.diffa-rings', pairEl), model: this.model.aggregates.forPair(pair.id)});
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