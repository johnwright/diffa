/**
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

$(function() {

jQuery.fn.multiselect = function() {
  $(this).each(function() {
    var checkboxes = $(this).find("input:checkbox");
    checkboxes.each(function() {
      var checkbox = $(this);
      // Highlight pre-selected checkboxes
      if (checkbox.attr("checked"))
        checkbox.parent().addClass("multiselect-on");

      // Highlight checkboxes that the user selects
      checkbox.click(function() {
        if (checkbox.attr("checked"))
          checkbox.parent().addClass("multiselect-on");
        else
          checkbox.parent().removeClass("multiselect-on");
      });
    });
  });
};

var directions = {
  left: 'left',
  right: 'right'
};

var colours = {
  black: 'black',
  darkGrey: '#555555',
  red: '#d12f19',
  transparent: 'rgba(0,0,0,0)',
  white: 'white',
  translucent: 'rgba(200,200,200,0.4)',
  clearBlack: 'rgba(0,0,0,0.1)'
};

Diffa.Routers.Blobs = Backbone.Router.extend({
  routes: {
    "":                             "index",     // #
    "blobs/:pair/:start-:end":      "viewBlob"   // # blobs/WEB-1/20110801134500/3600/5
  },

  initialize: function(opts) {
    var self = this;
    this.domain = opts.domain;

    opts.el.on('blob:selected', function(event, selectedPair, startTime, endTime) {
      self.navigate("blobs/" + selectedPair + '/' + startTime + '-' + endTime, true);
    });
  },

  index: function() {
  },

  viewBlob: function(pairKey, start, end) {
    // Currently, only the Diff list displays selection. When #320 is done, this will also need to inform the heatmap.
    this.domain.diffs.select(pairKey, start, end);
  }
});

Diffa.Models.HeatmapProjection = Backbone.Model.extend(Diffa.Collections.Watchable).extend({
  watchInterval: 5000,      // How frequently we poll for blob updates
  defaultZoomLevel:4,       // HOURLY
  defaultMaxRows: 1000,       // Will change as more pairs arrive. Correction: No, it won't!!!
  defaultBucketCount: 31,   // Default number of buckets. Will be overriden once heatmap is ready

  initialize: function() {
    _.bindAll(this, "sync");

    this.set({
      zoomLevel: this.defaultZoomLevel,
      bucketSize: this.calculateBucketSize(this.defaultZoomLevel),
      maxRows: this.defaultMaxRows,
      lastEndTime: nearestHour(),
      bucketCount: this.defaultBucketCount
    });
    this.aggregates = this.get('aggregates');   // Pull the aggregates collection out as a top-level attribute
    this.domain = this.get('domain');

    var self = this;
    this.hiddenPairs = this.domain.hiddenPairs;

    var fireBucketChange = function() { self.trigger('change:buckets'); };
    this.aggregates.on('add', fireBucketChange);
    this.aggregates.on('change', fireBucketChange);
    this.hiddenPairs.on('revealPair', fireBucketChange);
    this.hiddenPairs.on('hidePair', fireBucketChange);

    // The two different end time properties should event out as changes to the start time
    this.on('change:fixedEndTime', function() { self.trigger('change:startTime'); });
    this.on('change:lastEndTime', function() { self.trigger('change:startTime'); });
  },

  sync: function() {
    var self = this;

    var endTime = nearestHour();
    if (this.get('fixedEndTime')) {
      endTime = this.get('fixedEndTime');
    }

    var startTime = this.startTimeFromEndTime(endTime);

    this.aggregates.subscribeAggregate('map', {startTime: startTime, endTime: endTime, bucketing: (self.get('bucketSize') / 60)});
    this.aggregates.subscribeAggregate('left', {endTime: startTime});
    this.aggregates.subscribeAggregate('right', {startTime: endTime});

    this.aggregates.sync(function() {
      self.set({'lastEndTime': endTime});
      self.aggregates.change();   // Queue all change events till everything is completed
    }, {silent: true});
  },

  zoomOut: function() {
    this.maybeUpdateZoomLevel(-1);
  },
  zoomIn: function() {
    this.maybeUpdateZoomLevel(1);
  },

  maybeUpdateZoomLevel: function(factor) {
    var newZoomLevel = this.get('zoomLevel') + factor;
    if(this.isZoomLevelValid(newZoomLevel)) {
      this.set({ zoomLevel: newZoomLevel });
      this.set({ bucketSize: this.calculateBucketSize(newZoomLevel) });
      this.sync();
    }
  },

  getBucketSize: function() {
    return this.calculateBucketSize(this.get('zoomLevel'));
  },

  calculateBucketSize: function(zoomLevel) {
    switch(zoomLevel) {
      case 0 : return 24 *60 * 60;  // DAILY
      case 1 : return 8 * 60 * 60;  // EIGHT HOURLY
      case 2 : return 4 * 60 * 60;  // FOUR HOURLY
      case 3 : return 2 * 60 * 60;  // TWO HOURLY
      case 4 : return 60 * 60;      // HOURLY
      case 5 : return 30 * 60;      // HALF HOURLY
      case 6 : return 15 * 60;      // QUARTER HOURLY
      default: null
    }
  },

  isZoomLevelValid: function(zoomLevel) {
    return zoomLevel <= 6 && zoomLevel >= 0;
  },

  hidePair: function(cell) {
    var pairs = this.getSwimlaneLabels();
    var toHide = pairs[cell.row];
    this.saveHiddenPair(toHide);
  },

  resetPairFilter: function() {
    this.hiddenPairs.revealAllPairs();
  },

  saveHiddenPair: function(pairName) {
    var self = this;
    this.hiddenPairs.hidePair(pairName);
  },

  isVisible: function(pairKey) {
    var key = this.hiddenPairs.get(pairKey);
    if (key) {
//      console.debug('[HeatmapProjection.isVisible] hidden: ' + key.id);
      return false;
    } else {
//      console.debug('[HeatmapProjection.isVisible] visible: ' + pairKey);
      return true;
    }
  },

  getSwimlaneLabels: function() {
    var self = this;
    return _.filter(self.aggregates.pluck('pair'), function(pairKey) {
      return self.isVisible(pairKey);
    });
  },

  // VERY IMPORTANT. TODO: Fix this to update properly.
  getRow: function(row) {
    var pair = (this.getSwimlaneLabels())[row];

    if (pair) {
      var pairAggs = this.aggregates.get(pair).get('map') || [];
      var bucketCount = this.get('bucketCount');

      // Determine how many buckets different the projection is to the currently loaded data
      var timeOffsetBuckets = (this.get('lastEndTime').getTime() - this.getProjectionEndTime().getTime()) / 1000 / this.get('bucketSize');
      var lengthOffsetBuckets = bucketCount - pairAggs.length;
      var offsetBuckets = timeOffsetBuckets + lengthOffsetBuckets;
      
      if (offsetBuckets == 0) {
        return pairAggs;
      } else if (Math.abs(offsetBuckets) >= bucketCount) {
        // The aggregates are completely out of range. Return an empty array.
        return [];
      } else if (offsetBuckets > 0) {
        // We need to insert 0 entries
        var prefix = [];
        for (var i = 0; i < offsetBuckets; ++i) prefix.push(0);

        return prefix.concat(pairAggs).slice(0, bucketCount);
      } else {
        return pairAggs.slice(-offsetBuckets);
      }
    } else {
      return [];
    }
  },

  getLeftCount: function(row) {
    if (this.aggregates.length > row) {
      return (this.aggregates.at(row).get('left') || [0])[0];
    } else {
      return 0;
    }
  },

  getRightCount: function(row) {
    if (this.aggregates.length > row) {
      return (this.aggregates.at(row).get('right') || [0])[0];
    } else {
      return 0;
    }
  },

  getProjectionStartTime: function() {
    return this.startTimeFromEndTime(this.getProjectionEndTime());
  },

  getProjectionEndTime: function() {
    if (this.get('fixedEndTime')) return this.get('fixedEndTime');
    return this.get('lastEndTime');
  },

  startTimeFromEndTime: function(endTime) {
    return new Date(endTime.getTime() - this.get('bucketSize') * this.get('bucketCount') * 1000)
  },

  scrollView: function(offset) {
    if (!this.get('fixedEndTime')) this.set({fixedEndTime: this.get('lastEndTime')}, {silent: true});

    var newFixedTime = new Date(this.get('fixedEndTime').getTime() + offset * 1000);
    if (newFixedTime.getTime() > this.rightLimit().getTime()) {
      this.unset('fixedEndTime');
    } else {
      this.set({fixedEndTime: newFixedTime});
    }
  },

  isAtRightLimit: function() {
    return !this.has('fixedEndTime');
  },

  rightLimit: function() {
    return nearestHour();
  }
});

Diffa.Views.Heatmap = Backbone.View.extend(Diffa.Helpers.Viz).extend({
  minRows: 0,         // Minimum number of rows to be displayed

  // The original version of the heatmap was statically sized to 800x400 with 5 swimlanes @ 78 plus a 10 pixel gutter
  // When #232 lands, this will probably be calculated differently.
  statusBarHeight: 32,
  bottomGutter: 0,
  gutterSize: 24,
  gridSize: 30,
  scaleHeight: 40,

  show_grid: false,

  o_x: 0,
  o_y: 0,

  highlighted: null,

  initialize: function() {
    _.bindAll(this, "render", "update", "pollAndUpdate", "mouseUp", "mouseMove", "mouseDown");

    $(document).mouseup(this.mouseUp);
    $(document).mousemove(this.mouseMove);

    this.model.watch($(this.el));

    this.model.bind('change:buckets', this.update);

    this.render();
    this.zoomControls = new Diffa.Views.ZoomControls({el: this.$('.heatmap-controls'), model: this.model});

    // Attach a mousedown handler to the overlay
    this.overlay.onmousedown = this.mouseDown;
    var self = this;

    $('.pair-filter-reset button').on("click", function(event) {
      self.resetPairFilter();
    });
  },

  render: function() {
    $(this.el).html(JST['heatmap/map']());

    this.heatmap = $(this.el)[0];
    this.underlay = this.$('.underlay')[0];
    this.scale = this.$(".scale")[0];

    this.resizeLayer(this.underlay, this.underlay.offsetWidth);
    this.canvas = this.createLayer(this.heatmap, 2);
    this.overlay = this.createLayer(this.heatmap, 4);

    this.context = this.canvas.getContext("2d");
    this.overlayContext = this.overlay.getContext("2d");
    this.underlayContext = this.underlay.getContext("2d");
    this.scaleContext = this.scale.getContext("2d");

    this.update();

    return this;
  },

  pollAndUpdate: function() {
    console.debug('[Heatmap.pollAndUpdate]');
    this.update();
    this.model.sync();
  },

  update: function() {
    console.debug('[Heatmap.update]');
    this.clearEverything();
    this.recalibrateHeatmap();
    this.context.translate(this.o_x, this.o_y);
    this.scaleContext.translate(this.o_x, this.o_y);
    this.drawStatusBar();
    this.drawGrid();
  },

  clearEverything: function() {
    this.clearCanvas();
    this.clearOverlay();
    this.clearUnderlay();
    this.clearScale();
  },

  clearEverythingExceptPairLabels: function() {
    this.clearCanvas();
    this.clearOverlay();
    this.clearScale();
  },

  clearCanvas: function() { this.canvas.width = this.canvas.width; },
  clearOverlay: function() { this.overlay.width = this.overlay.width; },
  clearUnderlay: function() { this.underlay.width = this.underlay.width; },
  clearScale: function() { this.scale.width = this.scale.width; },

  calibrateHeatmap: function() {
    this.scale.width = this.scale.offsetWidth;
    this.scale.height = this.scaleHeight;
    this.visibleColumns = this.truncateInt(this.canvas.width / this.gridSize);
    this.model.set({bucketCount: this.visibleColumns});

    this.$('.heatmap-controls').
        show().
        css('top', $(this.heatmap).offset().top + 20).
        css('left', $(this.heatmap).offset().left - this.$('.heatmap-controls')[0].offsetWidth);
  },
  recalibrateHeatmap: function() {
    this.resizeLayer(this.underlay, this.underlay.offsetWidth);
    this.resizeLayerFromParent(this.canvas, this.underlay);
    this.resizeLayerFromParent(this.overlay, this.underlay);
    this.calibrateHeatmap();
  },

  resizeLayer: function(layer, width) {
    layer.width = width;
    layer.height = this.statusBarHeight +
        this.model.getSwimlaneLabels().length * this.swimlaneHeight() +
        this.bottomGutter;
  },
  resizeLayerFromParent: function(layer, parent) {
    if (parent) {
      var parentOffset = $(parent).offset();

      layer.style.position = "absolute";
      layer.style.left = parentOffset.left.toString() + "px";
      layer.style.top = parentOffset.top.toString() + "px";

      this.resizeLayer(layer, parent.offsetWidth);
    }
  },
  swimlaneHeight: function() { return 2 * this.gutterSize + this.gridSize; },

  createLayer: function(parent, z_index) {
    var layer = document.createElement("canvas");
    document.body.appendChild(layer);
    layer.style.zIndex = z_index;
    this.resizeLayerFromParent(layer,parent);
    return layer;
  },

  drawStatusBar: function() {
    this.placePairFilterChooser();
    this.drawLivenessIndicator();
  },

  resetPairFilter: function(event) {
    this.model.resetPairFilter();
    this.pollAndUpdate();
  },

  placePairFilterChooser: function() {
    var self = this;
    var canvasX = $(self.canvas).offset().left;
    var canvasY = $(self.canvas).offset().top;
    var offset_x = canvasX;
    var offset_y = canvasY;
    $('.pair-filter-chooser').html(self.pairFilter());
    $('.chzn-select').chosen();
    $('#pair-filter').chosen().on("change", function(event) {
      var remainingPairs = $(this).val();
      self.hidePairsExcept(remainingPairs);
    });
  },

  hidePairsExcept: function(visiblePairs) {
    var self = this;
    _.each(self.model.aggregates.pluck('pair'), function(pair) {
      try {
        console.debug('[Heatmap.hidePairsExcept] hiding pair: ' + pair);
        self.model.hiddenPairs.hidePair(pair);
      } catch (e) {}
      _.each(visiblePairs, function(vp) {
        if (vp == pair) {
          console.debug('[Heatmap.hidePairsExcept] revealing pair: ' + pair);
          self.model.hiddenPairs.revealPair(pair);
        }
      });
    });
//    self.pollAndUpdate();
//    window.location.reload(true);
  },

  buildOption: function(pair, optHiddenPair) {
    var html = '';
    if (optHiddenPair) {
      html = '<option value="' + pair + '">' + pair + '</option>';
    } else {
      html = '<option selected="selected" value="' + pair + '">' + pair + '</option>';
    }
    return html;
  },

  pairFilter: function() {
    var self = this;
    var pairs = this.model.aggregates.pluck('pair');

    var html = '<select id="pair-filter" data-placeholder="Select pairs to hide" style="width: 600px;" multiple class="chzn-select">'
    html = html + '<option value=""></option>'
    _.each(pairs, function(pair) {
      html = html + self.buildOption(pair, self.model.hiddenPairs.get(pair));
    });
    html = html + '</select>';

    return html;
  },

  coordToPositionStyle: function(x, y) {
    return 'style="position: absolute; top: ' + y + 'px; left: ' + x + 'px;"';
  },

  drawSwimLane: function(laneIndex, swimLaneLabels, laneHeight, offset, viewportX) {
    var laneTop = offset - laneHeight;
    var arrowWidth = 18;
    var arrowHeight = 12;
    var marginTop = 4;
    var marginSide = 10;
    var textHeight = 11;
    var labelBaseline = laneTop + marginTop; // y2 > y1 <=> y2 is below y1

    this.dashedLine(this.underlayContext, 0, offset, this.canvas.width, offset, 2);
    if (swimLaneLabels[laneIndex] != null) {
      this.underlayContext.fillStyle = colours.translucent;
      var textWidth = this.underlayContext.measureText(swimLaneLabels[laneIndex]).width;
      this.underlayContext.fillRect(marginSide, laneTop + marginTop, textWidth, labelBaseline - laneTop - marginTop + textHeight)

      this.underlayContext.fillStyle = colours.black;
      this.underlayContext.textBaseline = "top";
      this.underlayContext.font = textHeight.toString() + "px 'Lucida Grande', Tahoma, Arial, Verdana, sans-serif";
      this.underlayContext.fillText(swimLaneLabels[laneIndex], marginSide, labelBaseline);
    }

    // Draw arrows if we have values outside the map for this row
    var cell = this.coordsToCell({"x": viewportX, "y": laneTop});
    if (this.model.getLeftCount(cell.row)) {
      this.drawArrow(this.underlayContext, directions.left, marginSide, offset - (arrowHeight / 4) - (this.gridSize / 2), arrowWidth, arrowHeight);
    }
    if (this.model.getRightCount(cell.row)) {
      this.drawArrow(this.underlayContext, directions.right, this.canvas.width - marginSide - arrowWidth, offset - (arrowHeight / 4) - (this.gridSize / 2), arrowWidth, arrowHeight);
    }
  },

  drawSwimLanes: function(swimLaneLabels, laneHeight) {
    var canvasHeight = this.canvas.height;
    var viewportX = this.o_x;
    viewportX = Math.abs(viewportX);// workaround for a bug in Chrome, Math.abs sometimes gets optimized away or otherwise borked

    var lane = 0;
    for (var y_offset = laneHeight + this.statusBarHeight; y_offset <= canvasHeight; y_offset += laneHeight) {
      this.drawSwimLane(lane, swimLaneLabels, laneHeight, y_offset, viewportX);
      lane++;
    }
  },

  drawLivenessIndicator: function() {
    var isLive = this.model.isAtRightLimit() && this.o_x == 0;
    var pollText = isLive ? " LIVE " : " LOCKED ";
    var textWidth = this.underlayContext.measureText(pollText).width;
    var textSpacer = 20; // padding around LIVE/LOCKED
    this.underlayContext.fillStyle = colours.darkGrey;
    this.underlayContext.fillRect(0, 0, this.canvas.width, this.statusBarHeight);
    this.underlayContext.fillStyle = isLive ? colours.red : colours.darkGrey;
    this.underlayContext.fillRect(this.canvas.width - textWidth - textSpacer, 0, textWidth + textSpacer, this.statusBarHeight);
    this.underlayContext.fillStyle = colours.white;
    this.underlayContext.font = "12px 'Lucida Grande', Tahoma, Arial, Verdana, sans-serif";
    this.underlayContext.textBaseline = "top";
    this.underlayContext.fillText(pollText, this.canvas.width - this.underlayContext.measureText(pollText).width - (textSpacer / 2), 5);
  },

  drawBlobs: function(region_width, laneHeight, y_offset) {
    // (x,y) are pixel co-ordinates relative to an origin at top-left of heatmap.
    for (var x = 0; x <= region_width; x += this.gridSize) {
      for (var y = this.statusBarHeight; y <= this.canvas.height; y += laneHeight) {
        this.drawCircle(x, y);
      }
    }
  },

  drawCircle: function(x, y) {
    var cell = this.coordsToCell({"x":x, "y":y});

    if (cell.column <= this.visibleColumns && cell.row < this.model.get('maxRows')) {
      var cell_x = x + Math.floor(this.gridSize / 2);
      var cell_y = y + this.gutterSize + Math.floor(this.gridSize / 2);

      // *****************************************
      // VERY IMPORTANT!!!
      // *****************************************
      // This drives whether a blob is drawn here!
      // *****************************************
      var bucketSize = this.model.getRow(cell.row)[cell.column] || 0;


      var maximum = Math.floor((this.gridSize - 1) / 2);

      var cappedSize = this.transformBucketSize(bucketSize, {
        inputMin: 1,
        inputMax: 100,
        outputMin: 2,
        outputMax: maximum
      });
      var size = cappedSize.value;
      var isOverMaximum = cappedSize.limited;

      if (size > 0) {
//        console.debug('[Heatmap.drawCircle] (x,y): (' + x + ',' + y + ')');
//        console.debug('[Heatmap.drawCircle] (row,col): (' + cell.row + ',' + cell.column + ')');
        // if the size has been limited, draw the outline slightly thicker
        this.context.lineWidth = isOverMaximum ? 3 : 2;
        this.context.strokeStyle = colours.black;
        this.context.fillStyle = colours.white;
        this.context.beginPath();
        this.context.arc(cell_x, cell_y, size, 0, Math.PI * 2, false);
        this.context.closePath();
        this.context.stroke();
        this.context.fill();
      }
    }
  },

  drawTimeAxis: function() {
    var every = 3;      // At what column intervals labels will be placed
    var startTime = this.model.getProjectionStartTime();
    var bucketSize = this.model.get('bucketSize');
    var zoomLevel = this.model.get('zoomLevel');
    this.scaleContext.font = "9px sans-serif";
    var alignedStart = this.align(startTime, every);
    var alignOffset = (alignedStart.getTime() - startTime.getTime()) / 1000 / bucketSize * this.gridSize;

    // Iterate the columns, and place a label at the 'every' interval
    for (var sc = 0; sc < this.visibleColumns; sc++) {
      if (sc % every == 0) {
        var tick = new Date(alignedStart.getTime() + (sc * bucketSize * 1000));
        this.scaleContext.fillText(tick.toString("dd/MM"), sc * this.gridSize + alignOffset, 10);
        this.scaleContext.fillText(tick.toString("HH:mm"), sc * this.gridSize + alignOffset, 20);
      }
    }
  },

  drawGrid: function() {
    var region_width = this.visibleColumns * this.gridSize;
    // draw grid lines
    if (this.show_grid) {
      for (var x = 0.5; x <= region_width; x += this.gridSize) {
        this.context.moveTo(x, this.statusBarHeight);
        this.context.lineTo(x, this.canvas.height);
      }
      for (var y = this.statusBarHeight; y <= this.canvas.height; y += this.swimlaneHeight()) {
        this.context.moveTo(0, y);
        this.context.lineTo(region_width, y);
      }
      this.context.strokeStyle = colours.translucent;
      this.context.stroke();
    }

    this.drawSwimLanes(this.model.getSwimlaneLabels(), this.swimlaneHeight());
    this.drawBlobs(region_width, this.gridSize + 2 * this.gutterSize, this.statusBarHeight);
    this.drawTimeAxis();
  },

  align: function(time, skip) {
    var millis = time.getTime();
    var divisions = this.model.get('bucketSize') * skip;
    var next = Math.ceil(millis / 1000 / divisions) * divisions * 1000;
    return new Date(next);
  },

  dashedLine: function(ctx, x1, y1, x2, y2, dashLen) {
    if (dashLen == undefined) dashLen = 2;

    ctx.beginPath();
    ctx.moveTo(x1, y1);

    var dX = x2 - x1;
    var dY = y2 - y1;
    var dashes = Math.floor(Math.sqrt(dX * dX + dY * dY) / dashLen);
    var dashX = dX / dashes;
    var dashY = dY / dashes;

    var q = 0;
    while (q++ < dashes) {
      x1 += dashX;
      y1 += dashY;
      if (q % 2 == 0) {
        ctx.moveTo(x1, y1);
      }
      else {
        ctx.lineTo(x1, y1);
      }
    }
    if (q % 2 == 0) {
      ctx.moveTo(x1, y1);
    }
    else {
      ctx.lineTo(x1, y1);
    }

    ctx.lineWidth = 1;
    ctx.stroke();
    ctx.closePath();
  },

  drawArrow: function(ctx, dir, x, y, w, h) {
    var headWidth = w / 2;
    var cornerHeight = h - (h / 4);

    var startX = x + (dir == directions.left ? 0 : w),
        headX  = x + (dir == directions.left ? headWidth : w - headWidth),
        endX   = x + (dir == directions.left ? w : 0);

    var gradient = this.context.createLinearGradient(startX, y, endX, y);
    gradient.addColorStop(0, colours.darkGrey);
    gradient.addColorStop(1, colours.transparent);

    ctx.save();
    ctx.strokeStyle = colours.transparent;
    ctx.fillStyle = gradient;
    ctx.beginPath();
    ctx.moveTo(startX, y + h / 2);
    ctx.lineTo(headX, y);
    ctx.lineTo(headX, y + cornerHeight);
    ctx.lineTo(endX,  y + cornerHeight);
    ctx.lineTo(endX, y + h - cornerHeight);
    ctx.lineTo(headX, y + h - cornerHeight);
    ctx.lineTo(headX, y + h);
    ctx.closePath();
    ctx.stroke();
    ctx.fill();
    ctx.restore();
  },

  drawOverlay: function() {
    if (this.highlighted != null && this.highlighted.column >= 0 && this.highlighted.row >= 0) {
      var value = this.model.getRow(this.highlighted.row)[this.highlighted.column];
      if (value > 0) {
        var c_x = this.highlighted.column * this.gridSize;
        var c_y = this.statusBarHeight + (this.highlighted.row * this.swimlaneHeight()) + this.gutterSize + this.gridSize;
        this.overlayContext.font = "12px sans-serif";
        this.overlayContext.textBaseline = "top";
        var width = this.context.measureText("" + value).width;
        this.overlayContext.fillText(value, c_x + Math.floor(this.gridSize / 2) - Math.floor(width / 2), c_y);
      }
    }
  },

  coords: function(e) {
    var x;
    var y;
    if (e.pageX != undefined && e.pageY != undefined) {
      x = e.pageX;
      y = e.pageY;
    }
    else {
      x = e.clientX + document.body.scrollLeft + document.documentElement.scrollLeft;
      y = e.clientY + document.body.scrollTop + document.documentElement.scrollTop;
    }

    x -= this.heatmap.offsetLeft;
    y -= this.heatmap.offsetTop;

    return { "x":x, "y":y };
  },

  coordsToCell: function(coords) {
    return {
      "row": Math.floor((coords.y - this.statusBarHeight) / this.swimlaneHeight()),
      "column": Math.floor((coords.x) / this.gridSize)
    };
  },

  cellToCoords: function(cell) {
    return {
      "x": cell.column * this.gridSize,
      "y": cell.row * (2 * this.gutterSize + this.gridSize)
    };
  },

  dragging: false,
  dragged: false,

  mouseDown: function(e) {
    this.show_grid = true;
    this.dragging = e;
    this.dragged = false;
    e.target.style.cursor = "move";
    return false;
  },

  lastClick: undefined,
  lastClickTime: undefined,

  // TODO: register for double-click event instead. Although that may have strange consequences.
  isDoubleClick: function(lastClick, lastClickTime, thisClick) {
    var dblClick = false;
    var driftMargin = 6;
    var activeWindow = 400; // milliseconds
    var clickTime = new Date();

    if (lastClick && lastClickTime) {
      var cLast = this.coords(lastClick);
      var cThis = this.coords(thisClick);
      if (Math.abs(cLast.x - cThis.x) < driftMargin && Math.abs(cLast.y - cThis.y) < driftMargin) {
        if (clickTime - lastClickTime < activeWindow) {
          dblClick = true;
        }
      }
    }
    this.lastClickTime = clickTime;
    this.lastClick = lastClick;
    return dblClick;
  },

  mouseUp: function(e) {
    var self = this;
    var c = this.coords(e);
    if (self.isDoubleClick(self.lastClick, self.lastClickTime, e)) {
      this.model.hidePair(this.coordsToCell(c));
      self.pollAndUpdate();
    }

    this.show_grid = false;
    this.dragging = false;
    if (!this.dragged) {
      if (e.target.tagName == "CANVAS") {
        var c = this.coords(e);
        c.x -= this.o_x;

        // Perform a navigation
        var cell = this.coordsToCell(c);
        var selectedPair = this.model.getSwimlaneLabels()[cell.row];
        var gridStartTime = this.model.getProjectionStartTime();
        var selectedIdx = cell.column;
        var bucketSize = this.model.get('bucketSize');

        var selectionStartTime = new Date(gridStartTime.getTime() + (selectedIdx * bucketSize * 1000));
        var selectionEndTime = new Date(selectionStartTime.getTime() + (bucketSize * 1000));
        $(this.el).trigger('blob:selected', [selectedPair, Diffa.Helpers.DatesHelper.toISOString(selectionStartTime), Diffa.Helpers.DatesHelper.toISOString(selectionEndTime)]);
      }
    } else {
      this.pollAndUpdate();
    }
    this.dragged = false;
    e.target.style.cursor = "default";
    self.lastClick = e;
  },

  mouseMove: function(e) {
    if (this.dragging) {
      this.dragged = true;
      this.clearEverything();
      var m_coords = this.coords(e);
      var d_coords = this.coords(this.dragging);
      this.o_x += m_coords.x - d_coords.x;

      // Calculate the number of cumulative buckets we've changed, along with the remaining positional offset
      var bucketsChange = this.truncateInt(this.o_x / this.gridSize);
      this.o_x %= this.gridSize;

      var secondsChange = (bucketsChange * this.model.getBucketSize());
      if (secondsChange != 0) {
        this.model.scrollView(-secondsChange);
      }

      if (this.model.isAtRightLimit() && this.o_x < 0) {
        this.o_x = 0;
      }

      this.context.translate(this.o_x, this.o_y);
      this.scaleContext.translate(this.o_x, this.o_y);
      this.drawGrid();
      this.drawStatusBar();
      this.dragging = e;
    }
    else {
      this.clearOverlay();
      this.overlayContext.translate(this.o_x, this.o_y);
      this.mouseOver(e);
    }
  },

  truncateInt: function(d) {
    if (d < 0) return Math.ceil(d);
    return Math.floor(d);
  },

  mouseOver: function(e) {
    var c = this.coords(e);
    c.x -= this.o_x;
    var cell = this.coordsToCell(c);

    if (cell.row >= 0 && cell.row < this.model.get('maxRows') && cell.column >= 0 && cell.column < this.visibleColumns) {
      this.highlighted = cell;
      this.drawOverlay();
    }
  }
});

Diffa.Views.PairFilter = Backbone.View.extend({
  events: {
    "click .chzn-drop": ""
  }
});

Diffa.Views.ZoomControls = Backbone.View.extend({
  events: {
    "click  .zoomIn":   "zoomIn",
    "click  .zoomOut":  "zoomOut",

    "focus  .zoomIn":   "preventFocus",
    "focus  .zoomOut":  "preventFocus"
  },

  initialize: function() {
    var self = this;

    _.bindAll(this, "render");

    this.model.bind("change:zoomLevel", this.render);

    $(document).keypress(function(e) {
      if (e.charCode == '+'.charCodeAt()) {
        e.preventDefault();
        if (self.shouldAllowMoreZoomIn()) self.zoomIn();
      }
      if (e.charCode == '-'.charCodeAt()) {
        e.preventDefault();
        if (self.shouldAllowMoreZoomOut()) self.zoomOut();
      }

      return true;
    });

    this.render();
  },

  render: function() {
    function toggleControl(selector, isDisabled) {
      if (isDisabled) {
        $(selector).attr('disabled', 'disabled');
      } else {
        $(selector).removeAttr('disabled');
      }
    }

    toggleControl('.zoomIn', !this.shouldAllowMoreZoomIn());
    toggleControl('.zoomOut', !this.shouldAllowMoreZoomOut());
  },

  shouldAllowMoreZoomIn: function() {
    return this.model.get('zoomLevel') < 6;   // Maximum zoom level is 6 - corresponds to a 15 minute granularity
  },
  shouldAllowMoreZoomOut: function() {
    return this.model.get('zoomLevel') > 0;  // Minimal zoom level is 0 - corresponds to a daily granularity
  },

  zoomOut: function() { this.model.zoomOut(); },
  zoomIn: function() { this.model.zoomIn(); },

  preventFocus: function(e) { $(e.target).blur(); }
});

Diffa.Models.Diff = Backbone.Model.extend({
  pendingUpstreamRequest: null,
  pendingDownstreamRequest: null,

  initialize: function() {
    _.bindAll(this, "retrieveDetails", "ignore");
  },

  /**
   * Fill out this diff with more expensive-to-capture details, such as upstream/downstream content.
   */
  retrieveDetails: function() {
    var self = this;

    // Only retrieve the pair info if we don't already have it
    if (!self.get('upstreamName') || !self.get('downstreamName')) {
      $.get("/domains/" + self.collection.domain.id + "/config/pairs/" + this.get('objId').pair.key, function(data, status, xhr) {
        self.set({upstreamName: data.upstreamName, downstreamName: data.downstreamName});
      });
    }

    // Always retrieve the latest content for the content panels
    var getContent = function(field, upOrDown, pendingRequest) {
      if (pendingRequest) pendingRequest.abort();

      function setContent(content) {
        var attrs = {};
        attrs[field] = content;
        self.set(attrs);
      }

      pendingRequest = $.ajax({
            url: "/domains/" + self.collection.domain.id + "/diffs/events/" + self.id + "/" + upOrDown,
            success: function(data) {
              setContent(data || "no content found for " + upOrDown);
            },
            error: function(xhr, status, ex) {
              if (status != "abort") {
                if(console && console.warn)
                  console.warn('error getting the content for ' + upOrDown, status, ex, xhr);

                setContent("Content retrieval failed");
              }
            }
          });
      return pendingRequest;
    };

    this.pendingUpstreamRequest = getContent("upstreamContent", "upstream", this.pendingUpstreamRequest);
    this.pendingDownstreamRequest = getContent("downstreamContent", "downstream", this.pendingDownstreamRequest);
  },

  /**
   * Instructs the agent to ignore this difference.
   */
  ignore: function() {
    var self = this;

    $.ajax({
      url: "/domains/" + this.collection.domain.id + "/diffs/events/" + this.id,
      type: 'DELETE',
      success: function(data) {
        self.collection.domain.aggregates.sync();
        self.collection.domain.diffs.sync();
      },
      error: function(xhr, status, ex) {
        // TODO:
      }
    });
  }
});

Diffa.Collections.Diffs = Diffa.Collections.CollectionBase.extend({
  watchInterval: 5000,      // How frequently we poll for diff updates
  range: null,
  page: 0,
  listSize: 20,
  selectedEvent: null,
  model: Diffa.Models.Diff,
  totalEvents: 0,
  totalPages: 0,
  lastSeqId: null,

  initialize: function(models, opts) {
    _.bindAll(this, "sync", "select", "selectEvent", "selectNextEvent");

    this.domain = opts.domain;
  },

  sync: function(force) {
    var self = this;

    if (this.range == null) {
      this.reset([]);
    } else {
      var url = "/domains/" + self.domain.id + "/diffs?pairKey=" + this.range.pairKey + "&range-start="
          + this.range.start + "&range-end=" + this.range.end
          + "&offset=" + (this.page * this.listSize) + "&length=" + this.listSize;

      $.get(url, function(data) {
        if (!force && data.seqId == self.lastSeqId) return;

        var diffs = _.map(data.diffs, function(diffEl) { diffEl.id = diffEl.seqId; return diffEl; });

        if (self.totalEvents != data.total) {
          self.totalEvents = data.total;
          self.totalPages = Math.ceil(self.totalEvents / self.listSize);
          self.trigger("change:totalEvents", self);
        }

        // Apply updates to the diffs that we currently have
        var newDiffEls = _.map(diffs, function(diff) {
          var current = self.get(diff.seqId);
          if (current == null) {
            return diff;
          } else {
            current.set(diff);    // Apply changes to the difference
            return current;
          }
        });
        self.reset(newDiffEls);

        // Select the first event when we don't have anything selected, or when the current selection is no longer
        // valid
        if (self.selectedEvent == null || !self.get(self.selectedEvent.id)) {
          if (diffs.length > 0)
            self.selectEvent(diffs[0].seqId);
          else
            self.selectEvent(null);
        }

        // If we're now beyond the last page, then scroll back to it
        if (self.page >= self.totalPages && self.totalPages > 0) {
          self.setPage(self.totalPages - 1, true);
        }

        self.lastSeqId = data.seqId;
      });
    }
  },

  select: function(pairKey, start, end) {
    this.range = {
      pairKey: pairKey,
      start: start,
      end: end
    };
    this.setPage(0, true);
  },

  selectEvent: function(evtId) {
    this.selectedEvent = this.get(evtId);
    this.trigger("change:selectedEvent", this.selectedEvent);
  },

  selectNextEvent: function() {
    this.selectEventWithOffset(1);
  },

  selectPreviousEvent: function() {
    this.selectEventWithOffset(-1);
  },

  selectEventWithOffset: function(offset) {
    if (this.selectedEvent != null) {
      var selectedIdx = this.indexOf(this.selectedEvent);
      var newIdx = selectedIdx + offset;
      if (newIdx >= 0 && newIdx < this.length) {
        var nextEvent = this.at(newIdx);
        if (nextEvent != null) {
          this.selectEvent(nextEvent.id);
        }
      }
    }
  },

  nextPage: function() {
    if (this.page < this.totalPages) this.setPage(this.page + 1);
  },

  previousPage: function() {
    if (this.page > 0) this.setPage(this.page - 1);
  },

  setPage: function(page, force) {
    if (force || this.page != page) {
      this.page = page;
      this.trigger("change:page", this);

      this.sync(true);
    }
  }
});

Diffa.Views.DiffList = Backbone.View.extend({
  events: {
    "click .previous": "previousPage",
    "click .next":     "nextPage"
  },

  initialize: function() {
    var self = this;

    _.bindAll(this, "rebuildDiffList", "renderNavigation");

    this.model.watch($(this.el));

    this.model.bind("reset",              this.rebuildDiffList);
    this.model.bind("change:totalEvents", this.renderNavigation);
    this.model.bind("change:page",        this.renderNavigation);

    $(document).keydown(function(e) {
      if (e.keyCode == 38) {  // Up arrow
        e.preventDefault();
        self.model.selectPreviousEvent();
      }
      if (e.keyCode == 40) {    // Down arrow
        e.preventDefault();
        self.model.selectNextEvent();
      }
      if (e.keyCode == 37) {  // Left arrow
        e.preventDefault();
        self.model.previousPage();
      }

      if (e.keyCode == 39) {  // Right arrow
        e.preventDefault();
        self.model.nextPage();
      }

      return true;
    });

    $(this.el).html(JST['heatmap/difflist']());
    this.renderNavigation();
  },

  rebuildDiffList: function() {
    var self = this;

    this.$('.difflist-body').empty();   // Empty the current difflist out since we'll re-render everything

    this.model.forEach(function(diff) {
      var view = new Diffa.Views.DiffListItem({model: diff, collection: self.model});
      this.$('.difflist-body').append(view.render().el);
    });

    if ($('.difflist-body').children().length == 0) {
      $('.difflist-body').html("No differences.");
    }
  },

  renderNavigation: function() {
    var startIdx = (this.model.page * this.model.listSize) + 1;
    var endIdx = Math.min(startIdx + this.model.listSize - 1, this.model.totalEvents);

    this.$(".pagecount").text("Showing " + startIdx + " - " + endIdx + " of " + this.model.totalEvents + " differences");
    this.$(".navigation").toggle(this.model.totalPages > 1);
  },

  nextPage: function(e) { e.preventDefault(); this.model.nextPage(); },
  previousPage: function(e) { e.preventDefault(); this.model.previousPage(); }
});

Diffa.Views.DiffListItem = Backbone.View.extend({
  tagName: 'div',
  className: 'difflist-row',

  events: {
    "click": "select",
    "dblclick": "expand"
  },

  initialize: function() {
    _.bindAll(this, "render", "select", "updateSelected");

    this.collection.bind("change:selectedEvent", this.updateSelected);
  },

  render: function() {
    var time = new Date(this.model.get('detectedAt')).toString("HH:mm:ss");
    var date = new Date(this.model.get('detectedAt')).toString("dd/MM/yyyy");
    var row = $(this.el)
        .append("<div class='date-col'>" + date + "</div>")
        .append("<div class='time-col'>" + time + "</div>")
        .append("<div class='pairing-col wrappable'>" + this.model.get('objId').pair.key + "</div>")
        .append("<div class='item-id-col wrappable'>" + this.model.get('objId').id + "</div>");

    if (!this.model.get('upstreamVsn')) {
      row.append("<div class='difference-col last'>Missing from upstream</div>");
    }
    else if (!this.model.get('downstreamVsn')) {
      row.append("<div class='difference-col last'>Missing from downstream</div>");
    }
    else {
      row.append("<div class='difference-col last'>Data difference</div>");
    }

    this.updateSelected(this.collection.selectedEvent);

    return this;
  },

  select: function() {
    this.collection.selectEvent(this.model.id);
  },

  expand: function(e) {
    $(this.el).trigger('expand-event', [this.model]);

    // Double clicking results in the text being selected. We don't actually want that, so we'll
    // clear the selection.
    if(document.selection && document.selection.empty) {
        document.selection.empty();
    } else if(window.getSelection) {
        var sel = window.getSelection();
        sel.removeAllRanges();
    }
  },

  updateSelected: function(selectedEvent) {
    $(this.el).toggleClass("specific_selected", selectedEvent != null && selectedEvent.id == this.model.id)
  }
});

Diffa.Views.DiffDetail = Backbone.View.extend({
  lastSelected: null,

  initialize: function() {
    _.bindAll(this, "render", "updateSelected");

    this.model.bind("change:selectedEvent", this.updateSelected);

    var template = JST['heatmap/contentviewer'];

    $(this.el).html(template({API_BASE: API_BASE}));
    this.render();
  },

  updateSelected: function(newSelected) {
    if (this.lastSelected != newSelected) {
      // Swap the target of our event subscriptions
      if (this.lastSelected != null) this.lastSelected.bind("change", this.render);
      if (newSelected != null) newSelected.bind("change", this.render);

      // Record the last selected diff so we can cleanup event bindings
      this.lastSelected = newSelected;
      
      // Ensure that we have full details for the newly selected event
      if (newSelected != null) newSelected.retrieveDetails();

      this.render();
    }
  },

  render: function() {
    var event = this.model.selectedEvent;

    // Clear the state if we don't have a selected event
    if (event == null) {
      this.$('.content-label').text('No item selected');
      this.$('.item1 .upstreamLabel').text('upstream');
      this.$('.item1 .diff-hash').text('');
      this.$('.item2 .downstreamLabel').text('downstream');
      this.$('.item2 .diff-hash').text('');

      this.$(".controllist").hide();
      this.$(".actionlist").empty();
      return;
    }

    var itemID = event.get('objId').id,
        upstreamLabel = event.get('upstreamName') || "upstream",
        upstreamVersion = event.get('upstreamVsn') || "no version",
        downstreamLabel = event.get("downstreamName") || "downstream",
        downstreamVersion = event.get('downstreamVsn') || "no version",
        upstreamContent = event.get("upstreamContent"),
        downstreamContent = event.get("downstreamContent");

    this.$('.content-label').text('Versions for item ID: ' + itemID);

    this.$('.item1 .upstreamLabel').text(upstreamLabel);
    this.$('.item1 .diff-hash').text(upstreamVersion);

    this.$('.item2 .downstreamLabel').text(downstreamLabel);
    this.$('.item2 .diff-hash').text(downstreamVersion);

    var ignoreButton = $('<button class="repair">Ignore</button>');
    this.$('.controllist').empty().append(ignoreButton).show();
    ignoreButton.click(function() {
      event.ignore();
    });

    this.renderEntityScopedActions();
  },

  renderEntityScopedActions: function() {
    var event = this.model.selectedEvent;
    var self = this;

    var pairKey = event.get('objId').pair.key;
    var itemID = event.get('objId').id;
    var actionListContainer = this.$(".actionList").empty();
    var actionListCallback = function(actionList, status, xhr) {
      if (!actionList) {
        return;
      }
      
      self.$(".actionlist").empty();
      $.each(actionList, function(i, action) {
        var repairStatus = self.$('.repairstatus');
        appendActionButtonToContainer(actionListContainer, action, pairKey, itemID, repairStatus);
      });
    };

    $.ajax({ url: "/domains/" + this.model.domain.id + '/actions/' + pairKey + '?scope=entity', success: actionListCallback });
  }
});

Diffa.Views.DiffInspectorPopup = Backbone.View.extend({
  initialize: function() {
    var self = this;

    $(document).bind('expand-event', function(e, model) {
      // Attach the inspector view, and open a lightbox
      var inspector = new Diffa.Views.DiffInspector({model: model});
      $.colorbox({
        inline: true,
        href: $(inspector.el)
      });
    });

      // Keep our element invisible
    $(this.el).hide();
  }
});

Diffa.Views.DiffInspector = Backbone.View.extend({
  tagName: 'div',
  className: 'diffa-diffinspector',

  initialize: function() {
    _.bindAll(this, "render");

    $(this.el).html(JST['heatmap/contentinspector']({API_BASE: API_BASE}));
    this.render();
  },

  render: function() {
    var event = this.model;
    var self = this;

    var itemID = event.get('objId').id,
        upstreamLabel = event.get('upstreamName') || "upstream",
        upstreamVersion = event.get('upstreamVsn') || "no version",
        downstreamLabel = event.get("downstreamName") || "downstream",
        downstreamVersion = event.get('downstreamVsn') || "no version",
        upstreamContent = event.get("upstreamContent"),
        downstreamContent = event.get("downstreamContent");

    this.$('.content-label').text('Content for item ID: ' + itemID);

    this.$('.item1 .upstreamLabel').text(upstreamLabel);
    this.$('.item1 .diff-hash').text(upstreamVersion);

    this.$('.item2 .downstreamLabel').text(downstreamLabel);
    this.$('.item2 .diff-hash').text(downstreamVersion);

    function waitForOrDisplayContent(selector, content) {
      var busy = this.$(selector).prev();

      if (content == null) {
        self.$(selector).hide();
        busy.show();
      } else {
        self.$(selector).text(content).show();
        busy.hide();
      }
    }
    waitForOrDisplayContent(".item1 pre", upstreamContent);
    waitForOrDisplayContent(".item2 pre", downstreamContent);
  }
});

function nearestHour() {
  var hours = (new Date()).getHours() + 1;
  return Date.today().add({hours: hours});
}

function conditionalLoad(domain, msg, fn) {
  domain.loadAll(['pairs', 'hiddenPairs'], function() {
    if (domain.pairs.length > 0) {
      fn();
    }
  });
}

$('.diffa-heatmap').each(function() {
  var elem = this;
  var domain = Diffa.DomainManager.get($(elem).data('domain'), $(elem).data('user'));

  conditionalLoad(
    domain,
    'diffa-heatmap',
    function() {
      new Diffa.Views.Heatmap({
        el: $(elem),
        model: new Diffa.Models.HeatmapProjection({aggregates: domain.aggregates, domain: domain})
      });
    }
  );
});

$('.diffa-difflist').each(function() {
  var domain = Diffa.DomainManager.get($(this).data('domain'));
  var elem = this;
  conditionalLoad(
    domain,
    'diffa-difflist',
    function() {
      new Diffa.Views.DiffList({el: $(elem), model: domain.diffs})
    }
  );
});

$('.diffa-contentviewer').each(function() {
  var domain = Diffa.DomainManager.get($(this).data('domain'));
  var elem = this;
  conditionalLoad(
    domain,
    'diffa-contentviewer',
    function() {
      new Diffa.Views.DiffDetail({el: $(elem), model: domain.diffs})
    }
  );
});

$('.diffa-contentinspector').each(function() {
  var domain = Diffa.DomainManager.get($(this).data('domain'));
  var elem = this;
  conditionalLoad(
    domain,
    'diffa-contentinspector',
    function() {
      new Diffa.Views.DiffInspectorPopup({el: $(elem)})
    }
  );
});

$('.diffa-heatmap-page').each(function() {
  var domain = Diffa.DomainManager.get($(this).data('domain'));

  new Diffa.Routers.Blobs({domain: domain, el: $(this)});
  Backbone.history.start();
});
});
