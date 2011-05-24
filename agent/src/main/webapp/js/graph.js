var display;
var canvas;
var context;
var overlay;
var overlayContext;
var underlay;
var underlayContext;
var scale;
var scaleContext;
var buckets;
var maxColumns = 100;
var maxRows = 10;
var gridSize = 30;
var gutterSize = 12;

var rightLimit = 0;

function initCanvas() {
  display = document.getElementById("display");

  canvas = document.getElementById("canvas");
  context = canvas.getContext("2d");
  overlay = document.getElementById("overlay");
  overlayContext = overlay.getContext("2d");

  underlay = document.getElementById("underlay");
  underlayContext = underlay.getContext("2d");

  scale = document.getElementById("scale");
  scaleContext = scale.getContext("2d");

  rightLimit = (maxColumns * gridSize) - canvas.width;
}

function clearCanvas() {
  canvas.width = canvas.width;
}

function clearOverlay() {
  overlay.width = overlay.width;
}

function clearUnderlay() {
  underlay.width = underlay.width;
}

function clearScale() {
  scale.width = scale.width;
}

var sessionId = null;

function createSession(cb) {
  var handleSessionId = function(data, status, req) {
    var location = req.getResponseHeader('Location');
    var parts = location.split("/");
    var sessionID = parts[parts.length - 1];
    sessionId = sessionID;

    cb();
  };
  $.post(API_BASE + '/diffs/sessions', {}, handleSessionId, "json");
}

var startTime, endTime;
var bucketSize = 3600;

var swimlaneLabels = [];
function loadTestData() {
  buckets = [];
  for (var i = 0; i < maxRows; i++) {
    var row = [];
    for (var j = 0; j < maxColumns; j++) {
      row[j] = 0;
    }
    buckets[i] = row;
  }

  endTime = new Date();
  var now = endTime.formatString("YYYY0MM0DDT0hh0mm0ssZ");

  startTime = new Date(endTime - (3600 * maxColumns * 1000));
  var dayBeforeNow = startTime.formatString("YYYY0MM0DDT0hh0mm0ssZ");

  $.get("rest/diffs/sessions/" + sessionId + "/zoom?range-start=" + dayBeforeNow + "&range-end=" + now + "&bucketing=3600", function(data) {
    var indexer = 0;

    for (var pair in data) {
      swimlaneLabels[indexer] = pair;
      for (var x = 0; x < data[pair].length; x++) {
        buckets[indexer][x] = data[pair][x];
      }
      indexer++;
    }
    clearCanvas();
    o_x = -1 * rightLimit;
    context.translate(o_x, o_y);
    scaleContext.translate(o_x, o_y);
    drawGrid();
  });
}

function dashedLine(ctx, x1, y1, x2, y2, dashLen) {
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

  ctx.stroke();
  ctx.closePath();
}

var selected;
function drawCircle(i, j) {
  var cell = coordsToPosition({"x":i, "y":j});
  if (cell.column < maxColumns && cell.row < maxRows) {
    var cell_x = i + Math.floor(gridSize / 2);
    var cell_y = j + gutterSize + Math.floor(gridSize / 2);
    var size = limit(buckets[cell.row][cell.column], Math.floor((gridSize - 1) / 2));
    if (size.limited) {
      context.lineWidth = 2;
    }
    else {
      context.lineWidth = 1;
    }
    if (selected != null && selected.row == cell.row && selected.column == cell.column) {
      context.strokeStyle = "red";
    }
    else {
      context.strokeStyle = "black";
    }
    context.fillStyle = "white";
    context.beginPath();
    context.arc(cell_x, cell_y, size.value, 0, Math.PI * 2, false);
    context.closePath();
    context.stroke();
    context.fill();
  }
}

var show_grid = false;
function drawGrid() {
  var region_width = maxColumns * gridSize;
  if (show_grid) {
    for (var x = 0.5; x < region_width; x += gridSize) {
      context.moveTo(x, 0);
      context.lineTo(x, canvas.height);
    }
    for (var y = 0.5; y < canvas.height; y += (2 * gutterSize + gridSize)) {
      context.moveTo(0, y);
      context.lineTo(region_width, y);
    }
    context.strokeStyle = "red";
    context.stroke();
  }

  var lane = 0;
  for (var s = 0.5 + (2 * gutterSize + gridSize); s < canvas.height; s += (2 * gutterSize + gridSize)) {
    dashedLine(underlayContext, 0, s, region_width, s, 2);
    if (swimlaneLabels[lane] != null) {
      underlayContext.font = "italic 12px serif";
      underlayContext.fillText(swimlaneLabels[lane], 10, s - 3);
    }
    lane++;
  }


  for (var i = 0.5; i < region_width; i += gridSize) {
    for (var j = 0.5; j < canvas.height; j += (2 * gutterSize + gridSize)) {
      drawCircle(i, j);
    }
  }

  drawScale();
}

function drawScale() {
  scaleContext.font = "9px sans-serif";
  for (var i = 0; i < maxColumns; i++) {
    if (i % 3 == 0) {
      var tick = new Date(startTime.getTime() + (i * bucketSize * 1000));
      var time = tick.formatString("0hh:0mm");
      scaleContext.fillText(tick.formatString("0DD/0MM"), i * gridSize, 10);
      scaleContext.fillText(time, i * gridSize, 20);
    }
  }
}

var highlighted;
function drawOverlay() {
  if (highlighted != null && highlighted.column >= 0 && highlighted.row >= 0) {
    var value = buckets[highlighted.row][highlighted.column];
    if (value > 0) {
      var c_x = highlighted.column * gridSize;
      var c_y = (highlighted.row * (2 * gutterSize + gridSize)) + gutterSize + Math.floor(gridSize / 2);
      overlayContext.font = "bold 12px sans-serif";
      overlayContext.textBaseline = "middle";
      var width = context.measureText("" + value).width;
      overlayContext.fillText(value, c_x + Math.floor(gridSize / 2) - Math.floor(width / 2), c_y);
    }
  }
}

function limit(value, maximum) {
  if (value <= maximum) {
    return {"value":value, "limited":false};
  }
  return {"value":maximum, "limited":true};
}

var o_x = rightLimit;
var o_y = 0;
function coords(e) {
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

  x -= display.offsetLeft;
  y -= display.offsetTop;

  return { "x":x, "y":y };
}

function coordsToPosition(coords) {
  return {
    "row": Math.floor(coords.y / (2 * gutterSize + gridSize)),
    "column": Math.floor((coords.x) / gridSize)
  };
}

var dragging = false;
function mouseDown(e) {
  switch (e.which) {
    case 3:
      alert("RIGHT CLICK");
      break;
    default:
      dragging = e;
      var c = coords(e);
      c.x -= o_x;
      selected = coordsToPosition(c);
  }
}

function mouseUp(e) {
  dragging = false;
}

function mouseMove(e) {
  if (dragging) {
    clearCanvas();
    clearOverlay();
    clearUnderlay();
    clearScale();
    var m_coords = coords(e);
    var d_coords = coords(dragging);
    o_x += m_coords.x - d_coords.x;
    if (o_x > 0) {
      o_x = 0;
    }

    if (Math.abs(o_x) > rightLimit) {
      o_x = -1 * rightLimit;
    }
    context.translate(o_x, o_y);
    scaleContext.translate(o_x, 0);
    drawGrid();
    dragging = e;
    $("#debug").text("Offset " + o_x + " " + rightLimit);
  }
  else {
    clearOverlay();
    overlayContext.translate(o_x, o_y);
    mouseOver(e);
  }
}

function mouseOver(e) {
  var c = coords(e);
  c.x -= o_x;
  var position = coordsToPosition(c);
  if (position.row >= 0 && position.row < maxRows && position.column >= 0 && position.column < maxColumns) {
    highlighted = position;
    drawOverlay();
  }
}

function initGraph() {
  initCanvas();
  createSession(function() {
    loadTestData();

    $(document).mouseup(mouseUp);
    $("#display").mousedown(mouseDown);
    $(document).mousemove(mouseMove);


    $("#display").bind("contextmenu", function(e) {
      return false;
    });
  });
}
