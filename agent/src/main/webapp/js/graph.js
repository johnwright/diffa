var display;
var canvas;
var context;
var overlay;
var overlayContext;
var underlay;
var underlayContext;
var scale;
var scaleContext;
var buckets = [];
var maxColumns = 96;
var maxRows = 10;
var gridSize = 30;
var gutterSize = 14;

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

function createSession() {
	var handleSessionId = function(data, status, req) {
		var location = req.getResponseHeader('Location');
		var parts = location.split("/");
		var sessionID = parts[parts.length - 1];
		sessionId = sessionID;
	};
	$.post(API_BASE + '/diffs/sessions', {}, handleSessionId, "json");
}

var startTime, endTime;
var bucketSize = 3600;

const TIME_FORMAT = "YYYY0MM0DDT0hh0mm0ssZ";
var swimlaneLabels = [];
function loadBuckets() {
	buckets = [];
	for (var i = 0; i < maxRows; i++) {
		var row = [];
		for (var j = 0; j < maxColumns; j++) {
			row[j] = 0;
		}
		buckets[i] = row;
	}

	endTime = new Date();

	var now = endTime.formatString(TIME_FORMAT);

	startTime = new Date(endTime - (3600 * maxColumns * 1000));
	var dayBeforeNow = startTime.formatString(TIME_FORMAT);

	$.get("rest/diffs/sessions/" + sessionId + "/zoom?range-start=" + dayBeforeNow + "&range-end=" + now + "&bucketing=3600", function(data) {
		var indexer = 0;

		for (var pair in data) {
			swimlaneLabels[indexer] = pair;
			for (var x = 0; x < data[pair].length; x++) {
				buckets[indexer][x] = data[pair][x];
			}
			indexer++;
		}
		clearEverything();
		o_x = -1 * rightLimit;
		context.translate(o_x, o_y);
		scaleContext.translate(o_x, o_y);
		drawGrid();
	});
}

function renderEvents(event) {
	if(event != null)	{
		var itemID = event.objId.id,
			pairKey = event.objId.pairKey,
			seqID = event.seqId,
			upstreamLabel = "upstream",
			upstreamVersion = event.upstreamVsn || "no version",
			downstreamLabel = "downstream",
			downstreamVersion = event.downstreamVsn || "no version";

		$('#contentviewer h6').eq(0).text('Content for item ID: ' + itemID);
		$('#item1 .diffHash').html('<span>' + upstreamLabel + '</span>' + upstreamVersion);
		$('#item2 .diffHash').html('<span>' + downstreamLabel + '</span>' + downstreamVersion);
	}
}

function selectFromList(event) {
	if (!event) {
		return false;
	}
	var row = event.target.nodeName==="tr" ? $(event.target) : $(event.target).closest('tr');
	renderEvents(row.data("event"));
}

function addRow(table, event) {
	var time = new Date(event.detectedAt).formatString("0hh:0mm:0ss");
	var date = new Date(event.detectedAt).formatString("DD/MM/YYYY");
	var row = $("<tr id='evt_" + event.seqId + "></tr>")
		.append("<td class='date'>" + date + "</td>")
		.append("<td>" + time + "</td>")
		.append("<td id='" + event.detectedAt + "_" + event.objId.pairKey + "_group'></td>")
		.append("<td>" + event.objId.pairKey + "</td>")
		.append("<td>" + event.objId.id + "</td>")
		.data("event", event);

	if (!event.upstreamVsn) {
		row.append("<td>Missing from upstream</td>");
	}
	else if (!event.downstreamVsn) {
		row.append("<td>Missing from downstream</td>");
	}
	else {
		row.append("<td>Data difference</td>");
	}

	table.append(row);
}

function previous()	{
	fetchData();
}

function next()	{
	fetchData();
}

function fetchData() {
	if(selected != null)	{
		if(buckets[selected.row][selected.column] > 0)	{
			var selectedStart = new Date(startTime.getTime() + (selected.column * bucketSize * 1000));
			var selectedEnd = new Date(selectedStart.getTime() + (bucketSize * 1000));

			$.get("rest/diffs/sessions/" + sessionId, function(data) {
				renderEvents(data[0]);
				var list = $('#difflist').find('tbody').empty().end();
				$.each(data, function(i, event) {
					addRow(list, event);
				});
			});
		}
	}
}

var timeout;
var polling = true;
function startPolling() {
	polling = true;
	clearTimeout(timeout);
	loadBuckets();
	timeout = window.setTimeout(startPolling, 5000);
	$("#polling").text("Stop polling");
}

function stopPolling() {
	polling = false;
	clearTimeout(timeout);
	$("#polling").text("Start polling");
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
		context.strokeStyle = "black";
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
		dashedLine(underlayContext, 0, s, canvas.width, s, 2);
		if (swimlaneLabels[lane] != null) {
			underlayContext.font = "11px serif";
			underlayContext.fillText(swimlaneLabels[lane], 10, s - 3);
		}
		lane++;
	}


	for (var i = 0.5; i < region_width; i += gridSize) {
		for (var j = 0.5; j < canvas.height; j += (2 * gutterSize + gridSize)) {
			drawCircle(i, j);
		}
	}

	scaleContext.font = "9px sans-serif";
	for (var sc = 0; sc < maxColumns; sc++) {
		if (sc % 3 == 0) {
			var tick = new Date(startTime.getTime() + (sc * bucketSize * 1000));
			scaleContext.fillText(tick.formatString("0DD/0MM"), sc * gridSize, 10);
			scaleContext.fillText(tick.formatString("0hh:0mm"), sc * gridSize, 20);
		}
	}
}

var highlighted;
function drawOverlay() {
	if (highlighted != null && highlighted.column >= 0 && highlighted.row >= 0) {
		var value = buckets[highlighted.row][highlighted.column];
		if (value > 0) {
			var c_x = highlighted.column * gridSize;
			var c_y = (highlighted.row * (2 * gutterSize + gridSize)) + gutterSize;
			overlayContext.font = "12px sans-serif";
			overlayContext.textBaseline = "bottom";
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
			stopPolling();
			polling = false;
			var c = coords(e);
			c.x -= o_x;
			selected = coordsToPosition(c);
			fetchData();
	}
}

function mouseUp(e) {
	dragging = false;
	polling = true;
}

function clearEverything() {
	clearCanvas();
	clearOverlay();
	clearUnderlay();
	clearScale();
}

function mouseMove(e) {
	if (dragging) {
		clearEverything()
		var m_coords = coords(e);
		var d_coords = coords(dragging);
		o_x += m_coords.x - d_coords.x;
		if (o_x > 0) {
			o_x = 0;
			//alert("This application can only show 7 days worth of data.\n\nThis restriction could be removed if you raise a change request with the Diffa development team.")
		}

		if (Math.abs(o_x) > rightLimit) {
			o_x = -1 * rightLimit;
		}
		context.translate(o_x, o_y);
		scaleContext.translate(o_x, 0);
		drawGrid();
		dragging = e;
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
	createSession();
	initCanvas();
	startPolling();

	$(document).mouseup(mouseUp);
	$("#display").mousedown(mouseDown);
	$(document).mousemove(mouseMove);


	$("#display").bind("contextmenu", function(e) {
		return false;
	});
	$("#difflist").click(function(e) {
		selectFromList(e);
	});

	$("#next").hover(
		function(e) {
			$(this).css({"color":"red"});
		},
		function(e) {
			$(this).css({"color":"black"});
	});
	$("#next").click(function(e) {
		next();
	});

	$("#previous").hover(
		function(e) {
			$(this).css({color:"red"});
		},
		function(e) {
			$(this).css({color:"black"});
	});
	$("#previous").click(function(e) {
		previous();
	});




	$("#polling").toggle(
		function() {
			if (polling) {
				stopPolling();
			}
		},
		function() {
			startPolling();
		}
	);

}
