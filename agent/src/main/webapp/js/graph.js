var display;
var canvas;
var context;
var overlay;
var overlayContext;

function initCanvas()	{
	display = document.getElementById("display");

	canvas = document.getElementById("canvas");
	context = canvas.getContext("2d");
	overlay = document.getElementById("overlay");
	overlayContext = overlay.getContext("2d");
}

function clearCanvas()	{
	canvas.width = canvas.width;
}

function clearOverlay()	{
	overlay.width = overlay.width;
}

var data;
var maxColumns = 100;
var maxRows = 10;
var grid_size = 30;
var gutter_size = 20;
function loadData()	{
	data = [];
	for(var i = 0; i < maxRows; i++)	{
		var row = [];
		for(var j = 0; j < maxColumns; j++)	{
			row[j] = j % grid_size;
		}
		data[i] = row;
	} 
}

var selected;

function drawCircle(i, j, selected)	{
	var cell = coordsToPosition({"x":i, "y":j});
	if(cell.column < maxColumns && cell.row < maxRows)	{
		var cell_x = i + Math.floor(grid_size / 2);
		var cell_y = j + gutter_size + Math.floor(grid_size / 2);
		var size = limit(data[cell.row][cell.column], Math.floor((grid_size - 1) / 2));
		if(size.limited)	{
			context.lineWidth = 2;
		}
		else {
			context.lineWidth = 1;
		}
		if(selected != null && selected.row == cell.row && selected.column == cell.column)	{
			context.strokeStyle = "red";
		}
		else	{
			context.strokeStyle = "green";
		}
		context.beginPath();
		context.arc(cell_x, cell_y, size.value, 0, Math.PI * 2, false);
		context.closePath();
		context.stroke();
	}
}

var show_grid = false;
function drawGrid()	{
	var region_width = maxColumns * grid_size;
	if(show_grid)	{
		for(var x = 0.5; x < region_width; x += grid_size)	{
			context.moveTo(x, 0);
			context.lineTo(x, canvas.height);
		}
		for(var y = 0.5; y < canvas.height; y += (gutter_size + grid_size))	{
			context.moveTo(0, y);
			context.lineTo(region_width, y);
		}
		context.strokeStyle = "red";
		context.stroke();
	}

	for(var i = 0.5; i < region_width; i += grid_size)	{
		for(var j = 0.5; j < canvas.height; j += (gutter_size + grid_size))	{
			drawCircle(i, j, selected);
		}
	}
}

var highlighted;
function drawOverlay()	{
	if(highlighted != null && highlighted.column >= 0 && highlighted.row >= 0)	{
		var value = data[highlighted.row][highlighted.column];
		if(value > 0)	{
			var c_x = highlighted.column * grid_size;
			var c_y = (highlighted.row * (gutter_size + grid_size)) + gutter_size; + Math.floor(grid_size / 2);
			overlayContext.font = "bold 12px sans-serif";
			overlayContext.textBaseline = "bottom";
			var width = context.measureText("" + value).width;
			overlayContext.fillText(value, c_x + Math.floor(grid_size / 2) - Math.floor(width / 2), c_y);
		}
	}
}

function limit(value, maximum)	{
	if(value <= maximum)	{
		return {"value":value, "limited":false};
	}
	return {"value":maximum, "limited":true};
}

var o_x = 0;
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

function coordsToPosition(coords)	{
	return { 
		"row": Math.floor(coords.y / (gutter_size + grid_size)), 
		"column": Math.floor((coords.x) / grid_size)
	};
}

var dragging = false;
function mouseDown(e) {
	switch (e.which)	{
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
  if(dragging)  {
		clearCanvas();
		clearOverlay();
		var m_coords = coords(e);
		var d_coords = coords(dragging);
		o_x += m_coords.x - d_coords.x;
		context.translate(o_x, o_y);
		drawGrid();
		dragging = e;
	}
	else {
		clearOverlay();
		overlayContext.translate(o_x, o_y);
		mouseOver(e);
	}
}

function mouseOver(e)	{
	var c = coords(e);
	c.x -= o_x;
	var position = coordsToPosition(c);
	if(position.row >= 0 && position.row < maxRows && position.column >= 0 && position.column < maxColumns)	{
		highlighted = position;
		drawOverlay();
	}
}

function initGraph()	{
	loadData();
	initCanvas();
	drawGrid();

	$(document).mouseup(mouseUp);
	$("#display").mousedown(mouseDown);
	$(document).mousemove(mouseMove);
	

	$("#display").bind("contextmenu", function(e) {
		return false;
	});
}
