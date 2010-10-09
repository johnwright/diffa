/*
 * Copyright (C) 2010 LShift Ltd.
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

// Requirement: API_BASE must be set - see index.jsp for example

var HEATMAP_WIDTH = 900, // pixel width for heatmap viewport
	INTERVAL_MINS = 2, // the x-axis increments that difference events are bucketed into
	X_INCREMENTS = 360, // how many x-axis increments to fit into the HEATMAP_WIDTH. X_INCREMENTS * INTERVAL_MINS is the timeframe covered by the viewport; this is the default - it updates if the first poll that returns data shows data older than 12 hours
	IS_POLLING = false, // global tracker for whether polling is switched on
	COLOURS = {
		selected: "#FFF2CC", // lightyellow
		background: "#CFE2F3", // lightblue
		darkblue: "#0000FF"
	},
	POLL_SECS = 1, // how often the server is polled for new events - the client is intended to run with this interval
	POLL_INTERVAL = POLL_SECS*1000;

// enable testing from file URI's - this makes use of jquery.ajax.js, which is a patched verion of jQuery.ajax that allows cross-domain requests (it will not work in Chrome!)
if(document.location.protocol.indexOf("http") == -1) {
	API_BASE = "http://localhost:19093"+API_BASE;
}

function mapDiffaToRaphael(fdData, recalcXIncrements) {
	var data = [],	// this is all the blobs, where events within two minutes are grouped into a single blob and the value is the number of events
		axisx = [],	// this is the two-minute intervals from NOW back to the earliest event
		axisy = [];	// this is the unique list of pairKeys
	var pairKey,
		time,
		INTERVAL_MS = INTERVAL_MINS*60*1000,
		now = minTime = Date.now();
		
	if(!mapDiffaToRaphael.startTime) {
		mapDiffaToRaphael.startTime = now;
	} else {
		now += INTERVAL_MS - ((now - mapDiffaToRaphael.startTime) % INTERVAL_MS); // this keeps the blobs in the same clusters
	}

	fdData.sort(function(a, b) {
		return b.detectedAt - a.detectedAt;
	});
	
	// figure out the swimlanes
	$.each(fdData, function(i, event) {
		time = event.detectedAt;
		if(time<minTime) {
			minTime = time;
		}
		pairKey = event.objId.pairKey;
		if(axisy.indexOf(pairKey)===-1) {
			axisy.push(pairKey);
		}
	});

	// support rescaling the x-axis to fit all the data in
	if(recalcXIncrements) {
		if(mapDiffaToRaphael.defaultXIncrements) {
			X_INCREMENTS = mapDiffaToRaphael.defaultXIncrements;
		}
		if(minTime<now-INTERVAL_MS*X_INCREMENTS) {
			mapDiffaToRaphael.defaultXIncrements = X_INCREMENTS;
			X_INCREMENTS = ((now-minTime) / INTERVAL_MS);
			X_INCREMENTS += Math.ceil(10 / (HEATMAP_WIDTH / X_INCREMENTS)); // 10px room to breath for oldest data point
		}
	}
	// if minTime is not earlier than the limit created by X_INCREMENTS, we need to fill up the array so the graph is full width
	var limit = Math.min(minTime,now-INTERVAL_MS*X_INCREMENTS);
	for(var i=now; i>=limit; i-=INTERVAL_MS) {
		axisx.push(i);
	}
	axisx.push(i); // just to overlap beyond earliest event // TO-DO: figure out if this is necessary
	
	var index,
		currEvent,
		cluster,
		clusters = [];
	$.each(axisy, function(i, swimlane) {
		index=0;
		$.each(axisx, function(j, intervalBoundary) {
			cluster = [];
			while(currEvent = fdData[index]) {
				if(currEvent.detectedAt>=intervalBoundary) {
					index++;
					if(currEvent.objId.pairKey===swimlane) {
						cluster.push(currEvent);
					}
				} else {
					break;
				}
			}
			if(cluster.length>0) {
				clusters.push(cluster);
			}
			data.push(cluster.length);
		});
	});
	return {
		data: data.reverse(),
		clusters: clusters.reverse(),
		axisx: axisx.reverse(),
		axisy: axisy.reverse()
	}
}

function blankHeatmap(callback) {
	var width = HEATMAP_WIDTH,
		height = 300,
		leftgutter = 50,
		bottomgutter = 20,
		txt = {"font": '10px Fontin-Sans, Arial', stroke: "none", fill: "#000"};

	var $heatmapContainer = $('#heatmapContainer').width(width).height(height).css({
		'overflow': 'hidden',
		'position': 'relative'
	});
	$('<div id="heatmapBackground"></div>').width(width-leftgutter).height(height-bottomgutter).css({
		backgroundColor: COLOURS.background,
		position: 'absolute',
		right: '0'
	}).appendTo($heatmapContainer);
	var axisyPaperID = 'axisyPaper',
		axisxPaperID = 'axisxPaper';
	$("<div></div>").appendTo($heatmapContainer)
		.append($('<div id="'+axisxPaperID+'"></div>').css({
			'position': 'relative', // so we can move it and it gets a z-index for stacking
			'right': -leftgutter
		}))
		.append($('<div id="'+axisyPaperID+'"></div>').css({
			'position': 'absolute',
			'top': 0
		}));
	
	var axisyPaper = Raphael(axisyPaperID, leftgutter, height);
	axisyPaper.rect(0, 0, leftgutter, height).attr({
		fill: '#fff',
		stroke: 'none'
	});
	var axisxPaper = Raphael(axisxPaperID, width-leftgutter, height);

	startPolling.config = {
		axisyPaper: axisyPaper,
		axisxPaper: axisxPaper,
		width: width,
		height: height,
		leftgutter: leftgutter,
		bottomgutter: bottomgutter,
		txt: txt,
		COLOURS: COLOURS
	};
	
	callback();
}

function setupHeatmapConfig(raphael_data) {
	var data = raphael_data.data,
		clusters = raphael_data.clusters,
		axisx = raphael_data.axisx,
		axisy = raphael_data.axisy;

	var config = startPolling.config,
		axisxPaper = config.axisxPaper,
		width = axisxPaper.width,
		height = config.height,
		leftgutter = config.leftgutter,
		bottomgutter = config.bottomgutter,
		X = width / X_INCREMENTS;
		Y = (height - bottomgutter) / axisy.length,
		max = Math.round(Math.min(Y,X) / 2) - 1;
	if(max<=0) { // max can end up as -1, so we have to fix that
		// optional: we could set a flag to say we had to squeeze more blobs in that could really fit
		max = 1;
	}
	$.extend(config, {
		X: X,
		Y: Y,
		max: max,
		data: data,
		clusters: clusters,
		axisx: axisx,
		axisy: axisy
	});

	if(axisx.length>X_INCREMENTS) {
		axisxPaper.setSize(axisx.length*X);
		var overshot = (config.axisx.length-1-X_INCREMENTS)*X;
		$('#axisxPaper').css('right', overshot-config.leftgutter);
	}
	
	// addZoom(); here if you want to
}

function updateError(val) {
	$('#errorContainer').text(val+"s");
}

function stopPolling() {
	var timeout = startPolling.pollingTimeout;
	if(timeout) {
		clearTimeout(timeout);
	}
	IS_POLLING = false;
}

function startPolling() {
	if(IS_POLLING) {
		return false;
	}
	var poll = function() {
		var timeout = startPolling.pollingTimeout;
		if(timeout) {
			clearTimeout(timeout);
		}
		IS_POLLING = true;
		
		var sessionID = startPolling.config.sessionID,
			url = API_BASE+"/diffs/sessions/"+sessionID;
		var pollXHRCallback = function(data, status, xhr) {
			if(!data) {
				pollXHRError(xhr, status);
				return false;
			}
			var recalcXIncrements = false;
			if(!startPolling.config.blobs || !startPolling.config.blobs.length) {
				// support rescaling the x-axis to fit in more data
				recalcXIncrements = true;
			}
			if(!startPolling.etag) {
				startPolling.etag = "";
			}
			var etag = xhr.getResponseHeader('ETag');
			var raphael_data = mapDiffaToRaphael(data, recalcXIncrements);
			$(document).trigger('diffsLoaded', [{
				raphael_data: raphael_data,
				redraw: etag!==startPolling.etag,
				recalcXIncrements: recalcXIncrements
			}]);
			startPolling.etag = etag;
			startPolling.pollingTimeout = window.setTimeout(poll, POLL_INTERVAL);
		};
		var pollXHRError = function(xhr, status, ex) {
			IS_POLLING = false;
			updateError('error!');
			if(console && console.log) {
				var error = {
					url: url,
					status: status,
					exception: ex,
					xhr: xhr
				};
				console.log("error polling session "+sessionID+": ",error);
			}
		};
		if(sessionID) {
			$.ajax({
				url: url,
				dataType: 'json',
				success: pollXHRCallback,
				error: pollXHRError
			});
		}
	};
	poll();
}

function clearSwimLanes() {
	var config = startPolling.config,
		swimlanes = config.swimlanes;
	for(var i=0, il=swimlanes.length; i<il; i++) {
		swimlanes[i].remove();
	}
	config.swimlanes = [];
}

function drawSwimLanes() {
	var config = startPolling.config,
		axisyPaper = config.axisyPaper,
		axisxPaper = config.axisxPaper,
		axisy = config.axisy,
		height = config.height,
		width = axisxPaper.width,
		leftgutter = config.leftgutter,
		bottomgutter = config.bottomgutter,
		Y = config.Y,
		txt = config.txt,
		COLOURS = config.COLOURS,
		label,
		boundary;
	if(!config.swimlanes) {
		config.swimlanes = [];
	}
	clearSwimLanes();
	$.each(axisy, function(i, label) {
		/* Use something like this for highlighting swimlanes
		paper.rect(leftgutter+1, 1, width-2-leftgutter, (height-bottomgutter)/2-2, 0).attr({fill: COLOURS.selected, stroke: "none"}); */
		var laneHeight = Y*i;
		label = axisyPaper.text(20, Y * (i + .5), label).attr(txt);
		config.swimlanes.push(label);
		if(i>0) {
			boundary = axisxPaper.path("M "+0+" "+laneHeight+"L"+width+" "+laneHeight).attr({"stroke-dasharray": "--", stroke: "#000"});
		}
	});
}

function clearXAxis() {
	var config = startPolling.config,
		labels = config.xLabels;
	for(var i=0, il=labels.length; i<il; i++) {
		labels[i].remove();
	}
	config.xLabels = [];
}
/* NB: not working yet, see comment below
function nudgeXAxis() {
	var xDistance = (HEATMAP_WIDTH / X_INCREMENTS) * (POLL_SECS / (INTERVAL_MINS*60));
	// move axisxPaper over
	var config = startPolling.config;
	var $paper = $('#axisxPaper');
	var right = parseInt($paper.css('right'),10)+xDistance;
	right += 'px';
	$paper.css('right', right);
	// move NOW back same distance
	var now = config.xLabels[config.xLabels.length-1];
	var x = now.attr('x');
	//var width = config.axisxPaper.width;
	//now.attr('x', width+xDistance);
	config.axisxPaper.setSize(config.axisxPaper.width+xDistance);
	now.translate(xDistance); // not working out, I think because fractional pixels aren't being treated the same in SVG as in CSS
}*/

function drawXAxis() {
	var config = startPolling.config,
		paper = config.axisxPaper,
		height = config.height,
		leftgutter = config.leftgutter,
		bottomgutter = config.bottomgutter,
		axisx = config.axisx,
		X = config.X,
		txt = config.txt;
	if(!config.xLabels) {
		config.xLabels = [];
	}
	clearXAxis();
	
	var now = axisx[axisx.length-1];

	axisx = $.map(axisx, function(timestamp, i) {
		var label,
			d = new Date(timestamp),
			dmins = d.getMinutes();
		if(dmins<INTERVAL_MINS/2 || dmins>=60-INTERVAL_MINS/2) {
			if(dmins>=60-INTERVAL_MINS/2) {
				d.setHours(d.getHours()+1);
			}
			d.setMinutes(0);
			
			label = d.formatString("hh:0mm"); /* JRL: this will be misleading if the timeline goes back before midnight */
		} else {
			label = "";
		}
		if(i === axisx.length-1) {
			label = "NOW";
		}
		return label;
	});
	
	for (var i = 0, ii = axisx.length, label; i < ii; i++) {
		label = paper.text(X * i, height - bottomgutter + 10, axisx[i]).attr(txt).attr({
			'text-anchor': 'end'
		});
		config.xLabels.push(label);
	}
}

function clearBlobs() {
	var config = startPolling.config,
		paper = config.axisxPaper,
		blobs = config.blobs;
	for(var i=0, il=blobs.length; i<il; i++) {
		blobs[i].remove();
	}
	config.blobs = paper.set();
}

function drawBlobs() {
	var config = startPolling.config,
		paper = config.axisxPaper,
		data = config.data,
		axisx = config.axisx,
		axisy = config.axisy,
		width = config.width,
		leftgutter = config.leftgutter || 0,
		max = config.max || 5,
		X = config.X,
		Y = config.Y,
		o = 0;
	if(!config.blobs) {
		config.blobs = paper.set();
	}
	clearBlobs();
	var clusterCount = 0,
		clusters = config.clusters;
	for (var i = 0, ii = axisy.length; i < ii; i++) {
		for (var j = 0, jj = axisx.length; j < jj; j++) {
			var R,
				d = data[o];
			if(d<=0) {
				R = 0;
			} else if(d<=10) {
				R = Math.max(max*(d/10),5); // JRL: this '5' is a choice of min size for the blobs
			} else {
				//R = max;
				R = Math.max(max*(d/10),5); //JRL: max is not giving nice big sizes, so we're ignoring it for now. TO-DO: decide on a way to limit or appropriately adjust blob sizes
			}
			if (R) {
				
				var offset = (clusters[clusterCount][0].detectedAt-axisx[j])/(INTERVAL_MINS*60*1000);
				(function (dx, dy, R, value) {
					//var color = "hsb(" + [(1 - R / max) * .5, 1, .75] + ")";
					var color = "#FFF";
					var glow = paper.circle(dx, dy, 2*R).attr({stroke: "none", fill: config.COLOURS.darkblue, opacity: 0 });
					var dt = paper.circle(dx, dy, R).attr({stroke: "#000", fill: color});
					dt.cluster = clusters[clusterCount];
					clusters[clusterCount].dt = dt; // this to make it easy to get to the dt
					clusters[clusterCount++].glow = glow; // this to make it easy to get to the glow
					config.blobs.push(glow);
					config.blobs.push(dt);
					if(value>1) {
						if(value>5) {
							var lbl = paper.text(dx, dy, data[o])
							.attr({"font": '10px Fontin-Sans, Arial', stroke: "none", fill: "#00F"});
						} else {
							var lbl = paper.text(dx + R, dy - 10, data[o])
							.attr({"font": '10px Fontin-Sans, Arial', stroke: "none", fill: "#00F"});
						}
						config.blobs.push(lbl);
					}
					var dot = paper.circle(dx, dy, 2*R).attr({stroke: "none", fill: config.COLOURS.darkblue, opacity: 0});
					config.blobs.push(dot);
					$(dot[0]).hover(function() {
						$(document).trigger('blobHovered', {
							dt: dt,
							glow: glow
						});
					}, function() {
						$(document).trigger('blobUnHovered', {
							dt: dt,
							glow: glow
						});
					}).click(function() {
						$(document).trigger('blobSelected', {dt: dt});
					});
				})(X * j + X*offset, Y * (i + .5), R, d);
			}
			o++;
		}
	}
}

/* JRL: this is not right yet, don't use it
function addZoom() {
	var config = startPolling.config,
		paper = config.axisxPaper;
	$('#zoomin, #zoomout').click(function(event) {
		event.preventDefault();
		if(!paper.zoom) {
			paper.zoom = 1;
		}
		var direction = this.id === "zoomin" ? 1 : -1,
			data = config.data,
			axisx = config.axisx,
			axisy = config.axisy,		
			newData = [],
			newAxisx = [];

		if(direction>0) { // JRL: this supplies one zoomed-in level of zoom
			for(var i=0; i<data.length; i=i+2) {
				newData.push(data[i]+data[i+1]);
				if(typeof axisx[i]!=="undefined") {
					newAxisx.push(axisx[i]);
				}
			}
			paper.origData = data;
			config.data = newData;
			paper.origAxisx = axisx;
			config.axisx = newAxisx;
		} else {
			config.data = paper.origData;
			config.axisx = paper.origAxisx;
		}
		drawXAxis();
		drawBlobs();
		return false;
	});
}*/

function addDiffRow($difflist, event, cluster) {
	var config = startPolling.config;
	var findDiffType = function(upstream, downstream) {
		if(!upstream) {
			return "Missing from upstream";
		} else if(!downstream) {
			return "Missing from downstream";
		} else {
			return "Data difference";
		}
	};
	var detectedAtRaw = event.detectedAt,
		detectedAt = new Date(event.detectedAt),
		date = detectedAt.formatString("DD/MM/YYYY"),
		time = detectedAt.formatString("0hh:0mm:0ss"),
		pairing = event.objId.pairKey,
		group = config.groups[pairing],
		itemID = event.objId.id,
		diffType = findDiffType(event.upstreamVsn,event.downstreamVsn);
	var circle = cluster.dt,
		glow = cluster.glow;
	var $rows = $difflist.find('tbody tr').filter(function() {
		return $(this).find('td').eq(0).text();
	});
	var currentDate = $rows
		.eq($rows.length-1)
		.find('td')
		.eq(0)
		.text();
	$('<tr></tr>')
		.append('<td class="date">'+(currentDate===date ? "" : date)+'</td>')
		.append('<td>'+time+'</td>')
		.append('<td id="'+detectedAtRaw+'_'+pairing+'_group">'+(group||"")+'</td>')
		.append('<td>'+pairing+'</td>')
		.append('<td>'+itemID+'</td>')
		.append('<td>'+diffType+'</td>')
		.data('event', event)
		.data('circle', circle)
		.data('glow', glow)
		.appendTo($difflist);
	// now go get the group if we don't have it
	if(!group) {
		$.ajax({
			url: API_BASE+'/config/pairs/'+pairing,
			success: function(data) {
				group = config.groups[pairing] = data.group.key;
				$('#'+detectedAtRaw+'_'+pairing+'_group').text(group);
			}
		});
	}
}

function updateDiffList() {
	var config = startPolling.config,
		clusters = config.clusters;
	if(!clusters) {
		return;
	}
	if(!config.groups) {
		config.groups = {};
	}
	clusters.sort(function(a, b) {
		return b[0].detectedAt - a[0].detectedAt;
	});
	var $difflist = $('#difflist').find('tbody').empty().end();
	$.each(clusters, function(i, cluster) {
		$.each(cluster, function(j, event) {
			addDiffRow($difflist, event, cluster);
		});
	});
}

function highlightSelectedBlob(blob) {
	var config = startPolling.config,
		paper = config.axisxPaper,
		selectedBlob = paper.selectedBlob;
	if(selectedBlob) {
		selectedBlob.attr("fill", "#FFF");
		paper.selectedBlob = null;
	}
	if(blob) {
		blob.attr("fill", config.COLOURS.selected);
		paper.selectedBlob = blob;
	}
}

function highlightDiffListRows(circle, persist) {
	var $rows = $("#difflist").find("tbody tr").filter(function() {
		return $(this).data('circle')===circle;
	});
	if($rows.length) {
		$rows
			.siblings()
			.removeClass(persist ? "selected highlighted" : "highlighted")
			.end()
			.removeClass("highlighted")
			.addClass(persist ? "selected" : "highlighted");
	} else {
		$('#difflist').find('tbody tr').removeClass('highlighted');
	}
}

function showContent(circle, diffEvent, loadContent) {
	if(!circle) {
		if(!diffListSelect.selected) { // reset content box
			$('#contentviewer h6').eq(0).text('No item selected');
			$('#item1 .diffHash').html('<span>item 1</span>')
			$('#item1 pre').empty();
			$('#item2 .diffHash').html('<span>item 2</span>');
			$('#item2 pre').empty();
			return;
		} else {
			circle = diffListSelect.selected.dt;
			diffEvent = diffListSelect.selected.diffEvent;
			loadContent = true;
		}
	}

	if(!diffEvent) { // TO-DO: rewind this stack and see if we can do without diffEvent, since it might always be present on circle
		diffEvent = circle.cluster[0]; // diffEvent as a parameter comes from a clicking a specific row of the diffList
	}
	
	var itemID = diffEvent.objId.id,
		pairKey = diffEvent.objId.pairKey,
		seqID = diffEvent.seqId,
		upstreamLabel = "upstream",
		upstreamVersion = diffEvent.upstreamVsn || "no version",
		downstreamLabel = "downstream",
		downstreamVersion = diffEvent.downstreamVsn || "no version";
	
	$('#contentviewer h6').eq(0).text('Content for item ID: '+itemID);
	$('#item1 .diffHash')
		.html('<span>'+upstreamLabel+'</span>'+upstreamVersion);
	$('#item2 .diffHash')
		.html('<span>'+downstreamLabel+'</span>'+downstreamVersion);

	
	var getContent = function(selector,label) {
		var sessionID = startPolling.config.sessionID;
		$.ajax({
			url: API_BASE+'/diffs/events/'+sessionID+'/'+seqID,
			success: function(data) {
				$(selector).text(data||"no content found"); // TO-DO: separate out upstream and downstream detail when server supports it
			},
			error: function(xhr, status, ex) {
				if(console && console.log) {
					console.log('error getting the content for '+(label||"(no label)"), status, ex, xhr);
				}
			}
		});
	};

	// go get the real values for the upstream and downstream labels and the content
	$.ajax({
		method: "GET",
		url: API_BASE+"/config/pairs/"+pairKey,
		success: function(data, status, xhr) {
			upstreamLabel = data.upstream.name;
			$('#item1 h6').text(upstreamLabel);
			downstreamLabel = data.downstream.name;
			$('#item2 h6').text(downstreamLabel);
			// go get the content for upstream and downstream
			if(loadContent) {
				getContent("#item1 pre",upstreamLabel);
				getContent("#item2 pre",downstreamLabel);
			}			
		},
		error: function(xhr, status, ex) {
			if(console && console.log) {
				console.log('error getting the participant labels for '+pairKey, status, ex, xhr);
			}
		}
	});
	
	var $actionListContainer = $('#actionlist').empty();
	var actionListCallback = function(actionList, status, xhr) {
		if(!actionList) {
			return;
		}
		var $repairStatus = $('#repairstatus');
		$.each(actionList, function(i, action) {
		
			$("<label>"+action.name+"</label>").appendTo($actionListContainer);
			$('<button class="repair">Go</button>')
				.click(function(e) {
					e.preventDefault();
					var $button = $(this),
						url = API_BASE+action.action.replace("${id}", itemID);
					if($button.hasClass('disabled')) {
						return false;
					}
					$button.addClass('disabled');
					$repairStatus.text('Repairing...');
					$.ajax({
						type: action.method,
						url: url,
						success: function(data, status, xhr) {
							$repairStatus.html('Repair status: '+data.result+'<br/>output: '+data.output);
						},
						error: function(xhr, status, ex) {
							if(console && console.log) {
								var error = {
									type: action.method,
									url: url,
									status: status,
									exception: ex,
									xhr: xhr
								};
								console.log("error during repair for item "+itemID+": ",error);
							}
							$repairStatus.text('Error during repair: '+(status||ex.message));
						},
						complete: function() {
							$button.removeClass('disabled');
						}
					});
					return false;
				})
				.appendTo($actionListContainer);
			$('<br class="clearboth"/>').appendTo($actionListContainer);
		});
	};
	if(loadContent) {
		$.ajax({
			url: API_BASE+'/actions/'+pairKey,
			success: actionListCallback
		});
	}	
}

function createSession() {
	var handleSessionId = function(data, status, req) {
		var location = req.getResponseHeader('Location');
		var parts = location.split("/");
		var sessionID = parts[parts.length - 1];
		startPolling.config.sessionID = sessionID;
		startPolling();
	};
	$.post(API_BASE + '/diffs/sessions', {}, handleSessionId, "json");
}

function scrollHeatmapTo(pct) {
	var config = startPolling.config,
		axisxWidth = config.axisxPaper.width,
		viewportWidth = HEATMAP_WIDTH - config.leftgutter,
		scale = pct/100;
		
	// 100% means scrolled as far right as possible i.e. NOW is showing
	// 0% means scrolled as far left as possible i.e. earliest event is showing
	if(axisxWidth>viewportWidth) {
		var overshot = (config.axisx.length-1-X_INCREMENTS)*config.X;
		$('#axisxPaper').css({
			'right': overshot*scale-config.leftgutter
		});	
	}
}

function diffListSelect(e) {
	if(!e) {
		return diffListSelect.selected || {};
	}
	var $diffRow = e.target.nodeName==="tr" ? $(e.target) : $(e.target).closest('tr'),
		diffEvent = $diffRow.data('event'),
		dt = $diffRow.data('circle'),
		glow = $diffRow.data('glow');
	return {
		dt: dt,
		glow: glow,
		diffEvent: diffEvent // provide the event that was clicked, because the table can show more than one event per row
	}
}

$(function () {
	
	$('#livebutton').click(function(e) {
		e.preventDefault();
		if(IS_POLLING) {
			$('#livebutton').addClass('disabled');
			stopPolling();
		} else {
			$('#livebutton').removeClass('disabled');
			startPolling();
		}
		return false;
	});
	
	// set up click handlers to fire custom events
	$('#difflist').click(function(e) {
		// select
		var eData = diffListSelect(e);
		$(document).trigger('blobSelected', eData);
		return false;
	});
	$('#difflist tbody tr').live('mouseover', function(e) {
		// hover
		var eData = diffListSelect(e);
		$(document).trigger('blobHovered', eData);
	}).live('mouseout', function() {
		// unhover = select prev selected
		var eData = diffListSelect.hovered;
		$(document).trigger('blobUnHovered', eData);
	});
	
	// bind to custom events
	$(document).bind('diffsLoaded', function(e, params) {
		if(params && params.redraw) {
			var selectedRow = diffListSelect.selected,
				selectedEvent,
				rowToClick;
			setupHeatmapConfig(params.raphael_data);
			drawSwimLanes();
			drawXAxis();
			drawBlobs();
			updateDiffList();
			if(selectedRow) {
				selectedEvent = selectedRow.diffEvent;
				var clusters = startPolling.config.clusters,
					matched = false;
				$.each(clusters, function(i, cluster) {
					if(matched) {
						return false;
					}
					$.each(cluster, function(j, event) {
						if(event.seqId === selectedEvent.seqId) {
							$(document).trigger('blobSelected', {dt: cluster.dt});
							return false;
						} 
					});
				});
			}
			
		} else {
			updateError('last check for updates: '+(new Date).formatString('0hh:0mm:0ss'));
		}		
	});
	
	$(document).bind('blobSelected', function(e, params) {
		if(!params.diffEvent) {
			params.diffEvent = params.dt.cluster[0];
		}
		diffListSelect.selected = params;
		var circle = params.dt;
		highlightSelectedBlob(circle);
		highlightDiffListRows(circle, "persist");
		showContent(circle, params.diffEvent, true);
	});
	
	$(document).bind('blobHovered', function(e, params) {
		diffListSelect.hovered = params;
		var glow = params.glow,
			circle = params.dt;
		if(glow) {
			glow.attr({
				fill: "r"+COLOURS.darkblue+"-"+COLOURS.background,
				'opacity': 0.5
			});
		}
		highlightDiffListRows(circle);
		showContent(circle, params.diffEvent, false);
	});
	
	$(document).bind('blobUnHovered', function(e, params) {
		diffListSelect.hovered = null;
		var glow = params.glow,
			circle = params.dt;
		if(glow) {
			glow.attr({
				fill: "config.COLOURS.darkblue",
				opacity: 0
			});
		}
		highlightDiffListRows();
		showContent();
	});
	
	$('#scrollBar').slider({
		'value':'100',
		slide: function(event, ui) {
			scrollHeatmapTo(ui.value);
		}
	});

	/* JRL: uncomment this chunk if you want to have a session ID box you can use to change the session being polled
	
	var $sessionID = $('#sessionID'),
		$label = $sessionID
			.siblings() // this is label
			.css('backgroundColor', '#FFF');	
	var flash = function($elem,times) {
		if(times===0) {
			return;
		}
		$elem.animate({
				backgroundColor: COLOURS.selected
			}, "slow", function() {
				$label.animate({
					backgroundColor: '#FFF'
				}, "slow", function() {
					flash($elem,times-1);
				});
			});
	};
	
	$sessionID.bind('keyup', function() {
		if(this.value.length > 2) {
			if($sessionID.timeout) {
				clearTimeout($sessionID.timeout);
			}
			$sessionID.timeout = setTimeout(function() {
				startPolling();
			}, 500);
		} else {
			if($sessionID.timeout) {
				clearTimeout($sessionID.timeout);
			}
		}
	});

	if(!$sessionID.val()) {
		flash($label,3);
		createSession();
	} else {
		startPolling();
	}*/
	
	blankHeatmap(createSession);
	
});

/* Date utils - from TiddlyWiki */
// Substitute date components into a string
DATE_STRINGS = {
	am: "am",
	pm: "pm",
	daySuffixes: ["st", "nd", "rd", "th", "th", "th", "th", "th", "th", "th", "th", "th", "th", "th", "th", "th", "th", "th", "th", "th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th", "th", "st"],
	days: ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
	months: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
	shortDays: ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
	shortMonths: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
};

Date.prototype.formatString = function(template)
{
	var t = template.replace(/0hh12/g,String.zeroPad(this.getHours12(),2));
	t = t.replace(/hh12/g,this.getHours12());
	t = t.replace(/0hh/g,String.zeroPad(this.getHours(),2));
	t = t.replace(/hh/g,this.getHours());
	t = t.replace(/mmm/g,DATE_STRINGS.shortMonths[this.getMonth()]);
	t = t.replace(/0mm/g,String.zeroPad(this.getMinutes(),2));
	t = t.replace(/mm/g,this.getMinutes());
	t = t.replace(/0ss/g,String.zeroPad(this.getSeconds(),2));
	t = t.replace(/ss/g,this.getSeconds());
	t = t.replace(/[ap]m/g,this.getAmPm().toLowerCase());
	t = t.replace(/[AP]M/g,this.getAmPm().toUpperCase());
	t = t.replace(/wYYYY/g,this.getYearForWeekNo());
	t = t.replace(/wYY/g,String.zeroPad(this.getYearForWeekNo()-2000,2));
	t = t.replace(/YYYY/g,this.getFullYear());
	t = t.replace(/YY/g,String.zeroPad(this.getFullYear()-2000,2));
	t = t.replace(/MMM/g,DATE_STRINGS.months[this.getMonth()]);
	t = t.replace(/0MM/g,String.zeroPad(this.getMonth()+1,2));
	t = t.replace(/MM/g,this.getMonth()+1);
	t = t.replace(/0WW/g,String.zeroPad(this.getWeek(),2));
	t = t.replace(/WW/g,this.getWeek());
	t = t.replace(/DDD/g,DATE_STRINGS.days[this.getDay()]);
	t = t.replace(/ddd/g,DATE_STRINGS.shortDays[this.getDay()]);
	t = t.replace(/0DD/g,String.zeroPad(this.getDate(),2));
	t = t.replace(/DDth/g,this.getDate()+this.daySuffix());
	t = t.replace(/DD/g,this.getDate());
	var tz = this.getTimezoneOffset();
	var atz = Math.abs(tz);
	t = t.replace(/TZD/g,(tz < 0 ? '+' : '-') + String.zeroPad(Math.floor(atz / 60),2) + ':' + String.zeroPad(atz % 60,2));
	t = t.replace(/\\/g,"");
	return t;
};

Date.prototype.getWeek = function()
{
	var dt = new Date(this.getTime());
	var d = dt.getDay();
	if(d==0) d=7;// JavaScript Sun=0, ISO Sun=7
	dt.setTime(dt.getTime()+(4-d)*86400000);// shift day to Thurs of same week to calculate weekNo
	var n = Math.floor((dt.getTime()-new Date(dt.getFullYear(),0,1)+3600000)/86400000);
	return Math.floor(n/7)+1;
};

Date.prototype.getYearForWeekNo = function()
{
	var dt = new Date(this.getTime());
	var d = dt.getDay();
	if(d==0) d=7;// JavaScript Sun=0, ISO Sun=7
	dt.setTime(dt.getTime()+(4-d)*86400000);// shift day to Thurs of same week
	return dt.getFullYear();
};

Date.prototype.getHours12 = function()
{
	var h = this.getHours();
	return h > 12 ? h-12 : ( h > 0 ? h : 12 );
};

Date.prototype.getAmPm = function()
{
	return this.getHours() >= 12 ? DATE_STRINGS.pm : DATE_STRINGS.am;
};

Date.prototype.daySuffix = function()
{
	return DATE_STRINGS.daySuffixes[this.getDate()-1];
};

// Static method to left-pad a string with 0s to a certain width
String.zeroPad = function(n,d)
{
	var s = n.toString();
	if(s.length < d)
		s = "000000000000000000000000000".substr(0,d-s.length) + s;
	return s;
};
