jQuery.ajax = function( origSettings ) {
	var s = jQuery.extend(true, {}, jQuery.ajaxSettings, origSettings);
	
	var jsonp, status, data,
		callbackContext = origSettings && origSettings.context || s,
		type = s.type.toUpperCase();

	// convert data if not already a string
	if ( s.data && s.processData && typeof s.data !== "string" ) {
		s.data = jQuery.param( s.data, s.traditional );
	}

	// Handle JSONP Parameter Callbacks
	if ( s.dataType === "jsonp" ) {
		if ( type === "GET" ) {
			if ( !jsre.test( s.url ) ) {
				s.url += (rquery.test( s.url ) ? "&" : "?") + (s.jsonp || "callback") + "=?";
			}
		} else if ( !s.data || !jsre.test(s.data) ) {
			s.data = (s.data ? s.data + "&" : "") + (s.jsonp || "callback") + "=?";
		}
		s.dataType = "json";
	}

	// Build temporary JSONP function
	if ( s.dataType === "json" && (s.data && jsre.test(s.data) || jsre.test(s.url)) ) {
		jsonp = s.jsonpCallback || ("jsonp" + jsc++);

		// Replace the =? sequence both in the query string and the data
		if ( s.data ) {
			s.data = (s.data + "").replace(jsre, "=" + jsonp + "$1");
		}

		s.url = s.url.replace(jsre, "=" + jsonp + "$1");

		// We need to make sure
		// that a JSONP style response is executed properly
		s.dataType = "script";

		// Handle JSONP-style loading
		window[ jsonp ] = window[ jsonp ] || function( tmp ) {
			data = tmp;
			success();
			complete();
			// Garbage collect
			window[ jsonp ] = undefined;

			try {
				delete window[ jsonp ];
			} catch(e) {}

			if ( head ) {
				head.removeChild( script );
			}
		};
	}

	if ( s.dataType === "script" && s.cache === null ) {
		s.cache = false;
	}

	if ( s.cache === false && type === "GET" ) {
		var ts = now();

		// try replacing _= if it is there
		var ret = s.url.replace(rts, "$1_=" + ts + "$2");

		// if nothing was replaced, add timestamp to the end
		s.url = ret + ((ret === s.url) ? (rquery.test(s.url) ? "&" : "?") + "_=" + ts : "");
	}

	// If data is available, append data to url for get requests
	if ( s.data && type === "GET" ) {
		s.url += (rquery.test(s.url) ? "&" : "?") + s.data;
	}

	// Watch for a new set of requests
	if ( s.global && ! jQuery.active++ ) {
		jQuery.event.trigger( "ajaxStart" );
	}

	// Matches an absolute URL, and saves the domain
	var parts = rurl.exec( s.url ),
		remote = parts && (parts[1] && parts[1] !== location.protocol || parts[2] !== location.host);

	// If we're requesting a remote document
	// and trying to load JSON or Script with a GET
	if ( s.dataType === "script" && type === "GET" && remote ) {
		var head = document.getElementsByTagName("head")[0] || document.documentElement;
		var script = document.createElement("script");
		script.src = s.url;
		if ( s.scriptCharset ) {
			script.charset = s.scriptCharset;
		}

		// Handle Script loading
		if ( !jsonp ) {
			var done = false;

			// Attach handlers for all browsers
			script.onload = script.onreadystatechange = function() {
				if ( !done && (!this.readyState ||
						this.readyState === "loaded" || this.readyState === "complete") ) {
					done = true;
					success();
					complete();

					// Handle memory leak in IE
					script.onload = script.onreadystatechange = null;
					if ( head && script.parentNode ) {
						head.removeChild( script );
					}
				}
			};
		}

		// Use insertBefore instead of appendChild  to circumvent an IE6 bug.
		// This arises when a base node is used (#2709 and #4378).
		head.insertBefore( script, head.firstChild );

		// We handle everything using the script element injection
		return undefined;
	}

	var requestDone = false;

	// Create the request object
	var xhr = s.xhr();

	if ( !xhr ) {
		return;
	}

	// Open the socket
	// Passing null username, generates a login popup on Opera (#2865)
	// JRL: testing local XHR
	if(window.Components && window.netscape && window.netscape.security && document.location.protocol.indexOf("http") == -1) {
		window.netscape.security.PrivilegeManager.enablePrivilege("UniversalBrowserRead");
	}
	if ( s.username ) {
		xhr.open(type, s.url, s.async, s.username, s.password);
	} else {
		xhr.open(type, s.url, s.async);
	}

	// Need an extra try/catch for cross domain requests in Firefox 3
	try {
		// Set the correct header, if data is being sent
		if ( s.data || origSettings && origSettings.contentType ) {
			xhr.setRequestHeader("Content-Type", s.contentType);
		}

		// Set the If-Modified-Since and/or If-None-Match header, if in ifModified mode.
		if ( s.ifModified ) {
			if ( jQuery.lastModified[s.url] ) {
				xhr.setRequestHeader("If-Modified-Since", jQuery.lastModified[s.url]);
			}

			if ( jQuery.etag[s.url] ) {
				xhr.setRequestHeader("If-None-Match", jQuery.etag[s.url]);
			}
		}

		// Set header so the called script knows that it's an XMLHttpRequest
		// Only send the header if it's not a remote XHR
		if ( !remote ) {
			xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
		}

		// Set the Accepts header for the server, depending on the dataType
		xhr.setRequestHeader("Accept", s.dataType && s.accepts[ s.dataType ] ?
			s.accepts[ s.dataType ] + ", */*" :
			s.accepts._default );
	} catch(e) {}

	// Allow custom headers/mimetypes and early abort
	if ( s.beforeSend && s.beforeSend.call(callbackContext, xhr, s) === false ) {
		// Handle the global AJAX counter
		if ( s.global && ! --jQuery.active ) {
			jQuery.event.trigger( "ajaxStop" );
		}

		// close opended socket
		xhr.abort();
		return false;
	}

	if ( s.global ) {
		trigger("ajaxSend", [xhr, s]);
	}

	// Wait for a response to come back
	var onreadystatechange = xhr.onreadystatechange = function( isTimeout ) {
		// The request was aborted
		if ( !xhr || xhr.readyState === 0 || isTimeout === "abort" ) {
			// Opera doesn't call onreadystatechange before this point
			// so we simulate the call
			if ( !requestDone ) {
				complete();
			}

			requestDone = true;
			if ( xhr ) {
				xhr.onreadystatechange = jQuery.noop;
			}

		// The transfer is complete and the data is available, or the request timed out
		} else if ( !requestDone && xhr && (xhr.readyState === 4 || isTimeout === "timeout") ) {
			requestDone = true;
			xhr.onreadystatechange = jQuery.noop;

			status = isTimeout === "timeout" ?
				"timeout" :
				!jQuery.httpSuccess( xhr ) ?
					"error" :
					s.ifModified && jQuery.httpNotModified( xhr, s.url ) ?
						"notmodified" :
						"success";

			var errMsg;

			if ( status === "success" ) {
				// Watch for, and catch, XML document parse errors
				try {
					// process the data (runs the xml through httpData regardless of callback)
					data = jQuery.httpData( xhr, s.dataType, s );
				} catch(err) {
					status = "parsererror";
					errMsg = err;
				}
			}

			// Make sure that the request was successful or notmodified
			if ( status === "success" || status === "notmodified" ) {
				// JSONP handles its own success callback
				if ( !jsonp ) {
					success();
				}
			} else {
				jQuery.handleError(s, xhr, status, errMsg);
			}

			// Fire the complete handlers
			complete();

			if ( isTimeout === "timeout" ) {
				xhr.abort();
			}

			// Stop memory leaks
			if ( s.async ) {
				xhr = null;
			}
		}
	};

	// Override the abort handler, if we can (IE doesn't allow it, but that's OK)
	// Opera doesn't fire onreadystatechange at all on abort
	try {
		var oldAbort = xhr.abort;
		xhr.abort = function() {
			if ( xhr ) {
				oldAbort.call( xhr );
			}

			onreadystatechange( "abort" );
		};
	} catch(e) { }

	// Timeout checker
	if ( s.async && s.timeout > 0 ) {
		setTimeout(function() {
			// Check to see if the request is still happening
			if ( xhr && !requestDone ) {
				onreadystatechange( "timeout" );
			}
		}, s.timeout);
	}

	// Send the data
	try {
		xhr.send( type === "POST" || type === "PUT" || type === "DELETE" ? s.data : null );
	} catch(e) {
		jQuery.handleError(s, xhr, null, e);
		// Fire the complete handlers
		complete();
	}

	// firefox 1.5 doesn't fire statechange for sync requests
	if ( !s.async ) {
		onreadystatechange();
	}

	function success() {
		// If a local callback was specified, fire it and pass it the data
		if ( s.success ) {
			s.success.call( callbackContext, data, status, xhr );
		}

		// Fire the global callback
		if ( s.global ) {
			trigger( "ajaxSuccess", [xhr, s] );
		}
	}

	function complete() {
		// Process result
		if ( s.complete ) {
			s.complete.call( callbackContext, xhr, status);
		}

		// The request was completed
		if ( s.global ) {
			trigger( "ajaxComplete", [xhr, s] );
		}

		// Handle the global AJAX counter
		if ( s.global && ! --jQuery.active ) {
			jQuery.event.trigger( "ajaxStop" );
		}
	}
	
	function trigger(type, args) {
		(s.context ? jQuery(s.context) : jQuery.event).trigger(type, args);
	}

	// return XMLHttpRequest to allow aborting the request etc.
	return xhr;
};