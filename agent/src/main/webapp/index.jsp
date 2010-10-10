<%--

    Copyright (C) 2010 LShift Ltd.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

--%>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html>
	<head>
		<title>Diffa</title>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<link rel="stylesheet" type="text/css" href="css/reset.css" media="screen" />
		<link rel="stylesheet" type="text/css" href="css/grid24.css" media="screen" />
		<link rel="stylesheet" type="text/css" href="css/jbase.css" media="screen" />
		<link rel="stylesheet" type="text/css" href="css/stickyfooter.css" media="screen" />
		<link rel="stylesheet" type="text/css" href="css/jquery-ui-slider.1.8.5.css" media="screen" />
		<link rel="stylesheet" type="text/css" href="css/style.css" media="screen" />
		<link rel="stylesheet" type="text/css" href="css/styles.css" media="screen" />
	</head>
	<body>
		<div id="wrap" class="jbasewrap">

			<h1 class="padtop logo">Diffa</h1>
			<div id="heatmapContainer" class="margintop"></div>
			<div id="heatmapcontrols" class="clear padtop padbottom">
				<div class="right"><span id="errorContainer"></span></div>
				<div id="scrollBarContainer" class="left">
					<div id="scrollBar" class="left grid4col"></div>
					<br class="clearboth" />
				</div>
			</div>
			<div id="diffListContainer" class="left">
				<table id="difflist" summary="Table of differences"  border="0" cellspacing="0" cellpadding="0">
					<thead>
						<tr>
							<th class="grid2col date">Date</th>
							<th class="grid2col">Time</th>
							<th class="grid2col">Group</th>
							<th class="grid2col">Pairing</th>
							<th class="grid3col">Item ID</th>
							<th class="grid4col">Difference type</th>
						</tr>
					</thead>
					<tbody>
						<tr>
							<td colspan="6">No items loaded yet...</td>
						</tr>
					</tbody>
				</table>
			</div>
			<div id="contentviewer" class="grid9col left marginleft">
				<h6>No item selected</h6>
				<div>
					<div id="item1" class="diffSet">
						<div class="diffHash">
							<span>item 1</span>
						</div>
						<div class="codeBox">
							<pre></pre>
						</div>
					</div>
					<div id="item2" class="diffSet">
						<div class="diffHash">
							<span>item 2</span>
						</div>
						<div class="codeBox">
							<pre></pre>
						</div>
					</div>
				</div>
				<div id="diffRepair">
					<h6>Repair options</h6>
					<div id="actionlist">
					</div>					
					<br class="clearboth"/>
					<p id="repairstatus" class="margintopsmall marginleft marginright"><em>No repairs in progress</em></p>
				</div>
			</div>
			
			<br class="clearboth"/>
			<div class="push"></div>
		</div><!-- end wrap -->
		<div class="footer jbasewrap">
			&copy;2010 LShift Ltd
		</div>

    <script type="text/javascript">
      <%
        // Allow the API root to overriden via the environment

        javax.naming.Context ctx = new javax.naming.InitialContext();
        javax.naming.Context myenv = (javax.naming.Context) ctx.lookup("java:comp/env");
        java.lang.String customRoot = (java.lang.String) myenv.lookup("diffaCustomRoot");

        ServletContext servletCtx = pageContext.getServletContext();
        String ctxPath = servletCtx.getContextPath();

        String apiRoot;
        if (customRoot.equals("__context__")) {
          apiRoot = ctxPath;
        } else {
          apiRoot = customRoot;
        }
      %>

      var API_BASE = "<%= apiRoot %>/rest"
    </script>
		<script type="text/javascript" src="js/jquery-1.4.1.patched.js" charset="utf-8"></script>
		<script type="text/javascript" src="js/jquery-ui-slider.1.8.5.min.js" charset="utf-8"></script>
		<script type="text/javascript" src="js/jquery.color.js" charset="utf-8"></script>
		<script type="text/javascript" src="js/raphael.js" charset="utf-8"></script> 
		<script type="text/javascript" src="js/app.js" charset="utf-8"></script>
	</body>
</html>
