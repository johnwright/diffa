<%--

    Copyright (C) 2010-2011 LShift Ltd.

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

<%@ page contentType="text/html;charset=UTF-8" language="java" %>

<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<html>
<head>
  <title>diffa Web Participants</title>
  <style type="text/css">
    .participant {
      border: 1px solid black;
      width: 500px;
      margin: 20px;
      padding-left: 10px;
      padding-right: 10px;
    }
    .upstream {
      float: left;
    }
    .downstream {
      float: right;
    }
    .participant h1 {
      text-align: center;
    }

    .entity-list {
      width: 100%;
      height: 250px;
    }
    .add {
      margin-top: 10px;
    }

    #scenarios {
      clear: both;
    }
  </style>
</head>
<body>
  Welcome to diffa web participants

  <div id="upstream" class="participant upstream">
    <h1>Upstream</h1>
    <form action="">
      <select name="entities" class="entity-list" multiple="true">
      </select>
      <input type="button" name="remove" value="Remove">

      <div class="add">
        <label><input type="text" name="id" value="id"></label>
        <textarea name="body" value="Put your content here ..." rows="10" cols="40"></textarea>
        <input type="button" name="add" value="Add">
      </div>
    </form>
  </div>
  <div id="downstream" class="participant downstream">
    <h1>Downstream</h1>
    <form action="">
      <select name="entities" class="entity-list" multiple="true">
      </select>
      <input type="button" name="remove" value="Remove">

      <div class="add">
        <label><input type="text" name="id" value="id"></label>
        <textarea name="body" value="Put your content here ..." rows="10" cols="40"></textarea>
        <input type="button" name="add" value="Add">
      </div>
    </form>
  </div>

  <div id="scenarios">
    <h1>Test Scenarios</h1>
    <p>
      The following will load the test participants with a series of test data sets, useful for seeing various behaviours
      in the agents.
    </p>
    <ul>
      <li><a href="javascript:loadScenario(1)">Scenario 1</a></li>
    </ul>
  </div>

  <script type="text/javascript" src="<c:url value="/static/javascript/jquery-1.4.2.min.js"/>"></script>
  <script type="text/javascript" src="<c:url value="/static/javascript/date.js"/>"></script>
  <script type="text/javascript" src="<c:url value="/static/javascript/md5-min.js"/>"></script>
  <script type="text/javascript" src="<c:url value="/static/javascript/scenarios.js"/>"></script>
  <script type="text/javascript">
    function addEntity(listId, entityId, body) {
      var entityVsn = hex_md5(body);
      $(listId + ' .entity-list option[value="' + entityId + '"]').remove();
      $(listId + ' .entity-list').append($('<option></option>').attr("value", entityId).text(entityId + ": [" + entityVsn + "] -> " + body));
    }
    function addAndStoreEntity(listType, entityId, entityUpdated, body) {
      var listId = '#' + listType;
      addEntity(listId, entityId, body);
      
      var data = {
        partId: listType,
        lastUpdated: entityUpdated,
        entityId: entityId,
        body: body
      };
      $.post('<c:url value="/entities"/>', data);
    }

    function addAddHandler(listType) {
      var listId = '#' + listType;

      $(listId + ' input[name="add"]').click(function() {
        var newId = $(listId + ' input[name="id"]').val();
        var newLastUpdated = new Date().toString("yyyy-MM-ddTHH:mm:ss");
        var newBody = $(listId + ' textarea[name="body"]').val();
        addAndStoreEntity(listType, newId, newLastUpdated, newBody);
      });
    }

    function addRemoveHandler(listType) {
      var listId = '#' + listType;

      $(listId + ' input[name="remove"]').click(function() {
        var selected = $(listId + ' .entity-list :selected');
        var selectedId = selected.val();
        selected.remove();

        var data = {
          _method: 'DELETE',
          partId: listType,
          entityId: selectedId
        };
        $.post('<c:url value="/entities"/>', data);
      });
    }

    function queryCurrent(listType) {
      var listId = '#' + listType;

      $.getJSON('<c:url value="/entities.json"/>?partId=' + listType, function(data) {
        $(data).each(function(idx, entity) {
          addEntity(listId, entity.entityId, entity.body);
        })
      });
    }

    function loadScenario(scenarioNum) {
      var scenarioData = scenarios[scenarioNum.toString()];

      // Apply the parts of the scenario
      $.each(scenarioData['upstream'], function(idx, el) {
        addAndStoreEntity('upstream', el['id'], el['lastUpdated'], el['body']);
      });
      $.each(scenarioData['downstream'], function(idx, el) {
        addAndStoreEntity('downstream', el['id'], el['lastUpdated'], el['body']);
      });
    }

    addAddHandler('upstream');
    addRemoveHandler('upstream');
    addAddHandler('downstream');
    addRemoveHandler('downstream');
    queryCurrent('upstream');
    queryCurrent('downstream');
  </script>
</body>
</html>