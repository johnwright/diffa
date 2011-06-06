
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

var gLastStates = [];

function renderState(state) {
  // TODO: When #216 changes SYNCHRONIZING to SCANNING, this should be updated
  switch (state) {
    case "REQUESTING":    return "Requesting Scan";
    case "UNKNOWN":       return "Scan not run";
    case "FAILED":        return "Last Scan Failed";
    case "UP_TO_DATE":    return "Up to Date";
    case "SYNCHRONIZING": return "Scan In Progress";
  }

  return null;
}

function renderPairScopedActions(pairKey, actionListContainer, repairStatus) {
  var actionListCallback = function(actionList, status, xhr) {
    if (!actionList) return;

    $.each(actionList, function(i, action) {
      // filter out entity-scoped actions
      if (action.scope != "pair") return;
      appendActionButtonToContainer(actionListContainer, action, pairKey, null, repairStatus);
      actionListContainer.append('<br clear="both" />');
    });
  };

  $.ajax({ url: API_BASE + '/actions/' + pairKey, success: actionListCallback });
}

function removeAllPairs() {
  $('#pairs').find('tr').remove();
}

function addPair(name, state) {
  var actionButtonsForPair = $('<td></td>');
  var repairStatusForPair = $('<td></td>');
  $('#pairs').append($('<tr><td>' + name + '</td><td>' + renderState(state) + '</td></tr>')
      .append(actionButtonsForPair)
      .append(repairStatusForPair));
  renderPairScopedActions(name, actionButtonsForPair, repairStatusForPair);
}

$(document).ready(function() {
  $('#scan_all').click(function(e) {
    e.preventDefault();

    removeAllPairs();
    for (var pair in gLastStates) {
      addPair(pair, 'REQUESTING');
    }

    $.ajax({
          url: API_BASE + "/diffs/sessions/scan_all",
          type: "POST",
          success: function() {
            removeAllPairs();
            for (var pair in gLastStates) {
              addPair(pair, 'SYNCHRONIZING');
            }
          },
          error: function(jqXHR, textStatus, errorThrown) {
            alert("Error in scan request: " + errorThrown);
          }
        });
    return false;
  });

  $("#pairs").smartupdater({
        url : API_BASE + "/diffs/sessions/all_scan_states",
        dataType: "json",
        minTimeout: 5000
      }, function (states) {
    gLastStates = states;

    removeAllPairs();
    for (var pair in states) {
      addPair(pair, states[pair]);
    }
  }
  );

  setInterval('$("#pollState").html($("#pairs")[0].smartupdaterStatus.state)',1000);
  setInterval('$("#pollFrequency").html($("#pairs")[0].smartupdaterStatus.timeout)',1000);

});
