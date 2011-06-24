
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

function appendActionButtonToContainer($container, action, pairKey, itemID, $repairStatus) {
  // Reset the status box
  $repairStatus.text("No repairs in progress");
  $('<button class="repair">' + action.name +  '</button>')
    .click(function(e) {
      e.preventDefault();
      var $button = $(this);
      var url = API_BASE + ((itemID == null) ? action.path : action.path.replace("${id}", itemID));

      if ($button.hasClass('disabled')) {
        return false;
      }
      $button.addClass('disabled');
      $repairStatus.text('Repairing...');
      $.ajax({
            type: "POST",
            url: url,
            success: function(data, status, xhr) {
              $repairStatus.html('Repair status: ' + data.code + '<br/>output: ' + data.output);
            },
            error: function(xhr, status, ex) {
              if (console && console.log) {
                var error = {
                  type: "POST",
                  url: url,
                  status: status,
                  exception: ex,
                  xhr: xhr
                };
                if (itemID != null)
                  console.log("error during repair for item " + itemID + ": ", error);
                else
                  console.log("error during repair for pair " + pairKey + ": ", error);
              }
              $repairStatus.text('Error during repair: ' + (status || ex.message));
            },
            complete: function() {
              $button.removeClass('disabled');
            }
          });
      return false;
    })
    .appendTo($container);
}
