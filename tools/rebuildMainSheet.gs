/*
Google Apps Script: rebuildMainSheet

Purpose:
- Aggregate all sheets ending with "_raw"
- Keep only alive leads (is_alive === TRUE)
- Deduplicate by final_url, keeping the row with highest score
- Sort by score descending
- Assign stable 5-digit IDs in column A of the "Main" sheet (reuse existing IDs when final_url matches)

How to use:
1. Open your Google Spreadsheet in the browser.
2. Extensions → Apps Script
3. Create a new script file and paste this entire contents (or upload this file via the editor).
4. Run the function `rebuildMainSheet` (authorize when prompted).
5. Optionally set a time-based trigger to run `rebuildMainSheet` periodically.

Assumptions:
- All "*_raw" sheets share the same header row and column order.
- The header row includes at least: final_url, is_alive, score (case-insensitive match).
- The Main sheet will contain column A = id (5-digit), and the rest of the columns are exactly the raw header order.

Notes on IDs:
- Existing IDs in Main are preserved when final_url matches.
- New IDs start from max(existing IDs)+1 or 30000 if none exist.
- IDs are written as zero-padded 5-digit strings (e.g., 30000 -> "30000").

Logging:
- The script logs a short summary using `Logger.log` and `console.log` (visible in Executions).
*/

function rebuildMainSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getSheets();

  // Collect raw sheets ending with _raw
  var rawSheets = sheets.filter(function(sh) {
    return sh.getName().toLowerCase().endsWith('_raw');
  });

  if (rawSheets.length === 0) {
    Logger.log('No _raw sheets found. Exiting.');
    return;
  }

  // Read header from first raw sheet
  var headerRange = rawSheets[0].getRange(1, 1, 1, rawSheets[0].getLastColumn());
  var headers = headerRange.getValues()[0].map(function(h){ return String(h || '').trim(); });

  // Find indexes for required columns (0-based)
  var idx_final_url = findHeaderIndex(headers, 'final_url');
  var idx_is_alive = findHeaderIndex(headers, 'is_alive');
  var idx_score = findHeaderIndex(headers, 'score');

  if (idx_final_url === -1 || idx_is_alive === -1 || idx_score === -1) {
    throw new Error('Required columns not found in headers: final_url, is_alive, score');
  }

  // Build map from final_url -> best row
  var bestByFinal = {}; // final_url -> {rowValues: [...], score: number, firstSeenOrder: n}
  var orderCounter = 0;

  rawSheets.forEach(function(sh) {
    var name = sh.getName();
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2) return; // no data

    var data = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();

    data.forEach(function(row) {
      orderCounter += 1;
      var final_url = normalizeString(row[idx_final_url]);
      var is_alive_val = row[idx_is_alive];
      var is_alive = isTruthy(is_alive_val);
      if (!is_alive) return; // skip non-alive rows

      var scoreRaw = row[idx_score];
      var score = Number(scoreRaw) || 0;

      if (!final_url) return; // skip rows without final_url

      if (!(final_url in bestByFinal)) {
        bestByFinal[final_url] = { rowValues: row, score: score, firstSeenOrder: orderCounter };
      } else {
        var existing = bestByFinal[final_url];
        if (score > existing.score) {
          bestByFinal[final_url] = { rowValues: row, score: score, firstSeenOrder: existing.firstSeenOrder };
        }
        // if score equal, keep existing (first seen)
      }
    });
  });

  // Convert bestByFinal to array
  var results = Object.keys(bestByFinal).map(function(final_url) {
    var obj = bestByFinal[final_url];
    return { final_url: final_url, row: obj.rowValues, score: obj.score, order: obj.firstSeenOrder };
  });

  if (results.length === 0) {
    Logger.log('No alive rows found across raw sheets. Exiting.');
    return;
  }

  // Sort by score desc, then by first seen order
  results.sort(function(a,b){
    if (b.score !== a.score) return b.score - a.score;
    return a.order - b.order;
  });

  // Prepare Main sheet
  var mainName = 'Main';
  var prevMain = ss.getSheetByName(mainName);

  // Load existing ID map from current Main (if it had content before)
  var existingIdMap = {}; // final_url -> id
  var existingMaxId = 0;
  if (prevMain) {
    var prevLastRow = prevMain.getLastRow();
    if (prevLastRow > 1) {
      var prevRange = prevMain.getRange(1,1,1, prevMain.getLastColumn());
      var prevHeader = prevRange.getValues()[0].map(function(h){return String(h||'').trim();});
      var prevFinalIdx = prevHeader.indexOf('final_url');
      if (prevFinalIdx !== -1) {
        var prevData = prevMain.getRange(2,1,prevLastRow-1, prevMain.getLastColumn()).getValues();
        for (var i=0;i<prevData.length;i++){
          var row = prevData[i];
          var idVal = row[0];
          var finalVal = row[prevFinalIdx];
          if (finalVal) {
            existingIdMap[normalizeString(finalVal)] = idVal;
            var intId = parseInt(idVal,10);
            if (!isNaN(intId) && intId > existingMaxId) existingMaxId = intId;
          }
        }
      }
    }
  }

  // Now (re)create main sheet and header
  var mainHeader = ['id'].concat(headers);
  if (!prevMain) {
    prevMain = ss.insertSheet(mainName);
  } else {
    prevMain.clearContents();
  }
  prevMain.getRange(1,1,1,mainHeader.length).setValues([mainHeader]);

  var nextId = (existingMaxId > 0) ? existingMaxId + 1 : 30000;

  // Build rows to write into Main
  var outRows = results.map(function(item){
    var final_url = item.final_url;
    var row = item.row.slice(0); // clone
    var id;
    if (final_url in existingIdMap) {
      id = existingIdMap[final_url];
    } else {
      id = String(nextId);
      nextId += 1;
    }
    // Prepend id
    // Ensure ID is zero-padded to 5 digits
    var paddedId = id.toString();
    if (!/^[0-9]+$/.test(paddedId)) {
      paddedId = paddedId; // keep non-numeric as-is
    } else {
      paddedId = paddedId.padStart(5, '0');
    }

    return [paddedId].concat(row);
  });

  // Write rows
  prevMain.getRange(2,1,outRows.length, outRows[0].length).setValues(outRows);

  Logger.log('Rebuild complete. Rows written: ' + outRows.length);
}


/**
 * Dry-run wrapper: returns an object describing what would be written without modifying the Main sheet.
 * Useful for testing and for the Apps Script UI to preview changes.
 */
function rebuildMainSheetDryRun() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getSheets();

  var rawSheets = sheets.filter(function(sh) {
    return sh.getName().toLowerCase().endsWith('_raw');
  });

  if (rawSheets.length === 0) {
    return { ok: false, message: 'No _raw sheets found.' };
  }

  // Reuse the main function logic but without writing. To avoid duplicating too much logic,
  // call rebuildMainSheet and catch writes - but rebuildMainSheet writes by design. Instead,
  // replicate minimal selection + dedupe + sort logic here and return summary.

  // Read header from first raw sheet
  var headerRange = rawSheets[0].getRange(1, 1, 1, rawSheets[0].getLastColumn());
  var headers = headerRange.getValues()[0].map(function(h){ return String(h || '').trim(); });

  var idx_final_url = findHeaderIndex(headers, 'final_url');
  var idx_is_alive = findHeaderIndex(headers, 'is_alive');
  var idx_score = findHeaderIndex(headers, 'score');

  if (idx_final_url === -1 || idx_is_alive === -1 || idx_score === -1) {
    return { ok: false, message: 'Required columns not found in headers: final_url, is_alive, score' };
  }

  var bestByFinal = {};
  var orderCounter = 0;

  rawSheets.forEach(function(sh) {
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2) return;
    var data = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
    data.forEach(function(row) {
      orderCounter += 1;
      var final_url = normalizeString(row[idx_final_url]);
      var is_alive_val = row[idx_is_alive];
      var is_alive = isTruthy(is_alive_val);
      if (!is_alive) return;
      var scoreRaw = row[idx_score];
      var score = Number(scoreRaw) || 0;
      if (!final_url) return;
      if (!(final_url in bestByFinal)) {
        bestByFinal[final_url] = { rowValues: row, score: score, firstSeenOrder: orderCounter };
      } else {
        var existing = bestByFinal[final_url];
        if (score > existing.score) {
          bestByFinal[final_url] = { rowValues: row, score: score, firstSeenOrder: existing.firstSeenOrder };
        }
      }
    });
  });

  var results = Object.keys(bestByFinal).map(function(final_url) {
    var obj = bestByFinal[final_url];
    return { final_url: final_url, row: obj.rowValues, score: obj.score, order: obj.firstSeenOrder };
  });

  results.sort(function(a,b){
    if (b.score !== a.score) return b.score - a.score;
    return a.order - b.order;
  });

  return { ok: true, count: results.length, sample_final_urls: results.slice(0,10).map(function(r){return r.final_url;}) };
}


/* Helper functions */

function findHeaderIndex(headers, name) {
  var idx = -1;
  var lname = name.toLowerCase();
  for (var i=0;i<headers.length;i++){
    if (String(headers[i]).toLowerCase() === lname) { idx = i; break; }
  }
  return idx;
}

function normalizeString(v) {
  if (v === null || v === undefined) return '';
  return String(v).toString().trim();
}

function isTruthy(v) {
  if (v === true) return true;
  if (v === false) return false;
  if (v === null || v === undefined) return false;
  var s = String(v).trim().toLowerCase();
  return (s === 'true' || s === '1' || s === 'yes' || s === 'ok');
}
