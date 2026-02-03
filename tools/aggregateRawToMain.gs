/*
Apps Script: aggregateRawToMain

Purpose:
- Read all sheets whose name ends with "_raw"
- From each raw sheet, select rows where "score" >= 40
- Extract columns A-E (assumed order: store_name, url, comment, score, region)
- Deduplicate by url (column B in extracted set): if duplicate URLs, keep the row with the highest score
- Preserve existing IDs in `Main` by matching on url (column B of Main)
- Assign new stable 5-digit IDs for new rows starting at 03000 if no existing IDs
- Write results to `Main` sheet with header: [id, store_name, url, comment, score, region]

Usage:
1) Open your Google Spreadsheet -> Extensions -> Apps Script
2) Create a new script file and paste this content (or upload it)
3) Run `aggregateRawToMainDryRun()` first to preview (no writes)
4) If preview looks good, run `aggregateRawToMain()` to update `Main`

Notes on columns:
- The script expects raw sheets to have a header row where one column is named "score" (case-insensitive).
- It extracts the first five columns (A-E) from each raw row; ensure these correspond to store_name, url, comment, score, region.
- IDs are stored only in `Main` column A. Raw sheets are not modified.
*/

function aggregateRawToMainDryRun() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getSheets();
  var rawSheets = sheets.filter(function(sh) { return sh.getName().toLowerCase().endsWith('_raw'); });
  if (rawSheets.length === 0) {
    Logger.log('No _raw sheets found.');
    return { ok: false, message: 'No _raw sheets found.' };
  }

  var collected = [];
  var processed = 0;

  rawSheets.forEach(function(sh) {
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2) return; // no data

    var header = sh.getRange(1,1,1,Math.min(10,lastCol)).getValues()[0];
    // Attempt to find score index if score not in D (4th col)
    var scoreIndex = -1;
    for (var i=0;i<header.length;i++){
      if (String(header[i]).toLowerCase() === 'score') { scoreIndex = i; break; }
    }
    // Default: assume score is column 4 (index 3) if header not found
    if (scoreIndex === -1) scoreIndex = 3;

    var data = sh.getRange(2,1,lastRow-1, Math.max(5,lastCol)).getValues();
    data.forEach(function(row){
      processed += 1;
      var scoreRaw = row[scoreIndex];
      var score = Number(scoreRaw) || 0;
      if (score < 40) return; // skip
      // extract A-E (indices 0..4). If sheet has fewer cols, pad with ''
      var out = [];
      for (var j=0;j<5;j++) out.push(row[j] === undefined ? '' : row[j]);
      // Normalize url string for safe dedupe
      var url = (out[1] || '').toString().trim();
      if (!url) return; // skip rows without URL
      collected.push({url: url, row: out, score: score});
    });
  });

  if (collected.length === 0) {
    Logger.log('No candidate rows with score >= 40 found.');
    return { ok: true, count: 0 };
  }

  // Deduplicate by normalized url, keep highest score
  var byUrl = {};
  collected.forEach(function(item, idx){
    var u = normalizeString(item.url);
    if (!(u in byUrl)) {
      byUrl[u] = item;
      byUrl[u].firstIdx = idx;
    } else {
      if (item.score > byUrl[u].score) {
        byUrl[u] = item;
        byUrl[u].firstIdx = idx;
      }
    }
  });

  var results = Object.keys(byUrl).map(function(k){ return byUrl[k]; });
  // sort by score desc, then by first occurrence
  results.sort(function(a,b){ if (b.score !== a.score) return b.score - a.score; return a.firstIdx - b.firstIdx; });

  Logger.log('Processed raw rows: ' + processed + ', candidates after filter: ' + collected.length + ', deduped results: ' + results.length);
  // return sample urls
  var sample = results.slice(0,20).map(function(r){ return r.url; });
  return { ok: true, count: results.length, sample: sample };
}

function aggregateRawToMain() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getSheets();
  var rawSheets = sheets.filter(function(sh) { return sh.getName().toLowerCase().endsWith('_raw'); });
  if (rawSheets.length === 0) {
    Logger.log('No _raw sheets found. Exiting.');
    return;
  }

  // Read existing Main (if exists) to preserve IDs
  var mainName = 'Main';
  var mainSheet = ss.getSheetByName(mainName);
  var existingIdMap = {}; // url -> id (string)
  var existingMaxId = 0;
  if (mainSheet) {
    var mLast = mainSheet.getLastRow();
    if (mLast > 1) {
      var mLastCol = mainSheet.getLastColumn();
      var mHeader = mainSheet.getRange(1,1,1,mLastCol).getValues()[0];
      // find url column in Main (assumes url is one of the header names)
      var urlIdx = -1;
      for (var i=0;i<mHeader.length;i++){
        if (String(mHeader[i]).toLowerCase() === 'url') { urlIdx = i; break; }
      }
      // Read existing rows
      var prevData = mainSheet.getRange(2,1,mLast-1, Math.max(mLastCol,2)).getValues();
      prevData.forEach(function(r){
        var idVal = r[0];
        var urlVal = (urlIdx !== -1) ? r[urlIdx] : (r[1] || '');
        var u = normalizeString(urlVal);
        if (u) {
          existingIdMap[u] = idVal;
          var n = parseInt(idVal,10);
          if (!isNaN(n) && n > existingMaxId) existingMaxId = n;
        }
      });
    }
  }

  // collect candidates
  var collected = [];
  var processed = 0;
  rawSheets.forEach(function(sh){
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2) return;
    var header = sh.getRange(1,1,1,Math.min(10,lastCol)).getValues()[0];
    var scoreIndex = -1;
    for (var i=0;i<header.length;i++){ if (String(header[i]).toLowerCase()==='score'){ scoreIndex = i; break; } }
    if (scoreIndex === -1) scoreIndex = 3; // default
    var data = sh.getRange(2,1,lastRow-1, Math.max(5,lastCol)).getValues();
    data.forEach(function(row){
      processed += 1;
      var scoreRaw = row[scoreIndex];
      var score = Number(scoreRaw) || 0;
      if (score < 40) return;
      var out = [];
      for (var j=0;j<5;j++) out.push(row[j] === undefined ? '' : row[j]);
      var url = (out[1] || '').toString().trim();
      if (!url) return;
      collected.push({url: url, row: out, score: score});
    });
  });

  if (collected.length === 0) {
    Logger.log('No rows with score >= 40 found. Nothing to write.');
    return;
  }

  // Deduplicate by normalized url, keep highest score
  var byUrl = {};
  collected.forEach(function(item, idx){
    var u = normalizeString(item.url);
    if (!(u in byUrl)) { byUrl[u] = item; byUrl[u].firstIdx = idx; }
    else { if (item.score > byUrl[u].score) { byUrl[u] = item; byUrl[u].firstIdx = idx; } }
  });

  var results = Object.keys(byUrl).map(function(k){ return byUrl[k]; });
  results.sort(function(a,b){ if (b.score !== a.score) return b.score - a.score; return a.firstIdx - b.firstIdx; });

  // Build output rows with IDs
  var rowsOut = [];
  var nextId = (existingMaxId > 0) ? existingMaxId + 1 : 3000; // start at 03000 if none
  results.forEach(function(item){
    var u = normalizeString(item.url);
    var id;
    if (u in existingIdMap) {
      id = existingIdMap[u];
    } else {
      id = String(nextId);
      nextId += 1;
    }
    // zero-pad to 5 digits
    if (/^[0-9]+$/.test(id)) id = id.padStart(5,'0');
    // item.row is [store_name,url,comment,score,region]
    var outRow = [id].concat(item.row);
    rowsOut.push(outRow);
  });

  // Prepare Main sheet header: id + A-E
  var mainHeader = ['id','store_name','url','comment','score','region'];
  var main; if (!mainSheet) { main = ss.insertSheet('Main'); } else { main = mainSheet; main.clearContents(); }
  main.getRange(1,1,1,mainHeader.length).setValues([mainHeader]);
  main.getRange(2,1,rowsOut.length, rowsOut[0].length).setValues(rowsOut);

  Logger.log('Aggregate complete. Raw processed: ' + processed + ', candidates: ' + collected.length + ', deduped: ' + results.length + ', written: ' + rowsOut.length );
}

/* Helpers */
function normalizeString(v) { if (v === null || v === undefined) return ''; return String(v).toString().trim(); }

*/