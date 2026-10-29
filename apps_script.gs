// Скрипт для Google-таблицы: Расширения -> Apps Script -> вставить -> Развернуть как веб-приложение.
// SECRET замени на тот же секрет, который вписан в .env бота (SCRIPT_SECRET).
const SECRET = 'ПОМЕНЯЙ_СЕКРЕТ';
const TAB = 'Профили';
const HEADERS = ['Discord ID', 'Discord', 'Steam', 'BattleMetrics', 'Регион', 'Карта', 'Обновлено'];

function sheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(TAB);
  if (!sh) {
    sh = ss.insertSheet(TAB);
    sh.appendRow(HEADERS);
    sh.setFrozenRows(1);
    sh.getRange('A:A').setNumberFormat('@');  // текст, иначе длинные ID портятся
    sh.getRange('F:F').setNumberFormat('@');  // номер карты тоже текстом
  }
  return sh;
}

function out_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(15000);
    const d = JSON.parse(e.postData.contents);
    if (d.secret !== SECRET) return out_({ ok: false, error: 'bad secret' });
    const sh = sheet_();
    if (d.action === 'list') {
      const n = sh.getLastRow();
      const rows = n > 1 ? sh.getRange(2, 1, n - 1, HEADERS.length).getDisplayValues() : [];
      return out_({ ok: true, rows: rows });
    }
    if (d.action === 'upsert') {
      const row = d.row.map(String);
      const n = sh.getLastRow();
      const ids = n > 0 ? sh.getRange(1, 1, n, 1).getDisplayValues().map(function (r) { return r[0]; }) : [];
      const i = ids.indexOf(row[0]);
      const target = i >= 0 ? i + 1 : n + 1;
      const range = sh.getRange(target, 1, 1, row.length);
      range.setNumberFormat('@');
      range.setValues([row]);
      return out_({ ok: true });
    }
    return out_({ ok: false, error: 'unknown action' });
  } catch (err) {
    return out_({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}
