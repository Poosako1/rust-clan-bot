// Универсальный исполнитель для таблицы «клан». Менять этот код больше не нужно:
// вся логика живёт в боте, скрипт только читает и пишет ячейки.
// Установка: Расширения -> Apps Script -> вставить -> Развернуть как веб-приложение.
// SECRET замени на тот же секрет, который вписан в .env бота (SCRIPT_SECRET).
const SECRET = 'ПОМЕНЯЙ_СЕКРЕТ';

function out_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

function sheet_(name) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(name);
  if (!sh) throw new Error('нет вкладки: ' + name);
  return sh;
}

function run_(op) {
  const sh = sheet_(op.sheet);
  if (op.op === 'get') {            // весь лист как показан на экране (с заголовком)
    const n = sh.getLastRow(), m = sh.getLastColumn();
    return n > 0 && m > 0 ? sh.getRange(1, 1, n, m).getDisplayValues() : [];
  }
  if (op.op === 'set') {            // записать блок значений; text:true = как текст
    const v = op.values;
    if (!v.length) return true;
    const range = sh.getRange(op.row, op.col, v.length, v[0].length);
    if (op.text) range.setNumberFormat('@');
    range.setValues(v);
    return true;
  }
  if (op.op === 'insert_before') {  // вставить пустую строку перед строкой row
    sh.insertRowBefore(op.row);
    return true;
  }
  if (op.op === 'delete_rows') {    // удалить строки (номера как в таблице)
    op.rows.slice().sort(function (a, b) { return b - a; }).forEach(function (r) { sh.deleteRow(r); });
    return true;
  }
  throw new Error('unknown op: ' + op.op);
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(15000);
    const d = JSON.parse(e.postData.contents);
    if (d.secret !== SECRET) return out_({ ok: false, error: 'bad secret' });
    return out_({ ok: true, results: d.ops.map(run_) });
  } catch (err) {
    return out_({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}
