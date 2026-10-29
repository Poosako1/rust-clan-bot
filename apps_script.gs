// Скрипт для Google-таблицы «клан»: Расширения -> Apps Script -> вставить -> Развернуть как веб-приложение.
// SECRET замени на тот же секрет, который вписан в .env бота (SCRIPT_SECRET).
const SECRET = 'ПОМЕНЯЙ_СЕКРЕТ';
const PLAYERS = 'общее';       // Никнейм | роль | зарплата | банк | батлметрикс | Discord ID | Steam | Регион
const WIPES = 'вайп';          // Вайп | Начало (МСК) | Конец (МСК) | Статус
const RESULTS = 'итоги вайпа'; // Вайп | Ник | Discord ID | Явка | Опоздание, мин | Гс, ч | Rust, ч

function tab_(name) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(name);
  if (!sh) throw new Error('нет вкладки: ' + name);
  return sh;
}

function out_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

function rows_(sh, ncols) {
  const n = sh.getLastRow();
  return n > 1 ? sh.getRange(2, 1, n - 1, ncols).getDisplayValues() : [];
}

function norm_(s) {
  return String(s || '').trim().toLowerCase();
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(15000);
    const d = JSON.parse(e.postData.contents);
    if (d.secret !== SECRET) return out_({ ok: false, error: 'bad secret' });

    if (d.action === 'players_list') {
      return out_({ ok: true, rows: rows_(tab_(PLAYERS), 8) });
    }

    if (d.action === 'player_upsert') {
      const p = d.p;
      const sh = tab_(PLAYERS);
      const rows = rows_(sh, 8);
      let idx = -1, how = 'new';
      for (let i = 0; i < rows.length; i++) { if (rows[i][5] === p.discord) { idx = i; how = 'discord'; break; } }
      if (idx < 0) {
        for (let i = 0; i < rows.length; i++) {
          if (rows[i][4].indexOf('players/' + p.bm_id) >= 0) { idx = i; how = 'bm'; break; }
        }
      }
      if (idx < 0) {
        for (let i = 0; i < rows.length; i++) {
          if (norm_(rows[i][0]) === norm_(p.nick)) { idx = i; how = 'nick'; break; }
        }
      }
      const r = idx >= 0 ? idx + 2 : Math.max(sh.getLastRow(), 1) + 1;
      sh.getRange(r, 6).setNumberFormat('@').setValue(p.discord);          // Discord ID
      sh.getRange(r, 5).setValue(p.bm_url);                                 // батлметрикс
      sh.getRange(r, 7).setValue(p.steam);                                  // Steam
      sh.getRange(r, 8).setValue(p.region);                                 // Регион
      sh.getRange(r, 4).setNumberFormat('@').setValue(p.card);              // банк
      if (idx < 0 || !String(sh.getRange(r, 1).getValue()).trim()) sh.getRange(r, 1).setValue(p.nick);
      return out_({ ok: true, how: how, row: r });
    }

    if (d.action === 'wipes_list') {
      return out_({ ok: true, rows: rows_(tab_(WIPES), 4) });
    }

    if (d.action === 'wipe_upsert') {
      const sh = tab_(WIPES);
      const row = d.row.map(String);
      const rows = rows_(sh, 4);
      let r = -1;
      for (let i = 0; i < rows.length; i++) { if (rows[i][0] === row[0]) { r = i + 2; break; } }
      if (r < 0) r = Math.max(sh.getLastRow(), 1) + 1;
      const range = sh.getRange(r, 1, 1, 4);
      range.setNumberFormat('@');
      range.setValues([row]);
      return out_({ ok: true });
    }

    if (d.action === 'results_write') {
      const sh = tab_(RESULTS);
      const n = sh.getLastRow();
      if (n > 1) {   // удалить прежние строки этого вайпа (снизу вверх)
        const names = sh.getRange(2, 1, n - 1, 1).getDisplayValues();
        for (let i = names.length - 1; i >= 0; i--) { if (names[i][0] === d.wipe) sh.deleteRow(i + 2); }
      }
      if (d.rows.length) {
        const start = Math.max(sh.getLastRow(), 1) + 1;
        const range = sh.getRange(start, 1, d.rows.length, 7);
        sh.getRange(start, 1, d.rows.length, 4).setNumberFormat('@');
        range.setValues(d.rows);
      }
      return out_({ ok: true });
    }

    return out_({ ok: false, error: 'unknown action' });
  } catch (err) {
    return out_({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}
