const path = require('path');
const EventEmitter = require('events');
const low = require('lowdb');
const FileSync = require('lowdb/adapters/FileSync');

const dbPath = path.join(__dirname, '..', 'state.json');
const adapter = new FileSync(dbPath);
const db = low(adapter);

db.defaults({ files: {}, logs: [] }).write();

const emitter = new EventEmitter();

/**
 * NOTE: We always use ARRAY paths like ['files', filename] instead of the
 * dot-string form 'files.myzip.zip'. lowdb/lodash treats dot-strings as
 * nested paths, so a filename like "myzip.zip" would be misread as
 * files -> myzip -> zip. Array paths avoid that bug entirely.
 */

function getState(filename) {
  return db.get(['files', filename]).value() || null;
}

function updateState(filename, updates) {
  const existing = db.get(['files', filename]).value() || {
    status: 'pending',
    megaLink: null,
    zipPassword: null,
    channelId: null,
    error: null,
  };

  const merged = {
    ...existing,
    ...updates,
    lastUpdated: new Date().toISOString(),
  };

  db.set(['files', filename], merged).write();
  emitter.emit('update', { filename, state: merged });
  return merged;
}

function getAllStates() {
  return db.get('files').value();
}

function removeState(filename) {
  db.unset(['files', filename]).write();
  emitter.emit('update', { filename, state: null });
}

// ── Upload Logs ─────────────────────────────────────────────────────────────

/**
 * Appends a completed upload entry to the persistent log.
 * @param {{ filename, megaLink, zipPassword, channelId, channelName, messageId }} entry
 */
function appendLog(entry) {
  const record = {
    ...entry,
    sentAt: new Date().toISOString(),
  };
  db.get('logs').push(record).write();
  emitter.emit('log', record);
  return record;
}

function getLogs() {
  return db.get('logs').value() || [];
}

function clearLogs() {
  db.set('logs', []).write();
  emitter.emit('logs-cleared');
}

module.exports = { getState, updateState, getAllStates, removeState, appendLog, getLogs, clearLogs, emitter };
