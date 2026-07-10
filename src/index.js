/**
 * src/index.js — Main orchestrator
 *
 * Pipeline: raw zip → decrypt → re-encrypt → upload to MEGA → Discord channel + message
 *
 * Entry points (all funnel into the same pipeline):
 *   1. Folder watcher (watched-folder/*.zip)
 *   2. GUI drag-and-drop upload
 *   3. GUI MEGA link ingest form
 *   4. TXT file drop (one channel per link)
 *   5. Mirror engine (external server scanner)
 *   6. Download engine (configured source channels)
 *
 * Storage policy: NO zip files are kept on disk after processing.
 *   - Raw zip deleted immediately after successful encryption.
 *   - Encrypted zip deleted immediately after successful MEGA upload.
 *   - Staging files deleted on error or after use.
 */

'use strict';

// ── Node.js < 20 compatibility ─────────────────────────────────────────────────
// discord.js-selfbot-v13 bundles undici which uses the `File` Web API global.
// `File` became a stable global in Node 20. On Node 18 it doesn't exist.
// We polyfill it before any module requiring discord.js-selfbot-v13 is loaded.
if (typeof File === 'undefined') {
  // Node 20+ added File to the buffer module; use it if available
  const bufFile = (() => { try { return require('buffer').File; } catch { return null; } })();
  if (bufFile) {
    global.File = bufFile;
  } else {
    // Node 18 fallback: minimal stub that satisfies undici's isinstance check.
    // The selfbot will boot; only operations that actually use File objects may
    // be limited (not relevant for our use case).
    global.File = class File extends (require('buffer').Blob || class Blob {}) {
      constructor(parts, name, opts) { super(parts, opts); this.name = name || ''; }
    };
  }
}

const path = require('path');
const fs = require('fs');

// ── .env support (for hosting) ─────────────────────────────────────────────────
const envFilePath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envFilePath)) {
  for (const line of fs.readFileSync(envFilePath, 'utf-8').split('\n')) {
    const m = line.match(/^\s*([^#=\s][^=]*?)\s*=\s*["']?(.*?)["']?\s*$/);
    if (m) process.env[m[1]] = m[2];
  }
}

const { watchFolder } = require('./folderWatcher');
const { encryptZip } = require('./zipEncryptor');
const { uploadToMega } = require('./megaUploader');
const { createZipChannel } = require('./discordManager');
const { sendZipMessage } = require('./webhookSender');
const { getClient } = require('./discordClient');
const { registerCommands, attachCommandHandler } = require('./commandHandler');
const { updateState, getState, getAllStates, appendLog, removeState } = require('./stateStore');
const { startGuiServer } = require('./gui/server');
const { startDownloadEngine, pauseDownloads, resumeDownloads, cancelDownloads } = require('./downloadEngine');
const downloadManager = require('./downloadEngine/downloadManager');
const { startMirrorEngine, stopMirrorEngine } = require('./mirrorEngine');
const { parseTxtFile } = require('./txtLinkIngester');
const { sleep, retryWithBackoff } = require('./utils/retry');

// ── Config ─────────────────────────────────────────────────────────────────────
const configPath = path.join(__dirname, '..', 'config', 'config.json');
const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));

// Apply environment variable overrides (for cloud hosting)
const envMap = {
  DISCORD_TOKEN: 'discordToken',
  DISCORD_CLIENT_ID: 'discordClientId',
  BOT_OWNER_ID: 'botOwnerId',
  GUILD_ID: 'guildId',
  CATEGORY_ID: 'categoryId',
  PERMISSION_ROLE_ID: 'permissionRoleId',
  MEGA_EMAIL: 'megaEmail',
  MEGA_PASSWORD: 'megaPassword',
  GUI_PORT: 'guiPort',
};
for (const [envKey, cfgKey] of Object.entries(envMap)) {
  if (process.env[envKey] !== undefined) config[cfgKey] = process.env[envKey];
}
if (process.env.MIRROR_USER_TOKEN && config.mirrorEngine) {
  config.mirrorEngine.userToken = process.env.MIRROR_USER_TOKEN;
}

// ── Folder paths ───────────────────────────────────────────────────────────────
const watchFolderPath = path.resolve(__dirname, '..', config.watchFolder || './watched-folder');
const stagingFolderPath = path.join(watchFolderPath, 'staging');
const downloadFolderPath = path.resolve(__dirname, '..', config.downloadEngine?.downloadFolder || './downloads');

for (const dir of [watchFolderPath, stagingFolderPath, downloadFolderPath]) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

// ── Concurrent queue ───────────────────────────────────────────────────────────
// Files are processed up to MAX_CONCURRENT at once; no global serial blocking.
const MAX_CONCURRENT = Math.max(1, parseInt(config.processingConcurrency || 2));
let activeCount = 0;
const queue = [];
const queuedFiles = new Set();

function enqueue(filename, rawZipPath, meta = {}) {
  if (queuedFiles.has(filename)) return;
  queuedFiles.add(filename);
  queue.push({ filename, rawZipPath, meta });
  drainQueue();
}

function drainQueue() {
  while (queue.length > 0 && activeCount < MAX_CONCURRENT) {
    const { filename, rawZipPath, meta } = queue.shift();
    activeCount++;

    processZip(filename, rawZipPath, meta)
      .catch((err) => {
        console.error(`[index] Unexpected error processing "${filename}": ${err.message}`);
        updateState(filename, { status: 'failed', error: err.message });
      })
      .finally(() => {
        queuedFiles.delete(filename);
        activeCount--;
        drainQueue();
      });
  }
}

// ── Main pipeline ─────────────────────────────────────────────────────────────

/**
 * Full pipeline: raw zip → encrypt → MEGA upload → Discord channel → message
 *
 * @param {string}      filename   - State store key (basename).
 * @param {string|null} rawZipPath - Path to the unencrypted source zip. Null if already encrypted.
 * @param {object}      meta       - Extra context: { sourceCategoryName, ... }
 */
async function processZip(filename, rawZipPath, meta = {}) {
  const baseName = path.basename(filename, path.extname(filename));
  let state = getState(filename) || {};

  // ── Deduplication: size-based ─────────────────────────────────────────────
  if (state.status === 'message_sent') {
    const currentSize = rawZipPath && fs.existsSync(rawZipPath) ? fs.statSync(rawZipPath).size : null;
    const savedSize = state.fileSize || null;
    if (currentSize !== null && savedSize !== null && currentSize !== savedSize) {
      console.log(`[index] "${filename}" size changed (${savedSize} → ${currentSize}) — re-processing.`);
      removeState(filename);
      state = {};
    } else {
      console.log(`[index] Skipping "${filename}" — already fully processed.`);
      return;
    }
  }

  console.log(`[index] Processing "${filename}" (status: ${state.status || 'new'})`);

  // ── Step 1: Encrypt ────────────────────────────────────────────────────────
  let encryptedPath = state.encryptedPath;
  let password = state.zipPassword;
  const alreadyEncrypted = encryptedPath && fs.existsSync(encryptedPath);

  if (!alreadyEncrypted) {
    if (!rawZipPath || !fs.existsSync(rawZipPath)) {
      console.error(`[index] Cannot process "${filename}" — source file missing.`);
      updateState(filename, { status: 'failed', error: 'Source file missing.' });
      return;
    }

    const fileSize = fs.statSync(rawZipPath).size;
    updateState(filename, { status: 'encrypting', error: null, fileSize });

    try {
      const outputPassword = config.zipPasswordMode === 'auto-random' ? null : config.zipInputPassword;
      // Per-file sourcePassword (from mirror/download) used to decrypt source zip
      const inputPassword = state.sourcePassword || config.zipInputPassword || null;
      const result = await encryptZip(rawZipPath, outputPassword, inputPassword);
      encryptedPath = result.encryptedPath;
      password = result.password;
      updateState(filename, { status: 'uploading', zipPassword: password, encryptedPath, error: null });
      console.log(`[index] Encrypted "${filename}" → ${encryptedPath} (password: ${password})`);

      // ✅ Delete raw source zip immediately — no local storage
      safeDelete(rawZipPath);
    } catch (err) {
      // If a per-file sourcePassword was set and failed, retry once with only
      // the global zipInputPassword (the zip may use the global password instead)
      if (state.sourcePassword && /Wrong password/i.test(err.message)) {
        console.warn(`[index] Decryption with sourcePassword "${state.sourcePassword}" failed for "${filename}". Retrying with global zipInputPassword...`);
        try {
          const outputPassword = config.zipPasswordMode === 'auto-random' ? null : config.zipInputPassword;
          const result = await encryptZip(rawZipPath, outputPassword, config.zipInputPassword || null);
          encryptedPath = result.encryptedPath;
          password = result.password;
          updateState(filename, { status: 'uploading', zipPassword: password, encryptedPath, error: null });
          console.log(`[index] Encrypted "${filename}" → ${encryptedPath} (password: ${password})`);
          safeDelete(rawZipPath);
        } catch (err2) {
          console.error(`[index] Encryption failed for "${filename}": ${err2.message}`);
          updateState(filename, { status: 'failed', error: `Encryption failed: ${err2.message}` });
          safeDelete(rawZipPath);
          return;
        }
      } else {
        console.error(`[index] Encryption failed for "${filename}": ${err.message}`);
        updateState(filename, { status: 'failed', error: `Encryption failed: ${err.message}` });
        safeDelete(rawZipPath);
        return;
      }
    }
  }

  // ── Step 2: Upload to MEGA ─────────────────────────────────────────────────
  let megaLink = state.megaLink;

  if (!megaLink) {
    try {
      megaLink = await retryWithBackoff(
        () => uploadToMega(encryptedPath, config),
        {
          retries: 3,
          delaysMs: [2000, 5000, 10000],
          onAttemptFail: (attempt, err) =>
            console.warn(`[index] MEGA upload attempt ${attempt} failed for "${filename}": ${err.message}`),
        }
      );
      updateState(filename, { status: 'uploaded', megaLink, error: null });
      console.log(`[index] Uploaded "${filename}" → ${megaLink}`);

      // ✅ Delete encrypted zip immediately after upload — no local storage
      safeDelete(encryptedPath);
    } catch (err) {
      console.error(`[index] MEGA upload failed for "${filename}": ${err.message}`);
      updateState(filename, { status: 'failed', error: `MEGA upload failed: ${err.message}` });
      safeDelete(encryptedPath);
      return;
    }
  }

  // ── Step 3: Discord channel ────────────────────────────────────────────────
  let channel = null;

  if (state.channelId) {
    try {
      const client = await getClient(config);
      channel = await client.channels.fetch(state.channelId);
    } catch { channel = null; }
  }

  if (!channel) {
    try {
      // Pass sourceCategoryName so discordManager can clone the category
      const channelOptions = {
        sourceCategoryName: state.sourceCategoryName || meta.sourceCategoryName || null,
      };
      channel = await retryWithBackoff(
        () => createZipChannel(baseName, config, channelOptions),
        {
          retries: 3,
          delaysMs: [2000, 5000, 10000],
          onAttemptFail: (attempt, err) =>
            console.warn(`[index] Channel creation attempt ${attempt} failed for "${filename}": ${err.message}`),
        }
      );
      updateState(filename, { status: 'channel_created', channelId: channel.id, error: null });
      console.log(`[index] Channel "${channel.name}" (${channel.id}) ready for "${filename}"`);
    } catch (err) {
      console.error(`[index] Channel creation failed for "${filename}": ${err.message}`);
      updateState(filename, { status: 'failed', error: `Channel creation failed: ${err.message}` });
      return;
    }
  }

  // ── Step 4: Send message ────────────────────────────────────────────────────
  try {
    const sentMessage = await retryWithBackoff(
      () => sendZipMessage(channel, { name: baseName, link: megaLink, password }, config),
      {
        retries: 3,
        delaysMs: [2000, 5000, 10000],
        onAttemptFail: (attempt, err) =>
          console.warn(`[index] Message send attempt ${attempt} failed for "${filename}": ${err.message}`),
      }
    );

    const messageId = sentMessage?.id || null;
    updateState(filename, { status: 'message_sent', messageId, error: null });

    appendLog({
      filename,
      megaLink,
      zipPassword: password,
      channelId: channel.id,
      channelName: channel.name,
      messageId,
    });

    console.log(`[index] ✅ Pipeline complete for "${filename}".`);
  } catch (err) {
    console.error(`[index] Webhook message failed for "${filename}": ${err.message}`);
    updateState(filename, { status: 'failed', error: `Webhook message failed: ${err.message}` });
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function safeDelete(filePath) {
  if (filePath && fs.existsSync(filePath)) {
    try { fs.unlinkSync(filePath); } catch {}
  }
}

function retryFile(filename) {
  const state = getState(filename);
  if (state) updateState(filename, { status: 'pending', error: null });

  if (state?.encryptedPath && fs.existsSync(state.encryptedPath)) {
    enqueue(filename, null);
    return;
  }

  for (const folder of [watchFolderPath, stagingFolderPath]) {
    const zipPath = path.join(folder, filename);
    if (fs.existsSync(zipPath)) { enqueue(filename, zipPath); return; }
  }

  console.error(`[index] Cannot retry "${filename}" — source file missing.`);
  updateState(filename, { status: 'failed', error: 'Source file missing, cannot retry.' });
}

// ── Ingest entry points ────────────────────────────────────────────────────────

/**
 * Called by the download engine after downloading a file from a MEGA link.
 * Stages the raw zip and feeds it into the encrypt→upload→post pipeline.
 *
 * @param {string}      downloadedPath   - Temp path of the downloaded file.
 * @param {string}      suggestedFilename
 * @param {string|null} sourcePassword   - Password to decrypt the zip (if encrypted).
 * @param {object}      [meta]           - Extra context (e.g. sourceCategoryName).
 */
function ingestDownloadedFile(downloadedPath, suggestedFilename, sourcePassword, meta = {}) {
  try {
    const baseName = path.basename(suggestedFilename, path.extname(suggestedFilename)) || 'recovered-file';

    // Use a unique filename — skip collision with already-sent files
    let finalFilename = `${baseName}.zip`;
    let counter = 2;
    while (getState(finalFilename)?.status === 'message_sent') {
      finalFilename = `${baseName}-${counter}.zip`;
      counter++;
    }

    const stagedPath = path.join(stagingFolderPath, finalFilename);
    fs.copyFileSync(downloadedPath, stagedPath);
    safeDelete(downloadedPath); // clean up temp download immediately

    updateState(finalFilename, {
      status: 'new',
      megaLink: null,
      zipPassword: null,
      encryptedPath: null,
      sourcePassword: sourcePassword || null,
      sourceCategoryName: meta.sourceCategoryName || null,
      error: null,
    });

    console.log(`[index] Staged "${finalFilename}" → full pipeline.`);
    enqueue(finalFilename, stagedPath, meta);
  } catch (err) {
    console.error(`[index] Failed to ingest downloaded file "${downloadedPath}": ${err.message}`);
    safeDelete(downloadedPath);
  }
}

/**
 * Downloads a file from MEGA, then feeds it through the full pipeline.
 * Used by: mirror engine, TXT ingester, GUI MEGA-link form.
 *
 * @param {string}      name     - Display name for the file.
 * @param {string}      link     - MEGA link to download from.
 * @param {string|null} password - Source zip password (for decryption).
 * @param {object}      [meta]   - Extra context (e.g. sourceCategoryName).
 */
async function downloadAndIngest(name, link, password, meta = {}) {
  if (!fs.existsSync(downloadFolderPath)) fs.mkdirSync(downloadFolderPath, { recursive: true });

  const safeName = (name || 'link').replace(/[\\/:*?"<>|]/g, '-').trim().slice(0, 80);
  const tempPath = path.join(downloadFolderPath, `${Date.now()}-${safeName}.zip`);

  console.log(`[index] Downloading "${name}" from MEGA: ${link}`);
  try {
    const result = await downloadManager.downloadMegaFile(link, tempPath, {
      timeoutMs: config.downloadEngine?.timeoutMs || 180000,
    });
    const remoteName = result?.name || null;
    const finalName = remoteName || safeName;
    console.log(`[index] Downloaded "${finalName}" — feeding into pipeline.`);
    ingestDownloadedFile(tempPath, `${path.basename(finalName, path.extname(finalName))}.zip`, password, meta);
  } catch (err) {
    console.error(`[index] Download failed for "${name}" (${link}): ${err.message}`);
    safeDelete(tempPath);
  }
}

/**
 * Directly ingests a zip file that was uploaded via the GUI.
 * This bypasses the folder watcher (useful for hosted environments without FS events).
 *
 * @param {string} zipPath - Path where multer saved the file.
 * @param {string} [password] - Optional source password.
 */
function ingestUploadedZip(zipPath, password) {
  const filename = path.basename(zipPath);
  const state = getState(filename);

  // Size-based skip for identical already-processed files
  if (state?.status === 'message_sent') {
    const currentSize = fs.existsSync(zipPath) ? fs.statSync(zipPath).size : null;
    if (currentSize !== null && state.fileSize && currentSize === state.fileSize) {
      console.log(`[index] Skipping GUI upload "${filename}" — already processed (same size).`);
      safeDelete(zipPath); // uploaded file not needed
      return;
    }
    removeState(filename);
  }

  updateState(filename, {
    status: 'new',
    sourcePassword: password || config.zipInputPassword || null,
    error: null,
  });
  enqueue(filename, zipPath);
}

// ── TXT handler ────────────────────────────────────────────────────────────────

function handleTxtFile(txtPath) {
  const defaultName = path.basename(txtPath, '.txt');
  try {
    const jobs = parseTxtFile(txtPath, defaultName);
    if (jobs.length === 0) {
      console.log(`[index] No MEGA links found in "${path.basename(txtPath)}".`);
      return;
    }
    console.log(`[index] Found ${jobs.length} link(s) in "${path.basename(txtPath)}" — queuing downloads.`);
    for (const job of jobs) {
      downloadAndIngest(job.name, job.link, job.password).catch((err) =>
        console.error(`[index] Error ingesting "${job.name}": ${err.message}`)
      );
    }
  } catch (err) {
    console.error(`[index] Failed to parse "${path.basename(txtPath)}": ${err.message}`);
  }
}

// ── Main ────────────────────────────────────────────────────────────────────────

async function main() {
  console.log('[index] mega-discord-bot starting...');
  console.log(`[index] Watch folder: ${watchFolderPath}`);
  console.log(`[index] Pipeline concurrency: ${MAX_CONCURRENT}`);

  const client = await getClient(config);
  console.log(`[index] Discord bot logged in as ${client.user.tag}`);

  if (config.discordClientId) {
    try {
      await registerCommands(config);
      attachCommandHandler(client, config);
      console.log('[index] Registered slash commands: /settemplate /toggleembed /updateposts /status');
    } catch (err) {
      console.warn(`[index] Could not register slash commands: ${err.message}`);
    }
  } else {
    console.warn('[index] discordClientId not set — slash commands unavailable.');
  }

  startGuiServer(config, {
    onRetry: retryFile,
    onDownloadPause: pauseDownloads,
    onDownloadResume: resumeDownloads,
    onDownloadCancel: cancelDownloads,
    onIngestLink: downloadAndIngest,
    onIngestZip: ingestUploadedZip,   // direct pipeline trigger for GUI uploads
    watchFolderPath,
  });

  // Watch for new files in watched-folder/
  watchFolder(
    watchFolderPath,
    (zipPath) => {
      const filename = path.basename(zipPath);
      const state = getState(filename);
      if (state?.status === 'message_sent') {
        const currentSize = fs.existsSync(zipPath) ? fs.statSync(zipPath).size : null;
        if (currentSize !== null && state.fileSize && currentSize === state.fileSize) {
          console.log(`[index] Skipping "${filename}" — already processed (same size).`);
          return;
        }
      }
      enqueue(filename, zipPath);
    },
    (txtPath) => handleTxtFile(txtPath)
  );

  // Init the download manager (needed even when download engine is disabled,
  // so mirror engine and TXT ingester can use downloadMegaFile directly).
  downloadManager.init(config);

  startDownloadEngine(config, ingestDownloadedFile);
  startMirrorEngine(config);

}

main().catch((err) => {
  console.error('[index] Fatal startup error:', err.message);
  process.exit(1);
});

process.on('SIGINT', () => {
  console.log('\n[index] Shutting down...');
  stopMirrorEngine();
  process.exit(0);
});

process.on('SIGTERM', () => {
  stopMirrorEngine();
  process.exit(0);
});
