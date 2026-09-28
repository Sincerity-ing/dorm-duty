"use strict";

/*
 * 宿舍值日安排 —— 零依赖同步服务
 * 功能：
 *   1. 托管前端静态页面（index.html 等）
 *   2. 休息日数据 API（单日增删，避免多人同时修改互相覆盖）
 *   3. SSE 实时推送（/api/events），任一设备修改后其他设备立即更新
 *   4. 数据持久化到 data.json
 * 运行：node server.js   （端口可用环境变量 PORT 覆盖）
 */

const http = require("http");
const fs = require("fs");
const fsp = fs.promises;
const path = require("path");

const ROOT = __dirname;
const DATA_FILE = path.join(ROOT, "data.json");
const PORT = process.env.PORT || 8765;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/* ---------- 数据状态 ---------- */
let state = { rev: 0, days: [] };   // rev 单调递增；days 为 "YYYY-MM-DD" 数组
const clients = new Set();

function loadState() {
  try {
    const parsed = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    if (Number.isInteger(parsed.rev) && Array.isArray(parsed.days)) {
      state = { rev: parsed.rev, days: parsed.days.filter(function (d) { return DATE_RE.test(d); }) };
    }
  } catch (e) {
    if (e.code !== "ENOENT") console.error("读取 data.json 失败：" + e.message);
  }
}

async function persist() {
  const tmp = DATA_FILE + ".tmp";
  await fsp.writeFile(tmp, JSON.stringify(state));
  await fsp.rename(tmp, DATA_FILE);
}

function setRestDay(date, rest) {
  const has = state.days.indexOf(date) !== -1;
  let changed = false;
  if (rest && !has) { state.days.push(date); changed = true; }
  if (!rest && has) { state.days = state.days.filter(function (d) { return d !== date; }); changed = true; }
  if (changed) { state.rev++; state.days.sort(); }
  return changed;
}

/* ---------- SSE 推送 ---------- */
function broadcast() {
  const payload = "data: " + JSON.stringify({ type: "state", rev: state.rev, days: state.days }) + "\n\n";
  clients.forEach(function (res) {
    try { res.write(payload); } catch (e) { /* 忽略已断开连接 */ }
  });
}

function handleSSE(req, res) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-store, no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no"
  });
  res.write("retry: 3000\n");
  res.write("data: " + JSON.stringify({ type: "state", rev: state.rev, days: state.days }) + "\n\n");
  clients.add(res);

  const timer = setInterval(function () {
    try { res.write(": ping\n\n"); } catch (e) { /* noop */ }
  }, 25000);

  req.on("close", function () {
    clearInterval(timer);
    clients.delete(res);
  });
}

/* ---------- HTTP 辅助 ---------- */
function sendJSON(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(body);
}

function readBody(req) {
  return new Promise(function (resolve, reject) {
    const chunks = [];
    let len = 0;
    req.on("data", function (c) {
      len += c.length;
      if (len > 64 * 1024) {
        reject(new Error("请求体过大"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", function () { resolve(Buffer.concat(chunks).toString("utf8")); });
    req.on("error", reject);
  });
}

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".ico": "image/x-icon",
  ".svg": "image/svg+xml",
  ".txt": "text/plain; charset=utf-8"
};

async function serveStatic(res, urlPath) {
  let rel = decodeURIComponent(urlPath);
  if (rel === "/") rel = "/index.html";
  const normalized = path.normalize(rel).replace(/^(\.\.[\\/])+/, "");
  const filePath = path.join(ROOT, normalized);
  if (filePath.indexOf(ROOT) !== 0) { sendJSON(res, 403, { error: "forbidden" }); return; }

  const ext = path.extname(filePath).toLowerCase();
  if (!MIME[ext]) { sendJSON(res, 404, { error: "not found" }); return; }
  try {
    const data = await fsp.readFile(filePath);
    res.writeHead(200, { "Content-Type": MIME[ext] });
    res.end(data);
  } catch (e) {
    sendJSON(res, 404, { error: "not found" });
  }
}

/* ---------- 服务入口 ---------- */
const server = http.createServer(async function (req, res) {
  const p = new URL(req.url, "http://localhost").pathname;
  try {
    if (req.method === "GET" && p === "/api/health") {
      sendJSON(res, 200, { ok: true });
      return;
    }
    if (req.method === "GET" && p === "/api/state") {
      sendJSON(res, 200, { rev: state.rev, days: state.days });
      return;
    }
    if (req.method === "GET" && p === "/api/events") {
      handleSSE(req, res);
      return;
    }
    if (req.method === "POST" && p === "/api/rest-days") {
      const body = JSON.parse((await readBody(req)) || "{}");
      if (!DATE_RE.test(body.date || "")) { sendJSON(res, 400, { error: "日期格式不正确" }); return; }
      const rest = body.rest !== false;
      const changed = setRestDay(body.date, rest);
      if (changed) {
        await persist();
        broadcast();
      }
      sendJSON(res, 200, { rev: state.rev, days: state.days });
      return;
    }
    if (req.method === "POST" && p === "/api/rest-days/clear") {
      const changed = state.days.length > 0;
      state.days = [];
      if (changed) {
        state.rev++;
        await persist();
        broadcast();
      }
      sendJSON(res, 200, { rev: state.rev, days: state.days });
      return;
    }
    if (req.method === "GET") {
      await serveStatic(res, p);
      return;
    }
    sendJSON(res, 404, { error: "not found" });
  } catch (e) {
    sendJSON(res, 500, { error: e.message || "server error" });
  }
});

loadState();
server.listen(PORT, function () {
  console.log("宿舍值日服务已启动，端口：" + PORT);
});
