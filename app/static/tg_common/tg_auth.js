/* Shared Telegram Mini App auth. */
(function (w) {
  function tgApp() {
    return w.Telegram && w.Telegram.WebApp;
  }

  function decodeVal(v) {
    try { return decodeURIComponent(String(v || "").replace(/\+/g, "%20")); }
    catch (_) { return String(v || ""); }
  }

  function maybeDecode(s) {
    let cur = String(s || "");
    for (let n = 0; n < 2; n++) {
      const next = decodeVal(cur);
      if (next === cur) break;
      cur = next;
      if (cur.indexOf("hash=") >= 0 && (cur.indexOf("user=") >= 0 || cur.indexOf("query_id=") >= 0 || cur.indexOf("chat_instance=") >= 0)) {
        break;
      }
    }
    return cur;
  }

  /* iOS: #/?tgWebAppData=...  Desktop: #tgWebAppData=...
     Value may be encoded (inner & as %26) or raw until &tgWebApp. */
  function extractTgWebAppData(src) {
    const s = String(src || "");
    const key = "tgWebAppData=";
    const i = s.indexOf(key);
    if (i < 0) return "";
    let rest = s.slice(i + key.length);
    const next = rest.search(/&tgWebApp/);
    if (next >= 0) rest = rest.slice(0, next);
    return maybeDecode(rest);
  }

  function fromLaunchParams(raw) {
    if (!raw) return "";
    if (typeof raw !== "string") {
      try { raw = JSON.stringify(raw); } catch (_) { return ""; }
    }
    const fromKey = extractTgWebAppData(raw);
    if (fromKey) return fromKey;
    if (raw.includes("hash=") && (raw.includes("user=") || raw.includes("user%3D"))) {
      return raw.indexOf("%") >= 0 ? maybeDecode(raw) : raw;
    }
    try {
      const o = JSON.parse(raw);
      return o.tgWebAppData || o.initData || o["telegram-web-app-init-data"] || "";
    } catch (_) {
      return "";
    }
  }

  function fromStorage() {
    try {
      const keys = [
        "__telegram__initParams",
        "telegram-web-app-init-data",
        "telegram-apps/launch-params",
      ];
      for (const store of [w.sessionStorage, w.localStorage]) {
        if (!store) continue;
        for (const k of keys) {
          const got = fromLaunchParams(store.getItem(k));
          if (got) return got;
        }
        for (let i = 0; i < store.length; i++) {
          const got = fromLaunchParams(store.getItem(store.key(i)));
          if (got) return got;
        }
      }
    } catch (_) {}
    return "";
  }

  function getInitData() {
    const web = tgApp();
    if (web && web.initData) return web.initData;
    const launch = w.__ffLaunch || {};
    const sources = [
      w.location.hash,
      w.location.href,
      w.location.search,
      launch.hash,
      launch.href,
      launch.search,
    ];
    for (const src of sources) {
      const got = extractTgWebAppData(src);
      if (got) return got;
    }
    try {
      const sp = new URLSearchParams(w.location.search);
      const q = sp.get("tgWebAppData") || sp.get("initData") || "";
      if (q) return q;
    } catch (_) {}
    return fromStorage();
  }

  function remember(initData) {
    if (!initData) return;
    try { w.sessionStorage.setItem("telegram-web-app-init-data", initData); } catch (_) {}
  }

  function debugInfo() {
    const web = tgApp();
    const hash = String((w.__ffLaunch && w.__ffLaunch.hash) || w.location.hash || "");
    return {
      hash_len: hash.length,
      hash_head: hash.slice(0, 140),
      href_head: String(w.location.href || "").split("#")[0].slice(-80),
      has_tg: !!w.Telegram,
      has_webapp: !!web,
      init_len: web && web.initData ? String(web.initData).length : 0,
      parsed_len: (getInitData() || "").length,
      platform: (web && web.platform) || "",
      has_proxy: !!(w.TelegramWebviewProxy || w.TelegramWebview),
    };
  }

  function applyWeb() {
    const web = tgApp();
    if (web) {
      try { web.ready(); web.expand(); } catch (_) {}
    }
    return web || null;
  }

  function loadSdk() {
    if (tgApp() || w.__ffSdkLoading) return;
    const src = document.documentElement.getAttribute("data-tg-sdk");
    if (!src) return;
    w.__ffSdkLoading = true;
    const s = document.createElement("script");
    s.src = src;
    s.async = true;
    s.onload = function () { applyWeb(); };
    document.head.appendChild(s);
  }

  function waitTelegram(maxMs) {
    const max = maxMs == null ? 1200 : maxMs;
    return new Promise((resolve) => {
      const start = Date.now();
      let done = false;
      const finish = (web) => {
        if (done) return;
        done = true;
        remember(getInitData());
        resolve(web || applyWeb());
      };
      const tick = () => {
        applyWeb();
        if (getInitData() || Date.now() - start > max) return finish(tgApp());
        setTimeout(tick, 40);
      };
      w.addEventListener("hashchange", () => {
        if (getInitData()) finish(tgApp());
      });
      tick();
    });
  }

  function authErrorMessage(data, status) {
    const err = (data && (data.error || data.hint)) || "";
    if (status === 403 && data && data.error === "not_linked") {
      return (
        "Telegram не привязан к ERP. Ваш id: " + data.telegram_id
        + (data.username ? " (@" + data.username + ")" : "")
        + ". Добавьте в Amvera TG_USER_ID_MAP: " + data.telegram_id + ":admin"
      );
    }
    if (err === "no_init_data" || (data && data.hint === "no_init_data")) {
      return "Telegram не передал вход [" + (err || "no_init_data") + "]. Откройте мини-приложение кнопкой внизу чата с ботом (не ссылкой).";
    }
    if (err === "stale_init_data" || (data && data.hint === "stale_init_data")) {
      return "Сессия Telegram устарела [" + (err || "stale_init_data") + "]. Закройте мини-приложение и откройте его снова кнопкой в боте.";
    }
    if (err === "bad_signature" || (data && data.hint === "bad_signature")) {
      return "Подпись Telegram не принята [" + (err || "bad_signature") + "]. Закройте мини-приложение и откройте его кнопкой в боте.";
    }
    if (err === "no_telegram_id" || err === "reopen_from_bot") {
      return "Не вижу ваш Telegram [" + (err || "no_telegram_id") + "]. Закройте мини-приложение и откройте его кнопкой в чате с ботом.";
    }
    if (err === "file_missing") {
      return "Файла нет на сервере [file_missing]. Прикрепите PDF в карточке или откройте копию в чате.";
    }
    if (err === "pdf_failed") {
      return "Не удалось сформировать PDF [pdf_failed]. Попробуйте ещё раз.";
    }
    if (err === "send_failed") {
      return "Бот не смог отправить файл в чат [send_failed]. Проверьте TG_BOT_TOKEN или повторите позже.";
    }
    if (err === "forbidden" || status === 403) {
      return "Нет доступа к этому приложению [" + (err || "forbidden") + "].";
    }
    if (err === "unauthorized" || status === 401) {
      return "Нет входа [" + (err || "unauthorized") + "]. Откройте Mini App из бота.";
    }
    if (err) return String(err) + (status ? " [" + status + "]" : "");
    return "Нет входа. Откройте Mini App из бота.";
  }

  /* Cookie с прошлого запуска или hash уже в URL — не ждём SDK. */
  async function bootAuth(authUrl) {
    loadSdk();
    applyWeb();
    try {
      const me = await handshake(authUrl);
      waitTelegram(800).then(applyWeb);
      return me;
    } catch (e) {
      await waitTelegram(1200);
      applyWeb();
      return handshake(authUrl);
    }
  }

  async function handshake(authUrl) {
    const initData = getInitData();
    remember(initData);
    const res = await fetch(authUrl, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ initData: initData, debug: debugInfo() }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(authErrorMessage(data, res.status));
    return data;
  }

  /**
   * Перед send-pdf / скачиванием: если initData пропал — ждём SDK и
   * заново handshake, иначе явная ошибка «откройте из кнопки бота».
   */
  async function ensureAuth(authUrl) {
    loadSdk();
    applyWeb();
    let initData = getInitData();
    if (!initData) {
      await waitTelegram(1500);
      initData = getInitData();
    }
    if (!initData) {
      throw new Error(authErrorMessage({ hint: "no_init_data", error: "no_init_data" }, 401));
    }
    remember(initData);
    if (authUrl) {
      const me = await handshake(authUrl);
      if (me && me.has_telegram === false) {
        throw new Error(authErrorMessage({ error: "no_telegram_id", hint: "reopen_from_bot" }, 401));
      }
      return me;
    }
    return { has_telegram: true };
  }

  async function api(path, opts) {
    const options = opts || {};
    if (options.ensureAuth) {
      const authUrl = typeof options.ensureAuth === "string"
        ? options.ensureAuth
        : (options.authUrl || "");
      if (authUrl) await ensureAuth(authUrl);
      else if (!getInitData()) {
        await waitTelegram(1200);
        if (!getInitData()) {
          throw new Error(authErrorMessage({ hint: "no_init_data", error: "no_init_data" }, 401));
        }
      }
    }
    const initData = getInitData();
    const headers = Object.assign({}, options.headers || {});
    if (initData && initData.length < 4000) {
      headers["X-Telegram-Init-Data"] = initData;
    }
    const method = (options.method || "GET").toUpperCase();
    let body = options.body;
    if (initData && body && typeof body === "string" && !(body instanceof FormData)) {
      try {
        const obj = JSON.parse(body);
        if (obj && typeof obj === "object" && !Array.isArray(obj) && !obj.initData) {
          obj.initData = initData;
          body = JSON.stringify(obj);
        }
      } catch (_) {}
    }
    if (body && !(body instanceof FormData) && !headers["Content-Type"]) {
      headers["Content-Type"] = "application/json";
    }
    const res = await fetch(path, Object.assign({}, options, {
      credentials: "same-origin",
      method: method,
      headers: headers,
      body: body,
    }));
    const data = await res.json().catch(() => ({}));
    if (res.status === 401 || res.status === 403) throw new Error(authErrorMessage(data, res.status));
    if (!res.ok) {
      if (data && (data.error || data.hint)) throw new Error(authErrorMessage(data, res.status));
      throw new Error(authErrorMessage(data, res.status));
    }
    return data;
  }

  async function fetchBlob(path, authUrl) {
    if (authUrl) await ensureAuth(authUrl);
    else if (!getInitData()) {
      await waitTelegram(1200);
      if (!getInitData()) {
        throw new Error(authErrorMessage({ hint: "no_init_data", error: "no_init_data" }, 401));
      }
    }
    const initData = getInitData();
    const headers = {};
    if (initData && initData.length < 4000) {
      headers["X-Telegram-Init-Data"] = initData;
    }
    const res = await fetch(path, { credentials: "same-origin", headers: headers });
    if (res.ok) return res.blob();
    const data = await res.json().catch(() => ({}));
    if (res.status === 404 || (data && data.error === "file_missing")) {
      throw new Error(authErrorMessage({ error: "file_missing" }, 404));
    }
    throw new Error(authErrorMessage(data, res.status));
  }

  w.FFTg = {
    tgApp, getInitData, waitTelegram, handshake, bootAuth, ensureAuth,
    api, fetchBlob, authErrorMessage, remember, debugInfo, applyWeb,
  };
})(window);
