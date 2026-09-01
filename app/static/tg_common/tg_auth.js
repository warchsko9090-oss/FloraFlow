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

  function waitTelegram(maxMs) {
    const max = maxMs || 1200;
    return new Promise((resolve) => {
      const start = Date.now();
      let done = false;
      const finish = (web) => {
        if (done) return;
        done = true;
        remember(getInitData());
        resolve(web || tgApp() || null);
      };
      const tick = () => {
        const web = tgApp();
        if (web) {
          try { web.ready(); web.expand(); } catch (_) {}
        }
        if (getInitData() || Date.now() - start > max) return finish(web);
        setTimeout(tick, 40);
      };
      w.addEventListener("hashchange", () => {
        if (getInitData()) finish(tgApp());
      });
      tick();
    });
  }

  function authErrorMessage(data, status) {
    if (status === 403 && data && data.error === "not_linked") {
      return (
        "Telegram не привязан к ERP. Ваш id: " + data.telegram_id
        + (data.username ? " (@" + data.username + ")" : "")
        + ". Добавьте в Amvera TG_USER_ID_MAP: " + data.telegram_id + ":admin"
      );
    }
    if (data && data.hint === "no_init_data") {
      return "Telegram не передал вход. Откройте мини-приложение кнопкой внизу чата с ботом (не ссылкой).";
    }
    if (data && data.hint === "bad_signature") {
      return "Подпись Telegram не принята. Закройте мини-приложение и откройте его кнопкой в боте.";
    }
    return (data && (data.error || data.hint)) || "Нет входа. Откройте Mini App из бота.";
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

  async function api(path, opts) {
    const initData = getInitData();
    const headers = Object.assign({}, (opts && opts.headers) || {});
    if (initData && initData.length < 4000) {
      headers["X-Telegram-Init-Data"] = initData;
    }
    const method = ((opts && opts.method) || "GET").toUpperCase();
    let body = opts && opts.body;
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
    const res = await fetch(path, Object.assign({}, opts, {
      credentials: "same-origin",
      method: method,
      headers: headers,
      body: body,
    }));
    const data = await res.json().catch(() => ({}));
    if (res.status === 401) throw new Error(authErrorMessage(data, res.status));
    if (!res.ok) throw new Error(authErrorMessage(data, res.status));
    return data;
  }

  w.FFTg = { tgApp, getInitData, waitTelegram, handshake, api, authErrorMessage, remember, debugInfo };
})(window);
