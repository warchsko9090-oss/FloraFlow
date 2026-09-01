/* Shared Telegram Mini App auth. iPhone: no initData in GET URL. */
(function (w) {
  function tgApp() {
    return w.Telegram && w.Telegram.WebApp;
  }

  function decodeVal(v) {
    try { return decodeURIComponent(String(v || "").replace(/\+/g, "%20")); }
    catch (_) { return String(v || ""); }
  }

  function paramsFromHash(hash) {
    const h = String(hash || "").replace(/^#/, "");
    const out = {};
    if (!h) return out;
    h.split("&").forEach((part) => {
      const eq = part.indexOf("=");
      if (eq < 0) return;
      out[decodeVal(part.slice(0, eq))] = part.slice(eq + 1);
    });
    return out;
  }

  function fromLaunchParams(raw) {
    if (!raw) return "";
    if (typeof raw !== "string") {
      try { raw = JSON.stringify(raw); } catch (_) { return ""; }
    }
    if (raw.includes("hash=") && (raw.includes("user=") || raw.includes("user%3D"))) {
      if (raw.indexOf("tgWebAppData=") >= 0) {
        const p = paramsFromHash(raw);
        return decodeVal(p.tgWebAppData || "");
      }
      return raw.indexOf("%") >= 0 ? decodeVal(raw) : raw;
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
    const hp = paramsFromHash(w.location.hash);
    if (hp.tgWebAppData) return decodeVal(hp.tgWebAppData);
    try {
      const sp = new URLSearchParams(w.location.search);
      const q = sp.get("tgWebAppData") || sp.get("initData") || "";
      if (q) return q;
    } catch (_) {}
    const stored = fromStorage();
    if (stored) return stored;
    return "";
  }

  function remember(initData) {
    if (!initData) return;
    try { w.sessionStorage.setItem("telegram-web-app-init-data", initData); } catch (_) {}
  }

  function waitTelegram(maxMs) {
    const max = maxMs || 8000;
    return new Promise((resolve) => {
      const start = Date.now();
      let done = false;
      const finish = (web) => {
        if (done) return;
        done = true;
        const data = getInitData();
        remember(data);
        resolve(web || tgApp() || null);
      };
      const tick = () => {
        const web = tgApp();
        if (web) {
          try { web.ready(); web.expand(); } catch (_) {}
        }
        if (getInitData() || Date.now() - start > max) return finish(web);
        setTimeout(tick, 50);
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
      return "iPhone не передал вход Telegram. Закройте мини-приложение полностью и откройте его кнопкой в боте.";
    }
    if (data && data.hint === "bad_signature") {
      return "Подпись Telegram не принята. Обновите мини-приложение: закройте и откройте из бота.";
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
      body: JSON.stringify({ initData: initData }),
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

  w.FFTg = { tgApp, getInitData, waitTelegram, handshake, api, authErrorMessage, remember };
})(window);
