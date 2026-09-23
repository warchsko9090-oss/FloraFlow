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
        + ". Войдите логином и паролем один раз — привязка сохранится."
      );
    }
    if (err === "bad_credentials") {
      return "Неверный логин или пароль [bad_credentials].";
    }
    if (err === "need_credentials") {
      return "Введите логин и пароль ERP [need_credentials].";
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
      return (data && data.hint) || ("Нет доступа к этому приложению [" + (err || "forbidden") + "].");
    }
    if (err === "unauthorized" || status === 401) {
      return "Нет входа [" + (err || "unauthorized") + "]. Войдите логином ERP или откройте Mini App из бота.";
    }
    if (err) return String((data && data.hint) || err) + (status ? " [" + status + "]" : "");
    return "Нет входа. Откройте Mini App из бота или войдите логином ERP.";
  }

  function rememberedUsername() {
    try { return w.localStorage.getItem("ff_mini_username") || ""; } catch (_) { return ""; }
  }

  function rememberUsername(name) {
    try {
      if (name) w.localStorage.setItem("ff_mini_username", name);
    } catch (_) {}
  }

  /**
   * Форма логина ERP. После успеха сервер привязывает Telegram id навсегда —
   * пароль в приложении не хранится.
   */
  function promptLogin(container, opts) {
    const options = opts || {};
    const loginUrl = options.loginUrl;
    const title = options.title || "Вход в ERP";
    const hint = options.hint || "Один раз: логин и пароль. Telegram привяжется навсегда — дальше вход через кнопку бота.";
    const saved = rememberedUsername();
    container.innerHTML = `
      <div class="ff-login">
        <h2>${title}</h2>
        <p class="ff-login-hint">${hint}</p>
        <label class="ff-login-label">Логин
          <input type="text" id="ffLoginUser" autocomplete="username" value="${saved.replace(/"/g, "&quot;")}">
        </label>
        <label class="ff-login-label">Пароль
          <input type="password" id="ffLoginPass" autocomplete="current-password">
        </label>
        <p class="ff-login-err" id="ffLoginErr" hidden></p>
        <button type="button" class="ff-login-btn" id="ffLoginBtn">Войти</button>
      </div>
    `;
    if (!document.getElementById("ff-login-style")) {
      const st = document.createElement("style");
      st.id = "ff-login-style";
      st.textContent = `
        .ff-login{padding:8px 4px 24px;max-width:360px;margin:0 auto}
        .ff-login h2{font-size:20px;margin:8px 0 6px;color:inherit}
        .ff-login-hint{font-size:13px;opacity:.75;line-height:1.4;margin:0 0 16px}
        .ff-login-label{display:block;font-size:12px;font-weight:600;margin:0 0 10px}
        .ff-login-label input{display:block;width:100%;margin-top:4px;padding:12px 14px;border-radius:12px;
          border:1px solid rgba(0,0,0,.12);font-size:16px;box-sizing:border-box;background:#fff;color:#111}
        .ff-login-btn{width:100%;margin-top:8px;padding:14px;border:0;border-radius:14px;
          background:#1B5E20;color:#fff;font-weight:700;font-size:15px}
        .ff-login-btn:disabled{opacity:.55}
        .ff-login-err{color:#b91c1c;font-size:13px;margin:8px 0}
      `;
      document.head.appendChild(st);
    }
    const userEl = document.getElementById("ffLoginUser");
    const passEl = document.getElementById("ffLoginPass");
    const errEl = document.getElementById("ffLoginErr");
    const btn = document.getElementById("ffLoginBtn");
    const showErr = (msg) => {
      errEl.hidden = !msg;
      errEl.textContent = msg || "";
    };
    const submit = async () => {
      showErr("");
      const username = (userEl.value || "").trim();
      const password = passEl.value || "";
      if (!username || !password) {
        showErr("Введите логин и пароль");
        return;
      }
      btn.disabled = true;
      try {
        loadSdk();
        applyWeb();
        await waitTelegram(800);
        const initData = getInitData();
        remember(initData);
        const res = await fetch(loginUrl, {
          method: "POST",
          credentials: "same-origin",
          headers: {
            "Content-Type": "application/json",
            ...(initData && initData.length < 4000 ? { "X-Telegram-Init-Data": initData } : {}),
          },
          body: JSON.stringify({ username, password, initData: initData || "", debug: debugInfo() }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(authErrorMessage(data, res.status));
        rememberUsername(username);
        passEl.value = "";
        if (typeof options.onSuccess === "function") options.onSuccess(data);
      } catch (e) {
        showErr(e.message || "Ошибка входа");
      } finally {
        btn.disabled = false;
      }
    };
    btn.addEventListener("click", submit);
    passEl.addEventListener("keydown", (ev) => { if (ev.key === "Enter") submit(); });
    userEl.addEventListener("keydown", (ev) => { if (ev.key === "Enter") passEl.focus(); });
    setTimeout(() => { (saved ? passEl : userEl).focus(); }, 50);
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
   * Перед send-pdf / скачиванием: ждём initData, иначе пробуем cookie-сессию.
   * Серая reply-кнопка на iOS часто без initData — cookie.tg достаточно.
   */
  async function ensureAuth(authUrl) {
    loadSdk();
    applyWeb();
    let initData = getInitData();
    if (!initData) {
      await waitTelegram(1500);
      initData = getInitData();
    }
    if (initData) remember(initData);
    if (authUrl) {
      const me = await handshake(authUrl);
      if (me && me.has_telegram === false) {
        throw new Error(authErrorMessage({ error: "no_telegram_id", hint: "reopen_from_bot" }, 401));
      }
      return me;
    }
    if (!initData) {
      throw new Error(authErrorMessage({ hint: "no_init_data", error: "no_init_data" }, 401));
    }
    return { has_telegram: true };
  }

  /** Вкладки Оплата / Клиентам — одна синяя кнопка, разделы внутри. */
  function mountAppTabs(me, active) {
    const apps = (me && me.apps) || {};
    const canPay = apps.pay === true || (apps.pay == null && active === "pay");
    const canSale = apps.sale === true || (apps.sale == null && active === "sale");
    if (!(canPay && canSale)) return;
    if (document.getElementById("ff-app-tabs")) return;
    const bar = document.createElement("nav");
    bar.id = "ff-app-tabs";
    bar.innerHTML = `
      <a class="ff-tab${active === "pay" ? " is-on" : ""}" href="/tg/pay">Оплата</a>
      <a class="ff-tab${active === "sale" ? " is-on" : ""}" href="/tg/sale">Клиентам</a>
    `;
    const app = document.getElementById("app");
    if (app && app.firstChild) app.insertBefore(bar, app.firstChild);
    else document.body.insertBefore(bar, document.body.firstChild);
    if (!document.getElementById("ff-app-tabs-style")) {
      const st = document.createElement("style");
      st.id = "ff-app-tabs-style";
      st.textContent = `
        #ff-app-tabs{display:flex;gap:6px;padding:10px 12px 0;position:sticky;top:0;z-index:20;
          background:inherit;backdrop-filter:blur(8px)}
        #ff-app-tabs .ff-tab{flex:1;text-align:center;padding:10px 8px;border-radius:12px;
          text-decoration:none;font-weight:700;font-size:13px;color:inherit;
          border:1px solid rgba(0,0,0,.1);background:rgba(255,255,255,.55)}
        #ff-app-tabs .ff-tab.is-on{background:#1B5E20;color:#fff;border-color:#1B5E20}
      `;
      document.head.appendChild(st);
    }
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
    api, fetchBlob, authErrorMessage, remember, debugInfo, applyWeb, promptLogin,
    mountAppTabs,
  };
})(window);
