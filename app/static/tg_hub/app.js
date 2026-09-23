(() => {
  const view = document.getElementById("view");

  function startParam() {
    try {
      const tg = window.FFTg && window.FFTg.tgApp && window.FFTg.tgApp();
      const sp = (tg && (tg.initDataUnsafe && tg.initDataUnsafe.start_param)) || "";
      if (sp) return String(sp).toLowerCase();
    } catch (_) {}
    const q = new URLSearchParams(location.search || "");
    return (q.get("startapp") || q.get("tab") || "").toLowerCase();
  }

  function pathFor(apps, prefer) {
    const p = (prefer || "").toLowerCase();
    if ((p === "sale" || p === "client") && apps.sale) return "/tg/sale";
    if ((p === "pay" || p === "payment") && apps.pay) return "/tg/pay";
    if (apps.pay && apps.sale) return "/tg/pay";
    if (apps.sale) return "/tg/sale";
    if (apps.pay) return "/tg/pay";
    return "";
  }

  function showChooser(apps) {
    const rows = [];
    if (apps.pay) {
      rows.push(`<a class="tab" href="/tg/pay">Оплата<span>Счета поставщикам</span></a>`);
    }
    if (apps.sale) {
      rows.push(`<a class="tab" href="/tg/sale">Клиентам<span>Выставить счёт</span></a>`);
    }
    view.innerHTML = `
      <div class="brand">FloraFlow</div>
      <p class="sub">Выберите раздел. Позже сюда же добавим новые вкладки.</p>
      <div class="tabs">${rows.join("")}</div>
      <p class="hint">Открывайте Mini App синей кнопкой меню бота — так Telegram передаёт вход стабильно.</p>
    `;
  }

  async function boot() {
    const go = (me) => {
      const apps = (me && me.apps) || {};
      const prefer = startParam();
      const dest = pathFor(apps, prefer) || me.default_path || pathFor(apps);
      if (dest) {
        location.replace(dest);
        return;
      }
      if (apps.pay || apps.sale) {
        showChooser(apps);
        return;
      }
      view.innerHTML = `<p class="err">Нет доступных разделов для вашей роли.</p>`;
    };

    const showLogin = (hint) => {
      window.FFTg.promptLogin(view, {
        loginUrl: "/tg/api/login",
        title: "Вход — FloraFlow",
        hint: hint || "Один раз логин и пароль ERP. Telegram привяжется навсегда.",
        onSuccess: go,
      });
    };

    try {
      go(await window.FFTg.bootAuth("/tg/api/auth"));
    } catch (e) {
      try {
        go(await window.FFTg.handshake("/tg/api/auth"));
      } catch (e2) {
        showLogin(e2.message || e.message);
      }
    }
  }

  boot().catch((e) => {
    view.innerHTML = `<p class="err">${(e && e.message) || "Ошибка"}</p>`;
  });
})();
