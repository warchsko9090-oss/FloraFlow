(() => {
  const view = document.getElementById("view");

  function esc(s) {
    return String(s || "").replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[c]));
  }

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
    if (!p || !apps) return "";
    if ((p === "sale" || p === "client") && apps.sale) return "/tg/sale";
    if ((p === "buh" || p === "upd") && apps.buh) return "/tg/sale?tab=buh";
    if (p === "cash" && apps.cash) return "/tg/pay/cash";
    if ((p === "pay" || p === "payment") && apps.pay) return "/tg/pay";
    return "";
  }

  function showMenu(me) {
    const tiles = (me && me.tiles) || [];
    const rows = tiles.map((t) => (
      `<a class="tab" href="${esc(t.href)}">${esc(t.title)}<span>${esc(t.hint)}</span></a>`
    )).join("");
    view.innerHTML = `
      <div class="brand">FloraFlow</div>
      <p class="sub">Разделы по вашей роли. Синяя кнопка бота открывает это меню.</p>
      <div class="tabs">${rows}</div>
    `;
  }

  async function boot() {
    const go = (me) => {
      const dest = pathFor((me && me.apps) || {}, startParam());
      if (dest) {
        location.replace(dest);
        return;
      }
      const tiles = (me && me.tiles) || [];
      if (tiles.length) {
        showMenu(me);
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
