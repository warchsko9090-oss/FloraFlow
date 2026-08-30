(() => {
  const tg = window.Telegram && window.Telegram.WebApp;
  if (tg) {
    tg.ready();
    tg.expand();
    try { tg.setHeaderColor('#F4F0E6'); tg.setBackgroundColor('#F4F0E6'); } catch (_) {}
  }

  const view = document.getElementById('view');
  const titleEl = document.getElementById('screenTitle');
  const devBar = document.getElementById('devBar');
  let me = null;
  let budgetItems = [];

  function money(n) {
    const v = Math.round(Number(n) || 0);
    return v.toLocaleString('ru-RU') + ' ₽';
  }

  function headers() {
    const h = {};
    if (tg && tg.initData) h['X-Telegram-Init-Data'] = tg.initData;
    const m = document.cookie.match(/(?:^|; )tg_pay_as=([^;]*)/);
    if (m) h['X-Tg-Pay-As'] = decodeURIComponent(m[1]);
    return h;
  }

  async function api(path, opts) {
    const res = await fetch(path, {
      credentials: 'same-origin',
      ...opts,
      headers: { ...headers(), ...(opts && opts.headers || {}) },
    });
    if (res.status === 401) throw new Error('Нет входа. Откройте Mini App из бота или локально /tg/pay.');
    const data = await res.json().catch(() => ({}));
    if (res.status === 403 && data.error === 'not_linked') {
      throw new Error(
        'Telegram не привязан к ERP. Ваш id: ' + data.telegram_id
        + (data.username ? ' (@' + data.username + ')' : '')
        + '. Добавьте в Amvera TG_USER_ID_MAP: ' + data.telegram_id + ':admin'
      );
    }
    if (!res.ok) throw new Error(data.error || data.hint || ('HTTP ' + res.status));
    return data;
  }

  function haptic(kind) {
    try { tg && tg.HapticFeedback && tg.HapticFeedback.impactOccurred(kind || 'light'); } catch (_) {}
  }

  function setTitle(t) { titleEl.textContent = t; }

  function route() {
    const hash = (location.hash || '#/').replace(/^#/, '');
    const parts = hash.split('/').filter(Boolean);
    if (parts[0] === 'inv' && parts[1]) return renderDetail(+parts[1]);
    if (parts[0] === 'new') return renderUpload();
    return renderList();
  }

  async function boot() {
    try {
      me = await api('/tg/pay/api/me');
    } catch (e) {
      view.innerHTML = `<div class="empty"><h2>Нет входа</h2><p>${e.message}</p></div>`;
      return;
    }
    if (me.dev) {
      devBar.hidden = false;
      devBar.querySelectorAll('button').forEach((b) => {
        b.classList.toggle('on', (document.cookie.match(/tg_pay_as=([^;]*)/) || [])[1] === b.dataset.as
          || (!document.cookie.includes('tg_pay_as') && b.dataset.as === 'admin' && me.can_edit)
          || (!document.cookie.includes('tg_pay_as') && b.dataset.as === 'payer' && !me.can_edit));
        b.onclick = () => {
          document.cookie = 'tg_pay_as=' + b.dataset.as + '; path=/; SameSite=Lax';
          location.href = '/tg/pay?as=' + b.dataset.as;
        };
      });
    }
    if (me.can_edit) {
      budgetItems = await api('/tg/pay/api/budget-items');
    }
    window.addEventListener('hashchange', route);
    route();
  }

  async function renderList() {
    setTitle('Счета на оплату');
    const data = await api('/tg/pay/api/invoices');
    const rows = data.invoices || [];
    const payRows = rows.filter((x) => x.status === 'new');
    const drafts = rows.filter((x) => x.status === 'draft');
    const shown = me.can_edit ? rows : payRows;

    let html = `
      <div class="hero">
        <div class="label">Итого к оплате</div>
        <div class="sum">${money(data.total_new)}</div>
        <div class="meta">${data.count_new} счёт(ов)${data.count_draft ? ' · ' + data.count_draft + ' черновик' : ''}</div>
      </div>
    `;
    if (!shown.length) {
      html += `<div class="empty"><h2>Пусто</h2><p>${me.can_edit ? 'Загрузите PDF счёта.' : 'Неоплаченных счетов нет.'}</p></div>`;
    } else {
      html += '<div class="list">';
      for (const inv of shown) {
        const badge = inv.status === 'draft' ? '<span class="badge">черновик</span>' : '';
        html += `<a class="row" href="#/inv/${inv.id}">
          <div>
            <div class="name">${badge}${esc(inv.summary)}</div>
            <div class="sub">${esc((inv.budget && inv.budget.name) || 'без статьи')}</div>
          </div>
          <div class="amt">${money(inv.amount)}</div>
        </a>`;
      }
      html += '</div>';
    }
    if (me.can_edit) {
      html += `<button class="fab" type="button" id="fabAdd" aria-label="Загрузить счёт">+</button>`;
    }
    view.innerHTML = html;
    const fab = document.getElementById('fabAdd');
    if (fab) fab.onclick = () => { haptic(); location.hash = '#/new'; };
  }

  async function renderDetail(id) {
    setTitle('Счёт');
    const inv = await api('/tg/pay/api/invoices/' + id);
    const can = me.can_edit;
    let lines = '';
    if (inv.lines && inv.lines.length) {
      lines = '<ul class="lines">' + inv.lines.map((ln) => {
        const q = [ln.qty, ln.unit].filter(Boolean).join(' ');
        const sum = ln.total != null ? money(ln.total) : '';
        return `<li><span>${esc(ln.description || '')}${q ? `<div class="q">${esc(q)}</div>` : ''}</span><span>${sum}</span></li>`;
      }).join('') + '</ul>';
    } else {
      lines = '<p class="hint">Состав не распознан — смотрите PDF.</p>';
    }

    let editor = '';
    if (can) {
      editor = `
        <div class="field"><label>Назначение</label>
          <textarea id="fSummary">${esc(inv.summary)}</textarea></div>
        <div class="field"><label>Сумма</label>
          <input id="fAmount" inputmode="decimal" value="${inv.amount || ''}"></div>
        <div class="field"><label>Статья</label>
          <select id="fBudget">
            <option value="">— не выбрана —</option>
            ${budgetItems.map((b) => `<option value="${b.id}" ${inv.budget_item_id === b.id ? 'selected' : ''}>${esc(b.code || '')} ${esc(b.name)}</option>`).join('')}
          </select>
        </div>
      `;
    } else {
      editor = `<div class="article">${esc((inv.budget && inv.budget.name) || 'Статья не указана')}</div>`;
    }

    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <div class="card">
        <div class="amount-xl">${money(inv.amount)}</div>
        <p class="hint" style="margin-top:10px">${esc(inv.summary)}</p>
        ${editor}
        ${lines}
        ${inv.has_file ? '' : '<p class="warn">Файл на диске не найден.</p>'}
        <button class="btn btn-brass" type="button" id="btnPdf">Скачать PDF</button>
        ${can && inv.status === 'draft' ? '<button class="btn btn-ink" type="button" id="btnConfirm">В оплату</button>' : ''}
        ${can ? '<button class="btn btn-ghost" type="button" id="btnDrop">Удалить</button>' : ''}
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    document.getElementById('btnPdf').onclick = () => sendPdf(inv);
    const conf = document.getElementById('btnConfirm');
    if (conf) conf.onclick = () => saveInv(inv.id, true);
    const drop = document.getElementById('btnDrop');
    if (drop) drop.onclick = async () => {
      if (!confirm('Удалить счёт?')) return;
      await api('/tg/pay/api/invoices/' + inv.id + '/discard', { method: 'POST' });
      location.hash = '#/';
    };
  }

  async function saveInv(id, confirmPay) {
    const body = {
      summary: (document.getElementById('fSummary') || {}).value,
      amount: (document.getElementById('fAmount') || {}).value,
      budget_item_id: (document.getElementById('fBudget') || {}).value || null,
      confirm: !!confirmPay,
    };
    view.classList.add('busy');
    try {
      await api('/tg/pay/api/invoices/' + id, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      haptic('medium');
      location.hash = '#/';
    } catch (e) {
      alert(e.message);
    } finally {
      view.classList.remove('busy');
    }
  }

  async function sendPdf(inv) {
    haptic();
    try {
      const sent = await api('/tg/pay/api/invoices/' + inv.id + '/send-pdf', { method: 'POST' });
      if (sent.ok) {
        alert('PDF отправил в чат с ботом.');
        return;
      }
    } catch (_) {}
    const res = await fetch('/tg/pay/api/invoices/' + inv.id + '/file', {
      credentials: 'same-origin',
      headers: headers(),
    });
    if (!res.ok) {
      alert('Не удалось открыть PDF');
      return;
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    window.open(url, '_blank');
  }

  function renderUpload() {
    setTitle('Новый счёт');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <div class="card">
        <p class="hint">PDF счёта — бот разберёт позиции, статью и сумму. Потом подтвердите.</p>
        <div class="field"><label>Файл</label>
          <input id="fFile" type="file" accept="application/pdf,image/*"></div>
        <button class="btn btn-ink" type="button" id="btnUp">Разобрать</button>
        <p class="hint" id="upStatus"></p>
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    document.getElementById('btnUp').onclick = uploadFile;
  }

  async function uploadFile() {
    const input = document.getElementById('fFile');
    const status = document.getElementById('upStatus');
    if (!input.files || !input.files[0]) {
      status.textContent = 'Выберите файл.';
      return;
    }
    const fd = new FormData();
    fd.append('file', input.files[0]);
    status.textContent = 'Читаю счёт…';
    view.classList.add('busy');
    try {
      const inv = await api('/tg/pay/api/invoices/upload', { method: 'POST', body: fd });
      haptic('medium');
      location.hash = '#/inv/' + inv.id;
    } catch (e) {
      status.textContent = e.message;
    } finally {
      view.classList.remove('busy');
    }
  }

  function esc(s) {
    return String(s || '').replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  boot();
})();
