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
    const v = Number(n);
    if (!Number.isFinite(v)) return '0\u00a0₽';
    const rounded = Math.round(v * 100) / 100;
    const hasKop = Math.abs(rounded - Math.round(rounded)) >= 0.005;
    return rounded.toLocaleString('ru-RU', {
      minimumFractionDigits: hasKop ? 2 : 0,
      maximumFractionDigits: 2,
    }) + '\u00a0₽';
  }

  function headers() {
    const h = {};
    if (tg && tg.initData) h['X-Telegram-Init-Data'] = tg.initData;
    const m = document.cookie.match(/(?:^|; )tg_pay_as=([^;]*)/);
    if (m) h['X-Tg-Pay-As'] = decodeURIComponent(m[1]);
    return h;
  }

  function withAuth(path) {
    if (!tg || !tg.initData) return path;
    const sep = path.includes('?') ? '&' : '?';
    return path + sep + 'initData=' + encodeURIComponent(tg.initData);
  }

  async function api(path, opts) {
    const res = await fetch(withAuth(path), {
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
    if (parts[0] === 'inbox') return renderInbox();
    return renderList();
  }

  function prioBadge(p) {
    if (p === 'high') return '<span class="badge badge-high">срочно</span>';
    if (p === 'low') return '<span class="badge badge-low">не срочно</span>';
    return '';
  }

  async function loadStatus() {
    try {
      const res = await fetch('/tg/pay/api/status');
      return await res.json();
    } catch (_) {
      return null;
    }
  }

  function statusBlock(status, extra) {
    const bits = [];
    if (status) {
      bits.push(status.db ? 'БД ок' : 'БД ошибка');
      bits.push(status.bot_token ? 'бот ок' : 'нет TG_BOT_TOKEN');
      bits.push(status.last_telegram
        ? ('вебхук: ' + status.last_telegram.kind)
        : 'вебхук: тишина — Telegram не достучался');
    }
    if (extra) bits.push(extra);
    if (!bits.length) return '';
    return `<p class="hint">${bits.join(' · ')}</p>`;
  }

  async function boot() {
    const status = await loadStatus();
    try {
      me = await api('/tg/pay/api/me');
    } catch (e) {
      view.innerHTML = `<div class="empty"><h2>Нет входа</h2><p>${e.message}</p>${statusBlock(status)}</div>`;
      return;
    }
    me._status = status;
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
    if (me.can_edit || me.can_inbox) {
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
        <div class="meta">${(data.count_new || 0) + (data.count_draft || 0)} счёт(ов)</div>
      </div>
      ${statusBlock(me._status, me.can_edit
        ? ('вы ' + me.username + ' — можно загружать')
        : ('вы ' + me.username + ' / ' + me.role + ' — оплата'))}
    `;
    if (me.can_inbox && data.inbox_count) {
      html += `<a class="inbox-banner" href="#/inbox">Входящие из чата · ${data.inbox_count}</a>`;
    } else if (me.can_inbox) {
      html += `<a class="inbox-link" href="#/inbox">Входящие из чата</a>`;
    }
    if (!shown.length) {
      html += `<div class="empty"><h2>Пусто</h2><p>${me.can_edit ? 'Загрузите PDF счёта.' : 'Неоплаченных счетов нет.'}</p></div>`;
    } else {
      html += '<div class="list">';
      for (const inv of shown) {
        const badge = (inv.status === 'draft' ? '<span class="badge">черновик</span>' : '') + prioBadge(inv.priority);
        html += `<a class="row${inv.priority === 'high' ? ' row-high' : ''}" href="#/inv/${inv.id}">
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
        const qty = ln.qty != null && ln.qty !== '' ? String(ln.qty).replace('.', ',') : '';
        const q = [qty, ln.unit].filter(Boolean).join('\u00a0');
        let raw = ln.total;
        if (raw == null && ln.qty && ln.unit_price) raw = Number(ln.qty) * Number(ln.unit_price);
        const sum = raw != null ? money(raw) : '';
        return `<li><span class="ln-name">${esc(ln.description || '')}${q ? `<span class="q"> · ${esc(q)}</span>` : ''}</span><span class="ln-amt">${sum}</span></li>`;
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
        <div class="field"><label>Срочность</label>
          <select id="fPrio">
            <option value="high" ${inv.priority === 'high' ? 'selected' : ''}>Срочно</option>
            <option value="normal" ${inv.priority !== 'high' && inv.priority !== 'low' ? 'selected' : ''}>Обычный</option>
            <option value="low" ${inv.priority === 'low' ? 'selected' : ''}>Не срочно</option>
          </select>
        </div>
      `;
    } else {
      editor = `<div class="article">${prioBadge(inv.priority)}${esc((inv.budget && inv.budget.name) || 'Статья не указана')}</div>`;
    }

    const payBtn = inv.status === 'paid'
      ? ''
      : '<button class="btn btn-ink" type="button" id="btnPaid">Оплачено</button>';

    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <div class="card">
        <div class="amount-xl">${money(inv.amount)}</div>
        <p class="hint" style="margin-top:10px">${esc(inv.summary)}</p>
        ${editor}
        ${inv.has_file ? '' : '<p class="warn">PDF счёта не найден.</p>'}
        ${can ? '<button class="btn btn-ink" type="button" id="btnSave">Сохранить</button>' : ''}
        <button class="btn btn-brass" type="button" id="btnPdf">Скачать PDF</button>
        ${payBtn}
        ${lines}
        ${can ? '<button class="btn btn-ghost" type="button" id="btnDrop">Удалить</button>' : ''}
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    document.getElementById('btnPdf').onclick = () => sendPdf(inv);
    const save = document.getElementById('btnSave');
    if (save) save.onclick = () => saveInv(inv.id, false);
    const paid = document.getElementById('btnPaid');
    if (paid) paid.onclick = () => markPaid(inv.id);
    const drop = document.getElementById('btnDrop');
    if (drop) drop.onclick = async () => {
      if (!confirm('Удалить счёт?')) return;
      await api('/tg/pay/api/invoices/' + inv.id + '/discard', { method: 'POST' });
      location.hash = '#/';
    };
  }

  async function markPaid(id) {
    if (!confirm('Счёт оплачен? Он исчезнет из списка.')) return;
    view.classList.add('busy');
    try {
      await api('/tg/pay/api/invoices/' + id + '/mark-paid', { method: 'POST' });
      haptic('medium');
      location.hash = '#/';
    } catch (e) {
      alert(e.message);
    } finally {
      view.classList.remove('busy');
    }
  }

  async function saveInv(id, confirmPay) {
    const body = {
      summary: (document.getElementById('fSummary') || {}).value,
      amount: (document.getElementById('fAmount') || {}).value,
      budget_item_id: (document.getElementById('fBudget') || {}).value || null,
      priority: (document.getElementById('fPrio') || {}).value || 'normal',
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
    const res = await fetch(withAuth('/tg/pay/api/invoices/' + inv.id + '/file'), {
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

  function budgetSelect(selectedId) {
    return `<select class="inbox-budget">
      <option value="">— статья —</option>
      ${budgetItems.map((b) => `<option value="${b.id}" ${selectedId === b.id ? 'selected' : ''}>${esc(b.code || '')} ${esc(b.name)}</option>`).join('')}
    </select>`;
  }

  async function renderInbox() {
    setTitle('Входящие из чата');
    const data = await api('/tg/pay/api/inbox');
    const items = data.items || [];
    let html = `<button class="back" type="button" id="goBack">← к счетам</button>`;
    if (!items.length) {
      html += '<div class="empty"><h2>Пусто</h2><p>Сообщений из чата расходов нет.</p></div>';
    } else {
      html += '<div class="list">';
      for (const it of items) {
        const match = it.invoice
          ? `<div class="sub">похоже на счёт: ${esc(it.invoice.summary)} · ${money(it.invoice.amount)}</div>`
          : '';
        html += `<div class="card inbox-card" data-id="${it.id}">
          <div class="amount-xl" style="font-size:28px">${money(it.amount)}</div>
          <p class="hint" style="margin-top:8px">${esc(it.description)}</p>
          <div class="sub">${esc(it.sender)}${it.payment_type === 'cash' ? ' · нал' : it.payment_type === 'cashless' ? ' · безнал' : ''}</div>
          ${match}
          <div class="field"><label>Статья</label>${budgetSelect(it.suggested_budget_item_id)}</div>
          ${it.invoice ? '<button class="btn btn-ink" type="button" data-act="invoice">Это оплата счёта</button>' : ''}
          <button class="btn btn-brass" type="button" data-act="expense">В расходы</button>
          <button class="btn btn-ghost" type="button" data-act="reject">Не расход</button>
        </div>`;
      }
      html += '</div>';
    }
    view.innerHTML = html;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    view.querySelectorAll('.inbox-card').forEach((card) => {
      const id = card.dataset.id;
      card.querySelectorAll('[data-act]').forEach((btn) => {
        btn.onclick = () => inboxAct(id, btn.dataset.act, card);
      });
    });
  }

  async function inboxAct(id, act, card) {
    const sel = card.querySelector('.inbox-budget');
    const bid = sel && sel.value ? sel.value : null;
    view.classList.add('busy');
    try {
      if (act === 'reject') {
        await api('/tg/pay/api/inbox/' + id + '/reject', { method: 'POST' });
      } else {
        await api('/tg/pay/api/inbox/' + id + '/confirm', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            budget_item_id: bid,
            as_expense: act === 'expense',
          }),
        });
      }
      haptic('medium');
      await renderInbox();
    } catch (e) {
      alert(e.message);
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
