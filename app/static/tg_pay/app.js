(() => {
  const tg = window.Telegram && window.Telegram.WebApp;
  if (tg) {
    tg.ready();
    tg.expand();
    try { tg.setHeaderColor('#F4F0E6'); tg.setBackgroundColor('#F4F0E6'); } catch (_) {}
  }

  const view = document.getElementById('view');
  const titleEl = document.getElementById('screenTitle');
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
    const path = hash.split('?')[0];
    const parts = path.split('/').filter(Boolean);
    if (parts[0] === 'inv' && parts[1]) return renderDetail(+parts[1]);
    if (parts[0] === 'new') return renderNew();
    if (parts[0] === 'plan') return renderPlan();
    if (parts[0] === 'file') return renderUpload();
    if (parts[0] === 'inbox') {
      if (!me.can_inbox) return renderList();
      return renderInbox();
    }
    return renderList();
  }

  function hashParam(name) {
    const q = (location.hash.split('?')[1] || '');
    return new URLSearchParams(q).get(name);
  }

  function prioBadge(p) {
    if (p === 'high') return '<span class="badge badge-high">срочно</span>';
    if (p === 'low') return '<span class="badge badge-low">не срочно</span>';
    return '';
  }

  async function boot() {
    try {
      me = await api('/tg/pay/api/me');
    } catch (e) {
      view.innerHTML = `<div class="empty"><h2>Нет входа</h2><p>${e.message}</p></div>`;
      return;
    }
    if (me.can_edit || me.can_inbox) {
      budgetItems = await api('/tg/pay/api/budget-items');
    }
    window.addEventListener('hashchange', route);
    route();
  }

  function rowFill(inv) {
    const plan = Number(inv.planned_amount);
    const fact = Number(inv.fact_amount || 0);
    if (!(plan > 0)) return { cls: '', fill: 0 };
    if (fact <= 0) return { cls: 'row-wait', fill: 0 };
    if (fact > plan) return { cls: 'row-over', fill: 100 };
    return { cls: '', fill: Math.max(6, Math.round((fact / plan) * 100)) };
  }

  function rowHtml(inv) {
    const plan = Number(inv.planned_amount);
    const fact = Number(inv.fact_amount || 0);
    const { cls, fill } = rowFill(inv);
    let sub = esc((inv.budget && inv.budget.name) || 'без статьи');
    if (Number.isFinite(plan) && plan > 0) {
      sub = 'план ' + money(plan) + (fact > 0 ? ' · факт ' + money(fact) : ' · ждём счёт');
      if (fact > 0 && fact < plan) sub += ' · −' + money(plan - fact);
      if (fact > plan) sub += ' · +' + money(fact - plan);
    }
    const ptype = inv.payment_type === 'cash' ? 'нал' : 'безнал';
    const shownAmt = fact > 0 ? fact : (inv.amount || plan || 0);
    return `<a class="row${inv.priority === 'high' ? ' row-high' : ''}${inv.status === 'draft' ? ' row-draft' : ''} ${cls}" style="--fill:${fill}%" href="#/inv/${inv.id}">
      <div>
        <div class="name">${inv.status === 'draft' ? '<span class="badge">черновик</span>' : ''}${prioBadge(inv.priority)}${esc(inv.summary)} <span class="ptype">${ptype}</span></div>
        <div class="sub">${sub}</div>
      </div>
      <div class="amt">${money(shownAmt)}</div>
    </a>`;
  }

  async function renderList() {
    setTitle('Счета на оплату');
    const data = await api('/tg/pay/api/invoices');
    const rows = data.invoices || [];
    const isFact = (x) => (Number(x.fact_amount) || 0) > 0 || (x.kind !== 'plan' && (Number(x.amount) || 0) > 0);
    const drafts = me.can_edit ? rows.filter((x) => x.status === 'draft') : [];
    const live = rows.filter((x) => x.status !== 'draft');
    const shown = me.can_edit ? live : live.filter((x) => x.status === 'new' && isFact(x));
    const cash = shown.filter((x) => x.payment_type === 'cash');
    const cashless = shown.filter((x) => x.payment_type !== 'cash');
    const plan = Number(data.total_plan) || 0;
    const fact = Number(data.total_fact) || 0;
    let delta = '';
    if (plan > 0 && fact > 0) {
      delta = fact <= plan
        ? 'экономия ' + money(plan - fact)
        : 'перерасход ' + money(fact - plan);
    }

    let html = `
      <div class="hero">
        <div class="label">К оплате</div>
        <div class="sum">${money(data.total_new)}</div>
        <div class="split">
          <span>Безнал ${money(data.total_cashless || 0)}</span>
          <span>Нал ${money(data.total_cash || 0)}</span>
        </div>
        ${plan ? `<div class="meta">план на неделю ${money(plan)}${fact ? ' · факт ' + money(fact) : ''}${delta ? ' · ' + delta : ''}</div>` : `<div class="meta">${shown.filter(isFact).length} счёт(ов)</div>`}
      </div>
    `;
    if (me.can_inbox && data.inbox_count) {
      html += `<a class="inbox-banner" href="#/inbox">Входящие из чата · ${data.inbox_count}</a>`;
    } else if (me.can_inbox) {
      html += `<a class="inbox-link" href="#/inbox">Входящие из чата</a>`;
    }
    if (drafts.length) {
      html += '<h2 class="sec">Черновики</h2><div class="list">' + drafts.map(rowHtml).join('') + '</div>';
    }
    if (!shown.length && !drafts.length) {
      html += `<div class="empty"><h2>Пусто</h2><p>${me.can_edit ? 'План на неделю или PDF боту.' : 'Неоплаченных счетов нет.'}</p></div>`;
    } else {
      if (cashless.length) {
        html += '<h2 class="sec">Безнал</h2><div class="list">' + cashless.map(rowHtml).join('') + '</div>';
      }
      if (cash.length) {
        html += '<h2 class="sec">Нал</h2><div class="list">' + cash.map(rowHtml).join('') + '</div>';
      }
    }
    if (me.can_edit) {
      html += `<button class="fab" type="button" id="fabAdd" aria-label="Добавить">+</button>`;
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

    const isDraft = inv.status === 'draft';
    const shownAmt = (Number(inv.fact_amount) || 0) > 0
      ? inv.fact_amount
      : (inv.amount || inv.planned_amount || 0);
    const payHint = `<p class="hint" style="margin-top:10px">${esc(inv.summary)}</p>
      <p class="ptype" style="margin-top:8px">${inv.payment_type === 'cash' ? 'Нал' : 'Безнал'}${isDraft ? ' · черновик' : ''}</p>`;

    let assignPanel = '';
    if (can && isDraft) {
      const plans = inv.open_plans || [];
      assignPanel = `
        <div class="field"><label>К плану</label>
          <select id="fAssignPlan">
            <option value="">— как новый счёт —</option>
            ${plans.map((p) => `<option value="${p.id}">${esc(p.summary)} · план ${money(p.planned_amount)}</option>`).join('')}
          </select>
        </div>
        <button class="btn btn-ink" type="button" id="btnAssign">В оплату</button>
      `;
    }

    let editPanel = '';
    if (can) {
      editPanel = `
        <button class="btn btn-quiet" type="button" id="btnMore">Правки</button>
        <div id="editPanel" hidden>
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
          <div class="field"><label>Оплата</label>
            <select id="fPayType">
              <option value="cashless" ${inv.payment_type !== 'cash' ? 'selected' : ''}>Безнал</option>
              <option value="cash" ${inv.payment_type === 'cash' ? 'selected' : ''}>Нал</option>
            </select>
          </div>
          <div class="field"><label>План на неделю</label>
            <input id="fPlan" inputmode="decimal" value="${inv.planned_amount || ''}" placeholder="сумма в пятницу"></div>
          ${!inv.has_file ? `<div class="field"><label>Реальный счёт</label>
            <input id="fAttach" type="file" accept="application/pdf,image/*"></div>
            <button class="btn btn-quiet" type="button" id="btnAttach">Прикрепить файл</button>` : ''}
          <button class="btn btn-quiet" type="button" id="btnSave">Сохранить правки</button>
          <button class="btn btn-ghost" type="button" id="btnDrop">Удалить</button>
        </div>
      `;
    }

    const payBtn = (!isDraft && inv.status !== 'paid')
      ? '<button class="btn btn-ink" type="button" id="btnPaid">Оплачено</button>'
      : '';
    const fileBtns = inv.has_file
      ? '<button class="btn btn-brass" type="button" id="btnOpen">Открыть счёт</button>'
      : '<p class="warn">Файл счёта не найден.</p>';

    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <div class="card">
        <div class="amount-xl">${money(shownAmt)}</div>
        ${payHint}
        ${fileBtns}
        ${assignPanel}
        ${payBtn}
        ${editPanel}
        ${lines}
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    const openBtn = document.getElementById('btnOpen');
    if (openBtn) openBtn.onclick = () => openInvoice(inv);
    const more = document.getElementById('btnMore');
    const panel = document.getElementById('editPanel');
    if (more && panel) {
      more.onclick = () => {
        const open = panel.hasAttribute('hidden');
        if (open) panel.removeAttribute('hidden');
        else panel.setAttribute('hidden', '');
        more.textContent = open ? 'Скрыть правки' : 'Правки';
      };
    }
    const save = document.getElementById('btnSave');
    if (save) save.onclick = () => saveInv(inv.id, false);
    const attach = document.getElementById('btnAttach');
    if (attach) attach.onclick = () => attachToPlan(inv.id);
    const paid = document.getElementById('btnPaid');
    if (paid) paid.onclick = () => markPaid(inv.id);
    const assignBtn = document.getElementById('btnAssign');
    if (assignBtn) assignBtn.onclick = () => assignDraft(inv.id);
    const drop = document.getElementById('btnDrop');
    if (drop) drop.onclick = async () => {
      if (!confirm('Удалить счёт?')) return;
      await api('/tg/pay/api/invoices/' + inv.id + '/discard', { method: 'POST' });
      location.hash = '#/';
    };
  }

  async function assignDraft(id) {
    const sel = document.getElementById('fAssignPlan');
    const planId = sel && sel.value;
    view.classList.add('busy');
    try {
      const body = planId ? { plan_id: planId } : { as_new: true };
      const inv = await api('/tg/pay/api/invoices/' + id + '/assign', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      haptic('medium');
      location.hash = '#/inv/' + inv.id;
    } catch (e) {
      alert(e.message);
    } finally {
      view.classList.remove('busy');
    }
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
      payment_type: (document.getElementById('fPayType') || {}).value || 'cashless',
      planned_amount: (document.getElementById('fPlan') || {}).value,
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

  async function openInvoice(inv) {
    haptic();
    try {
      const sent = await api('/tg/pay/api/invoices/' + inv.id + '/send-pdf', { method: 'POST' });
      if (sent.ok) {
        alert('Счёт отправил в чат с ботом — откройте его там.');
        return;
      }
    } catch (_) {}
    const res = await fetch(withAuth('/tg/pay/api/invoices/' + inv.id + '/file'), {
      credentials: 'same-origin',
      headers: headers(),
    });
    if (!res.ok) {
      alert('Не удалось открыть счёт');
      return;
    }
    const blob = await res.blob();
    window.open(URL.createObjectURL(blob), '_blank');
  }

  function renderNew() {
    setTitle('Добавить');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <a class="choice" href="#/plan">
        <div class="choice-k">План</div>
        <p>На неделю вперёд — сумма без файла. В пт для руководителя.</p>
      </a>
      <a class="choice" href="#/file">
        <div class="choice-k">Файл</div>
        <p>PDF или фото реального счёта.</p>
      </a>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
  }

  function renderPlan() {
    setTitle('План на неделю');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← назад</button>
      <div class="card">
        <p class="hint">Пятничный план: назначение и сумма. Файл прикрепите в течение недели.</p>
        <div class="field"><label>Назначение</label>
          <textarea id="pSummary" placeholder="ЧОП, ГСМ, сетка…"></textarea></div>
        <div class="field"><label>План, ₽</label>
          <input id="pAmount" inputmode="decimal"></div>
        <div class="field"><label>Оплата</label>
          <select id="pType">
            <option value="cashless">Безнал</option>
            <option value="cash">Нал</option>
          </select>
        </div>
        <div class="field"><label>Статья</label>
          <select id="pBudget">
            <option value="">— не выбрана —</option>
            ${budgetItems.map((b) => `<option value="${b.id}">${esc(b.code || '')} ${esc(b.name)}</option>`).join('')}
          </select>
        </div>
        <button class="btn btn-ink" type="button" id="btnPlan">Создать план</button>
        <p class="hint" id="pStatus"></p>
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/new'; };
    document.getElementById('btnPlan').onclick = createPlan;
  }

  async function createPlan() {
    const status = document.getElementById('pStatus');
    const summary = (document.getElementById('pSummary') || {}).value;
    const planned_amount = (document.getElementById('pAmount') || {}).value;
    view.classList.add('busy');
    try {
      const inv = await api('/tg/pay/api/invoices/plan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          summary,
          planned_amount,
          payment_type: (document.getElementById('pType') || {}).value,
          budget_item_id: (document.getElementById('pBudget') || {}).value || null,
        }),
      });
      haptic('medium');
      location.hash = '#/inv/' + inv.id;
    } catch (e) {
      status.textContent = e.message;
    } finally {
      view.classList.remove('busy');
    }
  }

  async function attachToPlan(planId) {
    const input = document.getElementById('fAttach');
    if (!input || !input.files || !input.files[0]) {
      alert('Выберите PDF или фото.');
      return;
    }
    const fd = new FormData();
    fd.append('file', input.files[0]);
    fd.append('plan_id', String(planId));
    view.classList.add('busy');
    try {
      const inv = await api('/tg/pay/api/invoices/upload', { method: 'POST', body: fd });
      haptic('medium');
      location.hash = '#/inv/' + inv.id;
    } catch (e) {
      alert(e.message);
    } finally {
      view.classList.remove('busy');
    }
  }

  function renderUpload() {
    setTitle('Новый счёт');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← назад</button>
      <div class="card">
        <p class="hint">PDF или фото счёта — разберём сумму и позиции.</p>
        <div class="field"><label>Файл</label>
          <input id="fFile" type="file" accept="application/pdf,image/*"></div>
        <div class="field"><label>Оплата</label>
          <select id="fUpType">
            <option value="cashless">Безнал</option>
            <option value="cash">Нал</option>
          </select>
        </div>
        <button class="btn btn-ink" type="button" id="btnUp">Разобрать</button>
        <p class="hint" id="upStatus"></p>
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/new'; };
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
    const ptype = (document.getElementById('fUpType') || {}).value;
    if (ptype) fd.append('payment_type', ptype);
    const planId = hashParam('plan');
    if (planId) fd.append('plan_id', planId);
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
