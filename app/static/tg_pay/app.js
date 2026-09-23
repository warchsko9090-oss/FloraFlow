(() => {
  function tgApp() {
    return (window.FFTg && window.FFTg.tgApp()) || (window.Telegram && window.Telegram.WebApp);
  }
  function getInitData() {
    return window.FFTg ? window.FFTg.getInitData() : '';
  }
  function waitTelegram() {
    return window.FFTg ? window.FFTg.waitTelegram() : Promise.resolve(tgApp());
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
    const m = document.cookie.match(/(?:^|; )tg_pay_as=([^;]*)/);
    if (m) h['X-Tg-Pay-As'] = decodeURIComponent(m[1]);
    return h;
  }

  async function api(path, opts) {
    if (window.FFTg) {
      const extra = headers();
      const merged = Object.assign({}, extra, (opts && opts.headers) || {});
      return window.FFTg.api(path, Object.assign({}, opts, { headers: merged }));
    }
    throw new Error('Нет входа');
  }

  function haptic(kind) {
    try { const tg = tgApp(); tg && tg.HapticFeedback && tg.HapticFeedback.impactOccurred(kind || 'light'); } catch (_) {}
  }

  (function bindPress() {
    const sel = 'button, .btn, .fab, .back, a.row, a.choice, a.inbox-banner, a.inbox-link, a.week-plan-link';
    let cur = null;
    let downAt = 0;
    function clear() {
      if (!cur) return;
      const el = cur;
      cur = null;
      const wait = Math.max(0, 90 - (Date.now() - downAt));
      setTimeout(() => el.classList.remove('is-pressed'), wait);
    }
    document.addEventListener('pointerdown', (e) => {
      if (e.pointerType === 'mouse' && e.button !== 0) return;
      const t = e.target;
      if (t.closest && t.closest('input, textarea, select')) return;
      const el = t.closest && t.closest(sel);
      if (!el || el.disabled || el.classList.contains('busy')) return;
      if (cur) cur.classList.remove('is-pressed');
      cur = el;
      downAt = Date.now();
      el.classList.add('is-pressed');
      haptic('light');
    }, { passive: true });
    window.addEventListener('pointerup', clear, { passive: true });
    window.addEventListener('pointercancel', clear, { passive: true });
  })();

  function setTitle(t) { titleEl.textContent = t; }

  function route() {
    const hash = (location.hash || '#/').replace(/^#/, '');
    const path = hash.split('?')[0];
    const parts = path.split('/').filter(Boolean);
    if (parts[0] === 'inv' && parts[1]) return renderDetail(+parts[1]);
    if (parts[0] === 'new') return renderNew();
    if (parts[0] === 'week-plan') return renderWeekPlan();
    if (parts[0] === 'plan') return renderPlan();
    if (parts[0] === 'file') return renderUpload();
    if (parts[0] === 'bank') return renderBank();
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

  async function ensureBudgetItems() {
    if (budgetItems.length) return;
    if (!me || !(me.can_edit || me.can_inbox)) return;
    budgetItems = await api('/tg/pay/api/budget-items');
  }

  async function boot() {
    try {
      if (window.FFTg && window.FFTg.bootAuth) {
        me = await window.FFTg.bootAuth('/tg/pay/api/auth');
      } else {
        await waitTelegram();
        me = await api('/tg/pay/api/me');
      }
      try { const w = tgApp(); if (w) { w.setHeaderColor('#F4F0E6'); w.setBackgroundColor('#F4F0E6'); } } catch (_) {}
    } catch (e) {
      try {
        await waitTelegram();
        if (window.FFTg) me = await window.FFTg.handshake('/tg/pay/api/auth');
        else throw e;
      } catch (e2) {
        view.innerHTML = `<div class="empty"><h2>Нет входа</h2><p>${e2.message || e.message}</p></div>`;
        return;
      }
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
    const plan = Number(data.total_plan) || 0;
    const fact = Number(data.total_fact) || 0;
    let delta = '';
    if (plan > 0 && fact > 0) {
      delta = fact <= plan
        ? 'экономия ' + money(plan - fact)
        : 'перерасход ' + money(fact - plan);
    }
    const planMeta = plan
      ? `план ${money(plan)}${fact ? ' · факт ' + money(fact) : ''}${delta ? ' · ' + delta : ''}`
      : `${shown.filter(isFact).length} счёт(ов)`;

    let html = `
      <div class="hero">
        <div class="label">К оплате</div>
        <div class="sum">${money(data.total_new)}</div>
        <div class="meta">${esc(planMeta)}</div>
      </div>
    `;
    if (me.can_edit || me.role === 'executive') {
      html += `<a class="week-plan-link" href="#/week-plan">План недели →</a>`;
    }
    if (me.can_inbox && data.inbox_count) {
      html += `<a class="inbox-banner" href="#/inbox">Входящие из чата · ${data.inbox_count}</a>`;
    }
    if (drafts.length) {
      html += `<details class="drafts-acc"><summary>Черновики (${drafts.length})</summary>`
        + `<div class="list">${drafts.map(rowHtml).join('')}</div></details>`;
    }
    if (!shown.length && !drafts.length) {
      html += `<div class="empty"><h2>Пусто</h2><p>${me.can_edit ? 'План недели, счёт или выписка — кнопка +.' : 'Неоплаченных счетов нет.'}</p></div>`;
    } else if (shown.length) {
      html += '<div class="list">' + shown.map(rowHtml).join('') + '</div>';
    }
    if (me.can_edit) {
      html += `<button class="fab" type="button" id="fabAdd" aria-label="Добавить">+</button>`;
    }
    view.innerHTML = html;
    const fab = document.getElementById('fabAdd');
    if (fab) fab.onclick = () => { location.hash = '#/new'; };
  }

  async function renderDetail(id) {
    setTitle('Счёт');
    const inv = await api('/tg/pay/api/invoices/' + id);
    if (me.can_edit) await ensureBudgetItems();
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
        <button class="btn btn-quiet" type="button" id="btnMore">Ещё</button>
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
          <button class="btn btn-quiet" type="button" id="btnSave">Сохранить правки</button>
          ${inv.has_receipt ? '<button class="btn btn-quiet" type="button" id="btnReceipt">Квитанция в чат</button>' : ''}
          ${(inv.status !== 'paid')
            ? `<div class="field"><label>${inv.has_file ? 'Заменить PDF' : 'Прикрепить PDF'}</label>
                <input id="fAttach" type="file" accept="application/pdf,image/*"></div>
              <button class="btn btn-quiet" type="button" id="btnAttach">${inv.has_file ? 'Заменить файл' : 'Прикрепить файл'}</button>
              <button class="btn btn-ghost" type="button" id="btnDrop">Удалить</button>`
            : ''}
        </div>
      `;
    }

    const payBtn = (!isDraft && inv.status !== 'paid')
      ? '<button class="btn btn-ink" type="button" id="btnPaid">Оплачено</button>'
      : '';
    const openBtnHtml = inv.has_file
      ? '<button class="btn btn-brass" type="button" id="btnOpen">Счёт в чат</button>'
      : (can ? '<p class="warn">Файл счёта не найден — прикрепите в «Ещё».</p>' : '<p class="warn">Файл счёта не найден.</p>');

    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <div class="card">
        <div class="amount-xl">${money(shownAmt)}</div>
        ${payHint}
        ${openBtnHtml}
        ${assignPanel}
        ${payBtn}
        ${editPanel}
        ${lines}
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    const openBtn = document.getElementById('btnOpen');
    if (openBtn) openBtn.onclick = () => openInvoice(inv);
    const recBtn = document.getElementById('btnReceipt');
    if (recBtn) recBtn.onclick = () => openReceipt(inv);
    const more = document.getElementById('btnMore');
    const panel = document.getElementById('editPanel');
    if (more && panel) {
      more.onclick = () => {
        const open = panel.hasAttribute('hidden');
        if (open) panel.removeAttribute('hidden');
        else panel.setAttribute('hidden', '');
        more.textContent = open ? 'Скрыть' : 'Ещё';
      };
    }
    const save = document.getElementById('btnSave');
    if (save) save.onclick = () => saveInv(inv.id, false);
    const attach = document.getElementById('btnAttach');
    if (attach) attach.onclick = () => attachPdf(inv.id);
    const paid = document.getElementById('btnPaid');
    if (paid) paid.onclick = () => markPaid(inv.id);
    const assignBtn = document.getElementById('btnAssign');
    if (assignBtn) assignBtn.onclick = () => assignDraft(inv.id);
    const drop = document.getElementById('btnDrop');
    if (drop) drop.onclick = async () => {
      if (!confirm('Удалить счёт?')) return;
      view.classList.add('busy');
      try {
        await api('/tg/pay/api/invoices/' + inv.id + '/discard', { method: 'POST' });
        haptic('medium');
        location.hash = '#/';
      } catch (e) {
        alert(e.message);
      } finally {
        view.classList.remove('busy');
      }
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

  function sendErrorText(sent, fallback) {
    const err = (sent && sent.error) || '';
    if (err === 'no_telegram_id') {
      return 'Не вижу ваш Telegram. Закройте мини-приложение и откройте его кнопкой в чате с ботом.';
    }
    if (err === 'file_missing') return fallback || 'Файла нет.';
    return fallback || 'Не удалось отправить файл в чат.';
  }

  function afterSentToChat(msg) {
    haptic('medium');
    const tg = tgApp();
    if (tg && typeof tg.showAlert === 'function') {
      try {
        tg.showAlert(msg, () => { try { tg.close && tg.close(); } catch (_) {} });
        return;
      } catch (_) {}
    }
    alert(msg);
    if (tg && tg.close) setTimeout(() => tg.close(), 400);
  }

  async function sendFileToChat(inv, kind) {
    const isReceipt = kind === 'receipt';
    const sent = await api('/tg/pay/api/invoices/' + inv.id + '/send-pdf', {
      method: 'POST',
      body: JSON.stringify({ kind: isReceipt ? 'receipt' : 'file' }),
    });
    if (sent && sent.ok) {
      afterSentToChat(isReceipt ? 'Квитанцию отправил в чат с ботом.' : 'Счёт отправил в чат с ботом.');
      return;
    }
    alert(sendErrorText(sent, isReceipt ? 'Квитанция не найдена.' : 'У этого счёта нет PDF.'));
  }

  async function openInvoice(inv) {
    try {
      await sendFileToChat(inv, 'file');
    } catch (e) {
      alert(e.message || 'Не удалось отправить счёт в чат.');
    }
  }

  async function openReceipt(inv) {
    try {
      await sendFileToChat(inv, 'receipt');
    } catch (e) {
      alert(e.message || 'Не удалось отправить квитанцию в чат.');
    }
  }

  function renderNew() {
    setTitle('Добавить');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <a class="choice" href="#/week-plan">
        <div class="choice-k">План недели</div>
        <p>Пятничный план: нал и безнал, сохранить и закрепить у бота.</p>
      </a>
      <a class="choice" href="#/plan">
        <div class="choice-k">Одна строка плана</div>
        <p>Одна позиция плана без общего закрепа.</p>
      </a>
      <a class="choice" href="#/file">
        <div class="choice-k">Файл</div>
        <p>PDF или фото реального счёта.</p>
      </a>
      <a class="choice" href="#/bank">
        <div class="choice-k">Выписка</div>
        <p>Квитанция Альфа-Банка, платёжка или скрин списаний.</p>
      </a>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
  }

  function weekPlanRowHtml(item, idx, canEdit) {
    const id = item.id || '';
    const ptype = item.payment_type === 'cash' ? 'cash' : 'cashless';
    const locked = item.has_fact ? ' data-locked="1"' : '';
    if (!canEdit) {
      return `<div class="week-row">
        <div class="week-row-main"><b>${esc(_fmtPreviewAmt(item.planned_amount))}</b> — ${esc(item.summary || '')}</div>
        <div class="muted">${ptype === 'cash' ? 'нал' : 'безнал'}${item.has_fact ? ' · есть факт' : ''}</div>
      </div>`;
    }
    return `<div class="week-row" data-idx="${idx}"${locked}>
      <input type="hidden" class="wp-id" value="${id}">
      <div class="week-row-grid">
        <input class="wp-amt" inputmode="decimal" placeholder="сумма" value="${item.planned_amount || ''}">
        <select class="wp-type">
          <option value="cash" ${ptype === 'cash' ? 'selected' : ''}>Нал</option>
          <option value="cashless" ${ptype !== 'cash' ? 'selected' : ''}>Безнал</option>
        </select>
      </div>
      <textarea class="wp-sum" rows="2" placeholder="назначение">${String(item.summary || '').replace(/<\/textarea/gi, '')}</textarea>
      <button type="button" class="btn btn-ghost week-del" ${item.has_fact ? 'disabled title="Есть исполнение — нельзя удалить"' : ''}>Удалить</button>
    </div>`;
  }

  function _fmtPreviewAmt(n) {
    const v = Number(n);
    if (!Number.isFinite(v)) return '0';
    if (v >= 1000 && Math.abs(v % 1000) < 0.001) return Math.round(v / 1000) + ' тр';
    return money(v).replace(/\u00a0₽$/, '').trim();
  }

  function collectWeekPlanRows() {
    const items = [];
    view.querySelectorAll('.week-row[data-idx]').forEach((row) => {
      const summary = ((row.querySelector('.wp-sum') || {}).value || '').trim();
      const planned_amount = ((row.querySelector('.wp-amt') || {}).value || '').trim();
      const payment_type = ((row.querySelector('.wp-type') || {}).value || 'cashless');
      const idRaw = ((row.querySelector('.wp-id') || {}).value || '').trim();
      if (!summary && !planned_amount) return;
      const item = { summary, planned_amount, payment_type };
      if (idRaw) item.id = Number(idRaw);
      items.push(item);
    });
    return items;
  }

  async function renderWeekPlan() {
    setTitle('План недели');
    const data = await api('/tg/pay/api/week-plan');
    const canEdit = !!(me && me.can_edit);
    const items = data.items || [];
    let body;
    if (canEdit) {
      body = items.map((it, i) => weekPlanRowHtml(it, i, true)).join('')
        || '<p class="hint">Пока пусто — добавьте строки нал и безнал.</p>';
      body = `<div id="weekRows">${body}</div>
        <button class="btn btn-quiet" type="button" id="btnAddRow">+ строка</button>
        <button class="btn btn-ink" type="button" id="btnSavePin">Сохранить и закрепить</button>
        <p class="hint" id="weekStatus">${data.pinned ? 'Уже закреплён у админа и руководителя. Повторное сохранение обновит закреп.' : 'После сохранения план уйдёт в личку бота и закрепится у админа и руководителя.'}</p>`;
    } else {
      const cash = (data.cash || []).map((it, i) => weekPlanRowHtml(it, i, false)).join('') || '<p class="muted">пусто</p>';
      const cashless = (data.cashless || []).map((it, i) => weekPlanRowHtml(it, i, false)).join('') || '<p class="muted">пусто</p>';
      body = `<h2 class="sec">НАЛ</h2><div class="card">${cash}</div>
        <h2 class="sec">БЕЗНАЛ</h2><div class="card">${cashless}</div>
        <p class="hint">${data.pinned ? 'Закреплён в чате с ботом.' : 'Админ ещё не закрепил план.'}</p>`;
    }
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <div class="card">
        <p class="hint" style="margin:0">Период: <b>${esc(data.period_label || '')}</b></p>
      </div>
      ${body}
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    const addBtn = document.getElementById('btnAddRow');
    if (addBtn) {
      addBtn.onclick = () => {
        const wrap = document.getElementById('weekRows');
        const idx = wrap.querySelectorAll('.week-row').length;
        wrap.insertAdjacentHTML('beforeend', weekPlanRowHtml({
          summary: '', planned_amount: '', payment_type: 'cashless',
        }, idx, true));
        bindWeekRowDeletes();
      };
    }
    const saveBtn = document.getElementById('btnSavePin');
    if (saveBtn) {
      saveBtn.onclick = async () => {
        const status = document.getElementById('weekStatus');
        const items = collectWeekPlanRows();
        if (!items.length) {
          alert('Добавьте хотя бы одну строку плана.');
          return;
        }
        view.classList.add('busy');
        try {
          const res = await api('/tg/pay/api/week-plan', {
            method: 'POST',
            body: JSON.stringify({
              week_start: data.week_start,
              items,
              pin: true,
            }),
          });
          haptic('medium');
          const pin = res.pin || {};
          status.textContent = pin.pinned_to
            ? `Сохранено и закреплено у ${pin.pinned_to} чел.`
            : 'Сохранено, но закрепить не удалось — проверьте telegram_id админа/руководителя.';
          if (pin.errors && pin.errors.length) {
            status.textContent += ' ' + pin.errors.slice(0, 2).join('; ');
          }
        } catch (e) {
          status.textContent = e.message || 'Ошибка сохранения';
        } finally {
          view.classList.remove('busy');
        }
      };
    }
    bindWeekRowDeletes();
  }

  function bindWeekRowDeletes() {
    view.querySelectorAll('.week-del').forEach((btn) => {
      btn.onclick = () => {
        const row = btn.closest('.week-row');
        if (!row || row.dataset.locked === '1') return;
        row.remove();
      };
    });
  }

  async function renderPlan() {
    setTitle('План на неделю');
    await ensureBudgetItems();
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

  async function attachPdf(invId) {
    const input = document.getElementById('fAttach');
    if (!input || !input.files || !input.files[0]) {
      alert('Выберите PDF или фото.');
      return;
    }
    const fd = new FormData();
    fd.append('file', input.files[0]);
    view.classList.add('busy');
    try {
      const inv = await api('/tg/pay/api/invoices/' + invId + '/attach', { method: 'POST', body: fd });
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

  function renderBank() {
    setTitle('Выписка / платёжка');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← назад</button>
      <div class="card">
        <p class="hint">Скрин списка в Альфе, фото платёжки с монитора или PDF. Несколько платежей на одном кадре — разнесём все.</p>
        <div class="field"><label>Файл</label>
          <input id="bFile" type="file" accept="image/*,application/pdf,.jfif"></div>
        <button class="btn btn-ink" type="button" id="btnBank">Разнести</button>
        <p class="hint" id="bStatus"></p>
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/new'; };
    document.getElementById('btnBank').onclick = uploadBank;
  }

  function bankActLabel(act) {
    if (act === 'matched') return 'оплачен';
    if (act === 'created') return 'черновик';
    return 'пропуск';
  }

  function renderBankResult(data) {
    setTitle('Разнос');
    const items = data.items || [];
    const c = data.counts || {};
    let html = `<button class="back" type="button" id="goBack">← к списку</button>`;
    if (data.duplicate) {
      html += '<p class="hint">Этот файл уже загружали — повторно не проводим.</p>';
    }
    html += `<div class="card"><p class="hint">${c.matched || 0} оплачено · ${c.created || 0} черновик · ${c.skipped || 0} пропуск</p></div>`;
    if (!items.length) {
      html += `<div class="empty"><h2>Платежей нет</h2><p>${esc(data.error || 'Попробуйте более крупный кадр или другой файл.')}</p></div>`;
    } else {
      html += '<div class="list">';
      for (const it of items) {
        const href = it.invoice_id ? `#/inv/${it.invoice_id}` : '#/';
        html += `<a class="choice" href="${href}">
          <div class="choice-k">${esc(it.payee || it.invoice_no || 'платёж')}</div>
          <p>${money(it.amount)} · <span class="act-${esc(it.action)}">${bankActLabel(it.action)}</span>${it.invoice_no ? ' · №' + esc(it.invoice_no) : ''}</p>
          <p>${esc(it.note || '')}</p>
        </a>`;
      }
      html += '</div>';
    }
    view.innerHTML = html;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
  }

  async function uploadBank() {
    const input = document.getElementById('bFile');
    const status = document.getElementById('bStatus');
    if (!input.files || !input.files[0]) {
      status.textContent = 'Выберите файл.';
      return;
    }
    const fd = new FormData();
    fd.append('file', input.files[0]);
    status.textContent = 'Читаю выписку… может занять полминуты.';
    view.classList.add('busy');
    try {
      const data = await api('/tg/pay/api/bank-slips', { method: 'POST', body: fd });
      haptic('medium');
      renderBankResult(data);
    } catch (e) {
      status.textContent = e.message;
    } finally {
      view.classList.remove('busy');
    }
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
    await ensureBudgetItems();
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
