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

  function loadingCard(text) {
    return `<div class="loader" role="status" aria-live="polite"><div class="loader-spin"></div><p>${esc(text)}</p></div>`;
  }

  async function apiTimed(path, opts, ms) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), ms || 20000);
    try {
      return await api(path, Object.assign({}, opts || {}, { signal: ctrl.signal }));
    } catch (e) {
      const name = (e && e.name) || '';
      const msg = String((e && e.message) || '');
      if (name === 'AbortError' || /abort/i.test(msg)) {
        const err = new Error('Долго нет ответа. Повтор сейчас не создаст второй расход.');
        err.timeout = true;
        throw err;
      }
      throw e;
    } finally {
      clearTimeout(timer);
    }
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

  function goHome() {
    location.hash = '#/';
  }

  function errHuman(e, fallback) {
    const raw = String((e && e.message) || '').trim();
    if (raw === 'bad_amount') return 'Проверьте суммы: у каждой строки должна быть сумма больше 0.';
    if (raw === 'need_summary') return 'Укажите назначение.';
    if (raw === 'empty') return 'Добавьте хотя бы одну строку.';
    return raw || fallback || 'Ошибка';
  }

  function route() {
    view.classList.remove('busy');
    const hash = (location.hash || '#/').replace(/^#/, '');
    const path = hash.split('?')[0];
    const parts = path.split('/').filter(Boolean);
    if (parts[0] === 'inv' && parts[1]) return renderDetail(+parts[1]);
    if (parts[0] === 'new') return renderNew();
    if (parts[0] === 'quick') return renderQuickExpense(parts[1] ? +parts[1] : null);
    if (parts[0] === 'week-plan') {
      if (parts[1] === 'full') return renderWeekPlan();
      if (parts[1] === 'line') return renderPlan();
      return renderWeekPlanMenu();
    }
    if (parts[0] === 'plan') return renderPlan();
    if (parts[0] === 'inbox') {
      if (!me.can_inbox) return renderList();
      return renderInbox();
    }
    if (parts[0] === 'cash') {
      if (!me.can_edit) return renderList();
      if (!parts[1]) return renderCashList();
      if (parts[1] === 'new') return renderCashForm(null);
      const id = +parts[1];
      if (parts[2] === 'edit') return renderCashForm(id);
      if (parts[2] === 'receipt') return renderCashReceipt(id);
      if (parts[2] === 'in') return renderCashPayout(id);
      return renderCashPerson(id);
    }
    return renderList();
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
    if (/\/tg\/pay\/cash\/?$/.test(location.pathname)) {
      history.replaceState(null, '', '/tg/pay/' + (location.search || '') + '#/cash');
    }
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
    const noBudget = !inv.has_budget;
    let sub = esc((inv.budget && inv.budget.name) || 'без статьи');
    if (noBudget) sub = '⚠ нет статьи бюджета';
    if (Number.isFinite(plan) && plan > 0) {
      sub = (noBudget ? '⚠ нет статьи · ' : '') + 'план ' + money(plan) + (fact > 0 ? ' · факт ' + money(fact) : ' · ждём счёт');
      if (fact > 0 && fact < plan) sub += ' · −' + money(plan - fact);
      if (fact > plan) sub += ' · +' + money(fact - plan);
    }
    const ptype = inv.payment_type === 'cash' ? 'нал' : 'безнал';
    const shownAmt = (inv.kind === 'plan')
      ? (plan || 0)
      : (fact > 0 ? fact : (inv.amount || plan || 0));
    return `<a class="row${inv.priority === 'high' ? ' row-high' : ''}${inv.status === 'draft' ? ' row-draft' : ''}${noBudget ? ' row-nobudget' : ''} ${cls}" style="--fill:${fill}%" href="#/inv/${inv.id}">
      <div>
        <div class="name">${inv.status === 'draft' ? '<span class="badge">черновик</span>' : ''}${noBudget ? '<span class="badge badge-warn">нет статьи</span>' : ''}${prioBadge(inv.priority)}${esc(inv.summary)} <span class="ptype">${ptype}</span></div>
        <div class="sub">${sub}</div>
      </div>
      <div class="amt">${money(shownAmt)}</div>
    </a>`;
  }

  function budgetLabel(b) {
    return ((b.code || '') + ' ' + (b.name || '')).trim();
  }

  function filterBudgetItems(q) {
    const n = String(q || '').trim().toLowerCase();
    if (!n) return budgetItems.slice(0, 40);
    return budgetItems.filter((b) => {
      const name = String(b.name || '').toLowerCase();
      const code = String(b.code || '').toLowerCase();
      return name.includes(n) || code.includes(n);
    }).slice(0, 40);
  }

  function budgetPickerHtml(fieldId, selectedId) {
    const selected = budgetItems.find((b) => String(b.id) === String(selectedId || ''));
    const label = selected ? budgetLabel(selected) : '';
    return `<div class="budget-pick" data-budget-pick="${fieldId}">
      <input type="hidden" id="${fieldId}" class="wp-budget-id" value="${selected ? selected.id : ''}">
      <input type="search" class="budget-q" placeholder="поиск статьи…" value="${esc(label)}" autocomplete="off">
      <div class="budget-drop" hidden></div>
    </div>`;
  }

  function bindBudgetPickers(root) {
    (root || view).querySelectorAll('[data-budget-pick]').forEach((wrap) => {
      const hid = wrap.querySelector('input[type="hidden"]');
      const q = wrap.querySelector('.budget-q');
      const drop = wrap.querySelector('.budget-drop');
      if (!hid || !q || !drop) return;
      function renderDrop(list) {
        if (!list.length) {
          drop.innerHTML = '<div class="budget-empty">Ничего не найдено</div>';
          drop.hidden = false;
          return;
        }
        drop.innerHTML = list.map((b) =>
          `<button type="button" class="budget-opt" data-id="${b.id}">${esc(budgetLabel(b))}</button>`
        ).join('');
        drop.hidden = false;
        drop.querySelectorAll('.budget-opt').forEach((btn) => {
          btn.onclick = () => {
            const b = budgetItems.find((x) => String(x.id) === btn.dataset.id);
            hid.value = b ? b.id : '';
            q.value = b ? budgetLabel(b) : '';
            drop.hidden = true;
          };
        });
      }
      q.onfocus = () => renderDrop(filterBudgetItems(q.value));
      q.oninput = () => {
        hid.value = '';
        renderDrop(filterBudgetItems(q.value));
      };
      q.onblur = () => setTimeout(() => { drop.hidden = true; }, 180);
    });
  }

  async function renderList() {
    setTitle('Счета на оплату');
    const data = await api('/tg/pay/api/invoices');
    const rows = data.invoices || [];
    const isFact = (x) => (Number(x.fact_amount) || 0) > 0 || (x.kind !== 'plan' && (Number(x.amount) || 0) > 0);
    const drafts = (me.can_edit || me.role === 'executive')
      ? rows.filter((x) => x.status === 'draft')
      : [];
    const live = rows.filter((x) => x.status !== 'draft');
    const shown = me.can_edit ? live : live.filter((x) => x.status === 'new' && isFact(x));
    const weekRemain = Number(data.week_remain);
    const weekCash = Number(data.week_cash) || 0;
    const weekCashless = Number(data.week_cashless) || 0;
    const weekCount = Number(data.week_count) || 0;
    const ws = data.week_start || '';
    const we = data.week_end || '';
    let period = '';
    try {
      if (ws && we) {
        const a = new Date(ws + 'T12:00:00');
        const b = new Date(we + 'T12:00:00');
        period = a.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' })
          + ' — ' + b.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
      }
    } catch (_) {}

    let html = `
      <div class="hero">
        <div class="label">Осталось на неделе</div>
        <div class="sum">${money(Number.isFinite(weekRemain) ? weekRemain : 0)}</div>
        <div class="meta">${esc(period)}${weekCount ? ' · ' + weekCount + ' поз.' : ''}</div>
        <div class="hero-split">
          <div class="hero-pill"><span>Нал</span><b>${money(weekCash)}</b></div>
          <div class="hero-pill"><span>Безнал</span><b>${money(weekCashless)}</b></div>
        </div>
      </div>
    `;
    if (me.can_edit || me.role === 'executive') {
      html += `<a class="week-plan-link" href="#/week-plan/full">План недели →</a>`;
    }
    if (me.can_edit) {
      html += `<a class="week-plan-link" href="#/cash">Касса →</a>`;
    }
    if (me.can_inbox && data.inbox_count) {
      html += `<a class="inbox-banner" href="#/inbox">Входящие из чата · ${data.inbox_count}</a>`;
    }
    if (drafts.length) {
      html += `<details class="drafts-acc" open><summary>Черновики (${drafts.length})</summary>`
        + `<div class="list">${drafts.map(rowHtml).join('')}</div></details>`;
    }
    if (!shown.length && !drafts.length) {
      html += `<div class="empty"><h2>Пусто</h2><p>${me.can_edit ? 'План недели, счёт или выписка — кнопка +.' : (me.role === 'executive' ? 'Быстрая оплата — кнопка +, или скиньте файл боту: форма откроется сразу.' : 'Неоплаченных счетов нет.')}</p></div>`;
    } else if (shown.length) {
      html += '<div class="list">' + shown.map(rowHtml).join('') + '</div>';
    }
    if (me.can_edit || me.role === 'executive') {
      html += `<button class="fab" type="button" id="fabAdd" aria-label="Добавить">+</button>`;
    }
    view.innerHTML = html;
    const fab = document.getElementById('fabAdd');
    if (fab) {
      fab.onclick = () => {
        location.hash = (me.role === 'executive' && !me.can_edit) ? '#/quick' : '#/new';
      };
    }
    try {
      let toastText = '';
      if (sessionStorage.getItem('ff_quick_ok')) {
        sessionStorage.removeItem('ff_quick_ok');
        toastText = 'Отправлено админу';
      } else if (sessionStorage.getItem('ff_week_ok')) {
        sessionStorage.removeItem('ff_week_ok');
        toastText = 'План сохранён';
      }
      if (toastText) {
        const toast = document.createElement('div');
        toast.className = 'toast-ok';
        toast.textContent = toastText;
        view.appendChild(toast);
        setTimeout(() => toast.remove(), 2800);
      }
    } catch (_) {}
  }

  async function renderDetail(id) {
    setTitle('Счёт');
    const inv = await api('/tg/pay/api/invoices/' + id);
    if (me.can_edit) await ensureBudgetItems();
    const can = me.can_edit;
    const isExec = me.role === 'executive';
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
    const isPlan = (inv.kind || '') === 'plan';
    const planned = Number(inv.planned_amount) || 0;
    const factPaid = Number(inv.fact_amount) || 0;
    const remaining = isPlan
      ? Math.max(0, Math.round((planned - factPaid) * 100) / 100)
      : 0;
    const shownAmt = isPlan
      ? (planned || inv.amount || 0)
      : ((Number(inv.fact_amount) || 0) > 0 ? inv.fact_amount : (inv.amount || inv.planned_amount || 0));

    // Черновик быстрого расхода
    if (isDraft && (isExec || can)) {
      setTitle('Быстрый расход');
      const budgetHint = inv.budget && inv.budget.name
        ? `<p class="hint">Подсказка статьи: <b>${esc(inv.budget.code || '')} ${esc(inv.budget.name)}</b> — админ подтвердит.</p>`
        : '<p class="hint">Статья не определена — админ выберет на дашборде.</p>';
      view.innerHTML = `
        <button class="back" type="button" id="goBack">← к списку</button>
        <div class="card">
          <p class="hint" style="margin:0 0 12px">Файл из чата бота. Проверьте сумму и отправьте админу.</p>
          <div class="field"><label>Сумма, ₽</label>
            <input id="qAmount" inputmode="decimal" value="${inv.amount || ''}"></div>
          <div class="field"><label>Назначение</label>
            <input id="qSummary" type="text" value="${esc(inv.summary || '')}"></div>
          <div class="field"><label>Оплата</label>
            <div class="pay-toggle" id="qType">
              <button type="button" class="pay-opt" data-v="cashless" aria-pressed="${inv.payment_type !== 'cash' ? 'true' : 'false'}">Безнал</button>
              <button type="button" class="pay-opt" data-v="cash" aria-pressed="${inv.payment_type === 'cash' ? 'true' : 'false'}">Нал</button>
            </div>
          </div>
          ${inv.has_file ? '<p class="hint">Файл прикреплён ✓</p>' : '<p class="hint">Файла нет — можно отправить без него.</p>'}
          ${budgetHint}
          ${can ? `
            <div class="field"><label>Или привязать к плану</label>
              <select id="fAssignPlan">
                <option value="">— отправить админу как расход —</option>
                ${(inv.open_plans || []).map((p) => `<option value="${p.id}">${esc(p.summary)} · план ${money(p.planned_amount)}</option>`).join('')}
              </select>
            </div>` : ''}
          <button class="btn btn-ink" type="button" id="btnQuickSend">Отправить админу</button>
          ${can ? '<button class="btn btn-quiet" type="button" id="btnAssign" hidden>В оплату</button>' : ''}
          <button class="btn btn-ghost" type="button" id="btnDrop">Удалить черновик</button>
          <p class="hint" id="qStatus"></p>
        </div>
      `;
      document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
      let ptype = inv.payment_type === 'cash' ? 'cash' : 'cashless';
      view.querySelectorAll('#qType .pay-opt').forEach((btn) => {
        btn.onclick = () => {
          ptype = btn.dataset.v;
          view.querySelectorAll('#qType .pay-opt').forEach((b) => {
            b.setAttribute('aria-pressed', b === btn ? 'true' : 'false');
          });
        };
      });
      const sendBtn = document.getElementById('btnQuickSend');
      const assignSel = document.getElementById('fAssignPlan');
      const assignBtn = document.getElementById('btnAssign');
      if (assignSel && assignBtn) {
        assignSel.onchange = () => {
          const hasPlan = !!assignSel.value;
          assignBtn.hidden = !hasPlan;
          sendBtn.hidden = hasPlan;
        };
        assignBtn.onclick = () => assignDraft(inv.id);
      }
      sendBtn.onclick = async () => {
        const status = document.getElementById('qStatus');
        const amount = (document.getElementById('qAmount') || {}).value;
        const summary = (document.getElementById('qSummary') || {}).value;
        if (!String(amount || '').trim() || !String(summary || '').trim()) {
          status.textContent = 'Укажите сумму и назначение.';
          return;
        }
        view.classList.add('busy');
        status.textContent = 'Отправляю…';
        try {
          await api('/tg/pay/api/invoices/' + inv.id + '/submit-quick', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ amount, summary, payment_type: ptype }),
          });
          haptic('medium');
          try { sessionStorage.setItem('ff_quick_ok', '1'); } catch (_) {}
          location.hash = '#/';
        } catch (e) {
          status.textContent = e.message || 'Не удалось отправить';
        } finally {
          view.classList.remove('busy');
        }
      };
      document.getElementById('btnDrop').onclick = async () => {
        if (!confirm('Удалить черновик?')) return;
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
      return;
    }

    const payHint = `<p class="hint" style="margin-top:10px">${esc(inv.summary)}</p>
      <p class="ptype" style="margin-top:8px">${inv.payment_type === 'cash' ? 'Нал' : 'Безнал'}${isPlan ? ' · план' : ''}</p>
      ${isPlan ? `<p class="hint">План ${money(planned)} · оплачено ${money(factPaid)} · остаток ${money(remaining)}</p>` : ''}
      ${!inv.has_budget && can ? '<div class="warn-box">⚠ Статья бюджета не выбрана — укажите её ниже, иначе расход уйдёт «к разнесению».</div>' : ''}`;

    const downloadBtn = inv.has_file
      ? `<button class="btn btn-brass" type="button" id="btnDownload">Скачать счёт</button>`
      : '';
    const chatBtn = inv.has_file && (can || isPlan)
      ? `<button class="btn btn-quiet" type="button" id="btnOpen">Счёт в чат</button>`
      : '';
    let editPanel = '';
    if (can) {
      editPanel = `
        ${chatBtn}
        <button class="btn btn-quiet" type="button" id="btnMore">Ещё</button>
        <div id="editPanel" hidden>
          <div class="field"><label>Назначение</label>
            <textarea id="fSummary">${esc(inv.summary)}</textarea></div>
          ${isPlan
            ? `<p class="hint">Сумма плана: ${money(planned)}. Факт оплаты пишется кнопкой «Оплатить» сверху — поле «Сумма» счёта тут не нужно.</p>
               <input type="hidden" id="fAmount" value="${inv.amount || ''}">`
            : `<div class="field"><label>Сумма</label>
            <input id="fAmount" inputmode="decimal" value="${inv.amount || ''}"></div>`}
          <div class="field"><label>Статья</label>
            ${budgetPickerHtml('fBudget', inv.budget_item_id)}
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
    } else if (!can && isPlan) {
      editPanel = chatBtn;
    }

    let payPanel = '';
    if (!isDraft && inv.status !== 'paid' && isPlan) {
      const defPay = remaining > 0 ? remaining : planned;
      payPanel = `
        <div class="pay-box">
          <div class="field"><label>Сумма оплаты</label>
            <input id="fPayAmt" inputmode="decimal" value="${defPay}"></div>
          <div class="field"><label>Подтверждение (ПП, фото, скрин)</label>
            <input id="fPayFile" type="file" accept="application/pdf,image/*,.jpg,.jpeg,.png,.webp,.bmp"></div>
          <p class="hint">Файл уйдёт в чат расходов: «сумма р- назначение. Нал/Безнал».</p>
          <button class="btn btn-ink" type="button" id="btnPaid">Оплатить</button>
        </div>`;
    } else if (!isDraft && inv.status !== 'paid' && can) {
      payPanel = '<button class="btn btn-ink" type="button" id="btnPaid">Оплачено</button>';
    }

    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <div class="card">
        <div class="amount-xl">${money(shownAmt)}</div>
        ${payHint}
        ${downloadBtn}
        ${payPanel}
        ${editPanel}
        ${isPlan ? '' : lines}
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    if (can) bindBudgetPickers(view);
    const openBtn = document.getElementById('btnOpen');
    if (openBtn) openBtn.onclick = () => openInvoice(inv);
    const dlBtn = document.getElementById('btnDownload');
    if (dlBtn) dlBtn.onclick = () => downloadInvoiceFile(inv);
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
      if (!inv.has_budget) {
        panel.removeAttribute('hidden');
        more.textContent = 'Скрыть';
      }
    }
    const save = document.getElementById('btnSave');
    if (save) save.onclick = () => saveInv(inv.id, false);
    const attach = document.getElementById('btnAttach');
    if (attach) attach.onclick = () => attachPdf(inv.id);
    const paid = document.getElementById('btnPaid');
    if (paid) paid.onclick = () => markPaid(inv.id, isPlan);
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

  async function markPaid(id, isPlan) {
    view.classList.add('busy');
    try {
      let res;
      if (isPlan) {
        const raw = ((document.getElementById('fPayAmt') || {}).value || '').trim();
        if (!raw) {
          alert('Укажите сумму оплаты');
          return;
        }
        const fileInput = document.getElementById('fPayFile');
        const file = fileInput && fileInput.files && fileInput.files[0];
        if (!confirm('Провести оплату ' + raw + ' ₽' + (file ? ' с файлом' : '') + '?')) return;
        const fd = new FormData();
        fd.append('amount', raw);
        if (file) fd.append('file', file);
        res = await api('/tg/pay/api/invoices/' + id + '/mark-paid', {
          method: 'POST',
          body: fd,
        });
      } else {
        if (!confirm('Счёт оплачен? Он исчезнет из списка.')) return;
        res = await api('/tg/pay/api/invoices/' + id + '/mark-paid', {
          method: 'POST',
          body: '{}',
        });
      }
      haptic('medium');
      if (isPlan && res && res.closed === false) {
        location.hash = '#/inv/' + id;
        route();
        return;
      }
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

  async function downloadInvoiceFile(inv) {
    try {
      if (!window.FFTg || !window.FFTg.fetchBlob) {
        alert('Не удалось скачать файл.');
        return;
      }
      const blob = await window.FFTg.fetchBlob('/tg/pay/api/invoices/' + inv.id + '/file');
      let name = (inv.original_name || 'invoice.pdf').split(/[/\\]/).pop() || 'invoice.pdf';
      if (!/\.(pdf|png|jpe?g|webp|bmp)$/i.test(name)) name = 'invoice.pdf';
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = name;
      a.target = '_blank';
      a.rel = 'noopener';
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 15000);
      haptic('medium');
    } catch (e) {
      alert(e.message || 'Не удалось скачать файл.');
    }
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
      <a class="choice" href="#/quick">
        <div class="choice-k">Быстрый расход</div>
        <p>Сумма, назначение, нал/безнал и файл — админ разнесёт по статье.</p>
      </a>
      <a class="choice" href="#/week-plan">
        <div class="choice-k">План недели</div>
        <p>Общий план или одна строка.</p>
      </a>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
  }

  function renderWeekPlanMenu() {
    setTitle('План недели');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← назад</button>
      <a class="choice" href="#/week-plan/full">
        <div class="choice-k">Общий план</div>
        <p>Пятничный план: нал и безнал, сохранить и закрепить у бота.</p>
      </a>
      <a class="choice" href="#/week-plan/line">
        <div class="choice-k">Одна строка</div>
        <p>Одна позиция плана без общего закрепа.</p>
      </a>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/new'; };
  }

  async function renderQuickExpense(holdId) {
    if (!(me.can_edit || me.role === 'executive')) {
      location.hash = '#/';
      return;
    }
    setTitle('Быстрая оплата');
    view.innerHTML = loadingCard(holdId ? 'Открываем быструю оплату…' : 'Загружаем форму…');
    let pre = null;
    if (holdId) {
      try {
        pre = await apiTimed('/tg/pay/api/quick-hold/' + holdId, {}, 20000);
      } catch (e) {
        view.innerHTML = `<div class="empty"><h2>Форма не открылась</h2><p>${esc(e.message || 'Не удалось загрузить файл')}</p><button class="btn btn-ink" type="button" id="goEmpty">Пустая форма</button></div>`;
        const go = document.getElementById('goEmpty');
        if (go) go.onclick = () => { location.hash = '#/quick'; };
        return;
      }
    }
    const amountVal = pre && Number(pre.amount) > 0 ? String(pre.amount) : '';
    const summaryVal = pre ? String(pre.summary || '') : '';
    const startType = pre && pre.payment_type === 'cash' ? 'cash' : 'cashless';
    const heldName = pre && pre.has_file ? (pre.filename || 'файл из чата') : '';
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← к списку</button>
      <div class="card" id="quickCard">
        <p class="hint" style="margin:0 0 12px">Админ получит уведомление и разнесёт по статье бюджета.</p>
        <div class="field"><label>Сумма, ₽</label>
          <input id="qAmount" inputmode="decimal" placeholder="0" value="${esc(amountVal)}" autofocus></div>
        <div class="field"><label>Назначение</label>
          <input id="qSummary" type="text" placeholder="топливо, сетка, ЧОП…" value="${esc(summaryVal)}"></div>
        <div class="field"><label>Оплата</label>
          <div class="pay-toggle" id="qType">
            <button type="button" class="pay-opt" data-v="cashless" aria-pressed="${startType === 'cashless' ? 'true' : 'false'}">Безнал</button>
            <button type="button" class="pay-opt" data-v="cash" aria-pressed="${startType === 'cash' ? 'true' : 'false'}">Нал</button>
          </div>
        </div>
        <div class="field"><label>Файл${heldName ? '' : ' (необязательно)'}</label>
          <label class="file-btn">Прикрепить фото или PDF
            <input id="qFile" class="file-proxy" type="file" accept="image/*,application/pdf,.pdf,.jpg,.jpeg,.png,.webp">
          </label>
          <p class="hint" id="qFileName" style="margin-top:6px">${heldName ? esc('Из чата: ' + heldName) : ''}</p>
        </div>
        <button class="btn btn-ink" type="button" id="btnQuick">Отправить</button>
        <div id="qStatus"></div>
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    let ptype = startType;
    view.querySelectorAll('#qType .pay-opt').forEach((btn) => {
      btn.onclick = () => {
        ptype = btn.dataset.v;
        view.querySelectorAll('#qType .pay-opt').forEach((b) => {
          b.setAttribute('aria-pressed', b === btn ? 'true' : 'false');
        });
      };
    });
    const fileInput = document.getElementById('qFile');
    const fileName = document.getElementById('qFileName');
    fileInput.onchange = () => {
      const f = fileInput.files && fileInput.files[0];
      fileName.textContent = f ? f.name : (heldName ? ('Из чата: ' + heldName) : '');
    };
    const sendBtn = document.getElementById('btnQuick');
    sendBtn.onclick = async () => {
      if (sendBtn.disabled) return;
      const status = document.getElementById('qStatus');
      const amount = (document.getElementById('qAmount') || {}).value;
      const summary = (document.getElementById('qSummary') || {}).value;
      if (!String(amount || '').trim() || !String(summary || '').trim()) {
        status.innerHTML = '<p class="hint">Укажите сумму и назначение.</p>';
        return;
      }
      const fd = new FormData();
      fd.append('amount', amount);
      fd.append('summary', summary);
      fd.append('payment_type', ptype);
      if (holdId) fd.append('hold_id', String(holdId));
      if (fileInput.files && fileInput.files[0]) fd.append('file', fileInput.files[0]);
      sendBtn.disabled = true;
      view.classList.add('busy');
      status.innerHTML = loadingCard('Отправляем…');
      try {
        await apiTimed('/tg/pay/api/quick-expense', { method: 'POST', body: fd }, 25000);
        haptic('medium');
        try { sessionStorage.setItem('ff_quick_ok', '1'); } catch (_) {}
        location.hash = '#/';
      } catch (e) {
        status.innerHTML = `<p class="hint">${esc(e.message || 'Не удалось отправить')}</p>`;
        view.classList.remove('busy');
        if (!e.timeout) sendBtn.disabled = false;
      }
    };
    setTimeout(() => {
      const el = document.getElementById('qAmount');
      if (el && !amountVal) el.focus();
    }, 80);
  }

  function weekPlanRowHtml(item, idx, canEdit) {
    const id = item.id || '';
    const ptype = item.payment_type === 'cash' ? 'cash' : 'cashless';
    const locked = item.has_fact ? ' data-locked="1"' : '';
    if (!canEdit) {
      const open = id ? ` href="#/inv/${id}"` : '';
      return `<a class="week-row"${open}>
        <div class="week-row-main"><b>${esc(_fmtPreviewAmt(item.planned_amount))}</b> — ${esc(item.summary || '')}</div>
        <div class="muted">${ptype === 'cash' ? 'нал' : 'безнал'}${item.budget_name ? ' · ' + esc(item.budget_name) : ' · ⚠ нет статьи'}${item.has_fact ? ' · есть факт' : ''}${id ? ' · открыть' : ''}</div>
      </a>`;
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
      <div class="field" style="margin-top:8px"><label>Статья бюджета</label>
        ${budgetPickerHtml('wpBudget' + idx, item.budget_item_id)}
      </div>
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
      const bidRaw = ((row.querySelector('.wp-budget-id') || {}).value || '').trim();
      if (!summary && !planned_amount) return;
      const item = { summary, planned_amount, payment_type };
      if (idRaw) item.id = Number(idRaw);
      if (bidRaw) item.budget_item_id = Number(bidRaw);
      items.push(item);
    });
    return items;
  }

  async function renderWeekPlan() {
    setTitle('План недели');
    if (me && me.can_edit) await ensureBudgetItems();
    const data = await api('/tg/pay/api/week-plan');
    const canEdit = !!(me && me.can_edit);
    const items = data.items || [];
    let body;
    if (canEdit) {
      body = items.map((it, i) => weekPlanRowHtml(it, i, true)).join('')
        || '<p class="hint">Пока пусто — добавьте строки нал и безнал, сразу укажите статью.</p>';
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
      <button class="back" type="button" id="goBack">← назад</button>
      <div class="card">
        <p class="hint" style="margin:0">Период: <b>${esc(data.period_label || '')}</b></p>
      </div>
      ${body}
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/week-plan'; };
    if (canEdit) bindBudgetPickers(view);
    const addBtn = document.getElementById('btnAddRow');
    if (addBtn) {
      addBtn.onclick = () => {
        const wrap = document.getElementById('weekRows');
        const idx = wrap.querySelectorAll('.week-row').length;
        wrap.insertAdjacentHTML('beforeend', weekPlanRowHtml({
          summary: '', planned_amount: '', payment_type: 'cashless',
        }, idx, true));
        bindWeekRowDeletes();
        bindBudgetPickers(wrap);
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
        const bad = items.find((it) => {
          const n = Number(String(it.planned_amount || '').replace(/\s/g, '').replace(',', '.'));
          return !(n > 0) || !(it.summary || '').trim();
        });
        if (bad) {
          status.textContent = 'У каждой строки нужны назначение и сумма больше 0.';
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
          try { sessionStorage.setItem('ff_week_ok', '1'); } catch (_) {}
          goHome();
        } catch (e) {
          status.textContent = errHuman(e, 'Ошибка сохранения');
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
    setTitle('Одна строка плана');
    await ensureBudgetItems();
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← назад</button>
      <div class="card">
        <p class="hint">Пятничный план: сумма, назначение и статья. Файл прикрепите в течение недели.</p>
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
        <div class="field"><label>Статья бюджета</label>
          ${budgetPickerHtml('pBudget', null)}
        </div>
        <button class="btn btn-ink" type="button" id="btnPlan">Создать план</button>
        <p class="hint" id="pStatus"></p>
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/week-plan'; };
    bindBudgetPickers(view);
    document.getElementById('btnPlan').onclick = createPlan;
  }

  async function createPlan() {
    const status = document.getElementById('pStatus');
    const summary = (document.getElementById('pSummary') || {}).value;
    const planned_amount = (document.getElementById('pAmount') || {}).value;
    view.classList.add('busy');
    try {
      await api('/tg/pay/api/invoices/plan', {
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
      goHome();
    } catch (e) {
      status.textContent = errHuman(e, 'Не удалось создать план');
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

  function budgetSelect(selectedId, fieldId) {
    return budgetPickerHtml(fieldId || ('ibBudget' + Math.random().toString(36).slice(2, 8)), selectedId);
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
          <div class="field"><label>Статья</label>${budgetSelect(it.suggested_budget_item_id, 'ib' + it.id)}</div>
          ${it.invoice ? '<button class="btn btn-ink" type="button" data-act="invoice">Это оплата счёта</button>' : ''}
          <button class="btn btn-brass" type="button" data-act="expense">В расходы</button>
          <button class="btn btn-ghost" type="button" data-act="reject">Не расход</button>
        </div>`;
      }
      html += '</div>';
    }
    view.innerHTML = html;
    document.getElementById('goBack').onclick = () => { location.hash = '#/'; };
    bindBudgetPickers(view);
    view.querySelectorAll('.inbox-card').forEach((card) => {
      const id = card.dataset.id;
      card.querySelectorAll('[data-act]').forEach((btn) => {
        btn.onclick = () => inboxAct(id, btn.dataset.act, card);
      });
    });
  }

  async function inboxAct(id, act, card) {
    const hid = card.querySelector('.wp-budget-id');
    const bid = hid && hid.value ? hid.value : null;
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

  function todayISO() {
    const d = new Date();
    const z = (n) => String(n).padStart(2, '0');
    return d.getFullYear() + '-' + z(d.getMonth() + 1) + '-' + z(d.getDate());
  }

  function ruDate(iso) {
    if (!iso) return '';
    const p = String(iso).slice(0, 10).split('-');
    if (p.length !== 3) return iso;
    return p[2] + '.' + p[1] + '.' + p[0];
  }

  function cashWord(balance) {
    const n = Number(balance) || 0;
    if (n > 0.004) return 'мы должны ему';
    if (n < -0.004) return 'он должен кассе';
    return 'расчётов нет';
  }

  function cashTone(balance) {
    const n = Number(balance) || 0;
    if (n > 0.004) return 'cash-plus';
    if (n < -0.004) return 'cash-minus';
    return '';
  }

  async function renderCashList() {
    setTitle('Касса');
    const hidden = (location.hash || '').includes('hidden=1');
    const data = await api('/tg/pay/api/cash/holders' + (hidden ? '?hidden=1' : ''));
    const rows = data.holders || [];
    let html = `
      <button class="back" type="button" id="goBack">${hidden ? '← к кассе' : '← к счетам'}</button>
      <p class="hint" style="margin-top:0">Наличные по сотрудникам. В табель не записываются. Плюс — мы должны, минус — он должен кассе.</p>
    `;
    if (!hidden) {
      html += `<button class="btn btn-ink" type="button" id="addHolder">Новый сотрудник</button>`;
    }
    if (!rows.length) {
      html += `<div class="empty"><h2>${hidden ? 'Скрытых нет' : 'Пока пусто'}</h2><p>${hidden ? '' : 'Добавьте сотрудника, которому выдаёте наличные или принимаете чеки.'}</p></div>`;
    } else {
      html += '<div class="list">';
      for (const row of rows) {
        html += `<a class="row" href="#/cash/${row.id}">
          <div>
            <div class="name">${esc(row.name)}</div>
            <div class="sub">${esc(cashWord(row.balance))}${row.employee_name ? ' · табель: ' + esc(row.employee_name) : ''}</div>
          </div>
          <div class="amt ${cashTone(row.balance)}">${money(row.balance)}</div>
        </a>`;
      }
      html += '</div>';
    }
    if (!hidden && data.hidden_count) {
      html += `<a class="week-plan-link" href="#/cash?hidden=1">Скрытые · ${data.hidden_count}</a>`;
    }
    view.innerHTML = html;
    document.getElementById('goBack').onclick = () => {
      location.hash = hidden ? '#/cash' : '#/';
    };
    const add = document.getElementById('addHolder');
    if (add) add.onclick = () => { location.hash = '#/cash/new'; };
  }

  async function renderCashForm(id) {
    setTitle(id ? 'Сотрудник' : 'Новый сотрудник');
    const [people, current] = await Promise.all([
      api('/tg/pay/api/cash/employees'),
      id ? api('/tg/pay/api/cash/holders/' + id) : Promise.resolve(null),
    ]);
    const holder = current && current.holder;
    const options = ['<option value="">Не привязан</option>'].concat(
      (people.employees || []).map((emp) =>
        `<option value="${emp.id}"${holder && String(holder.employee_id) === String(emp.id) ? ' selected' : ''}>${esc(emp.name)}</option>`
      )
    ).join('');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← назад</button>
      <div class="card">
        <p class="hint" style="margin-top:0">В табель не попадает. Привязка к действующему сотруднику понадобится позже и сейчас ничего в табеле не меняет.</p>
        <div class="field"><label>Имя</label>
          <input id="cName" type="text" value="${esc(holder ? holder.name : '')}" placeholder="Имя" autofocus></div>
        <div class="field"><label>Сотрудник табеля</label>
          <select id="cEmp">${options}</select></div>
        <button class="btn btn-ink" type="button" id="cSave">${id ? 'Сохранить' : 'Создать'}</button>
        ${id ? '<button class="btn btn-ghost" type="button" id="cDel">Удалить</button>' : ''}
        <p class="hint" id="cStatus"></p>
      </div>
    `;
    document.getElementById('goBack').onclick = () => {
      location.hash = id ? '#/cash/' + id : '#/cash';
    };
    document.getElementById('cSave').onclick = async () => {
      const status = document.getElementById('cStatus');
      const body = {
        name: document.getElementById('cName').value,
        employee_id: document.getElementById('cEmp').value,
      };
      status.textContent = '';
      try {
        const saved = id
          ? await api('/tg/pay/api/cash/holders/' + id, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
          })
          : await api('/tg/pay/api/cash/holders', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
          });
        location.hash = '#/cash/' + (id || saved.holder.id);
      } catch (e) {
        status.textContent = e.message || 'Не сохранилось';
      }
    };
    const del = document.getElementById('cDel');
    if (del) {
      del.onclick = async () => {
        if (!confirm('Удалить сотрудника из кассы?')) return;
        try {
          await api('/tg/pay/api/cash/holders/' + id, { method: 'DELETE' });
          location.hash = '#/cash';
        } catch (e) {
          const msg = e.message || '';
          if (msg.includes('Скрыть') && confirm(msg)) {
            await api('/tg/pay/api/cash/holders/' + id, {
              method: 'PATCH',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ is_active: false }),
            });
            location.hash = '#/cash';
            return;
          }
          alert(msg || 'Не удалось удалить');
        }
      };
    }
  }

  async function renderCashPerson(id) {
    setTitle('Касса');
    const data = await api('/tg/pay/api/cash/holders/' + id);
    const holder = data.holder;
    const moves = data.moves || [];
    let html = `
      <button class="back" type="button" id="goBack">← к кассе</button>
      <div class="card">
        <div class="name">${esc(holder.name)}</div>
        ${holder.employee_name ? `<div class="sub">Табель: ${esc(holder.employee_name)}</div>` : ''}
        <div class="cash-bal ${cashTone(holder.balance)}">${money(holder.balance)}</div>
        <div class="cash-sign">${esc(cashWord(holder.balance))}</div>
        <button class="btn btn-ink" type="button" id="cReceipt">Расход</button>
        <button class="btn btn-brass" type="button" id="cIn">Поступление ДС</button>
        <button class="btn btn-ghost" type="button" id="cEdit">Изменить</button>
      </div>
    `;
    if (moves.length) {
      html += '<div class="sec">Движения</div><div class="list">';
      for (const move of moves) {
        html += `<div class="row">
          <div>
            <div class="name">${esc(move.title)}</div>
            <div class="sub">${esc(ruDate(move.date))}</div>
          </div>
          <div>
            <div class="amt ${cashTone(move.signed)}">${money(move.signed)}</div>
            <button class="btn btn-ghost cash-del" type="button" data-id="${move.id}" data-posted="${move.posted ? '1' : '0'}">Удалить</button>
          </div>
        </div>`;
      }
      html += '</div>';
    }
    view.innerHTML = html;
    document.getElementById('goBack').onclick = () => { location.hash = '#/cash'; };
    document.getElementById('cReceipt').onclick = () => { location.hash = '#/cash/' + id + '/receipt'; };
    document.getElementById('cIn').onclick = () => { location.hash = '#/cash/' + id + '/in'; };
    document.getElementById('cEdit').onclick = () => { location.hash = '#/cash/' + id + '/edit'; };
    view.querySelectorAll('.cash-del').forEach((btn) => {
      btn.onclick = async () => {
        const posted = btn.dataset.posted === '1';
        const ask = posted
          ? 'Удалить поступление? Оно уйдёт из кассы и из расходов.'
          : 'Удалить этот расход? Он есть только в кассе.';
        if (!confirm(ask)) return;
        try {
          await api('/tg/pay/api/cash/moves/' + btn.dataset.id, { method: 'DELETE' });
          await renderCashPerson(id);
        } catch (e) {
          alert(e.message || 'Не удалось удалить');
        }
      };
    });
  }

  async function renderCashReceipt(id) {
    setTitle('Расход');
    view.innerHTML = `
      <button class="back" type="button" id="goBack">← назад</button>
      <div class="card">
        <p class="hint" style="margin-top:0">Расход сотрудника. Пишется только в кассу, в чат и в базу не уходит.</p>
        <div class="field"><label>Сумма, ₽</label>
          <input id="cAmount" inputmode="decimal" placeholder="0"></div>
        <div class="field"><label>Дата</label>
          <input id="cDate" type="date" value="${todayISO()}"></div>
        <div class="field"><label>На что</label>
          <input id="cPurpose" placeholder="например, бензин"></div>
        <button class="btn btn-ink" type="button" id="cSave">Записать</button>
        <p class="hint" id="cStatus"></p>
      </div>
    `;
    document.getElementById('goBack').onclick = () => { location.hash = '#/cash/' + id; };
    document.getElementById('cSave').onclick = async () => {
      const status = document.getElementById('cStatus');
      status.textContent = '';
      try {
        await api('/tg/pay/api/cash/holders/' + id + '/receipt', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            amount: document.getElementById('cAmount').value,
            date: document.getElementById('cDate').value,
            purpose: document.getElementById('cPurpose').value,
          }),
        });
        location.hash = '#/cash/' + id;
      } catch (e) {
        status.textContent = e.message || 'Не сохранилось';
      }
    };
  }

  async function renderCashPayout(id) {
    setTitle('Поступление ДС');
    await ensureBudgetItems();
    const week = await api('/tg/pay/api/cash/week-plans');
    const plans = week.plans || [];
    let mode = 'article';
    let planId = '';

    function draw() {
      const prevAmount = (document.getElementById('cAmount') || {}).value || '';
      const prevDate = (document.getElementById('cDate') || {}).value || todayISO();
      const prevBudget = (document.getElementById('cBudget') || {}).value || '';
      let plansHtml = '';
      if (mode === 'plan') {
        if (!plans.length) {
          plansHtml = '<p class="hint">В плане этой недели нет открытых строк.</p>';
        } else {
          plansHtml = '<div class="sec">План недели</div><div class="list">';
          for (const row of plans) {
            const picked = String(row.id) === String(planId) ? ' cash-picked' : '';
            const kind = row.payment_type === 'cash' ? 'нал' : 'безнал';
            const article = row.budget_name || 'нет статьи';
            plansHtml += `<button type="button" class="row cash-plan${picked}" data-plan="${row.id}">
              <div>
                <div class="name">${esc(row.title)}</div>
                <div class="sub">${esc(article)} · ${kind} · осталось ${money(row.left)}</div>
              </div>
            </button>`;
          }
          plansHtml += '</div>';
        }
      }
      view.innerHTML = `
        <button class="back" type="button" id="goBack">← назад</button>
        <div class="card">
          <p class="hint" style="margin-top:0">Пополняем кассу сотрудника. Нал уходит в чат расходов и в базу. Остаток уменьшается: он должен кассе.</p>
          <div class="pay-toggle" id="cMode">
            <button type="button" class="pay-opt" data-v="article" aria-pressed="${mode === 'article' ? 'true' : 'false'}">Новая статья</button>
            <button type="button" class="pay-opt" data-v="plan" aria-pressed="${mode === 'plan' ? 'true' : 'false'}">Из плана</button>
          </div>
          <div class="field"><label>Сумма, ₽</label>
            <input id="cAmount" inputmode="decimal" placeholder="0" value="${esc(prevAmount)}"></div>
          <div class="field"><label>Дата</label>
            <input id="cDate" type="date" value="${esc(prevDate)}"></div>
          ${mode === 'article'
            ? `<div class="field"><label>Статья</label>${budgetPickerHtml('cBudget', prevBudget)}</div>`
            : ''}
          <button class="btn btn-ink" type="button" id="cSave">Провести</button>
          <p class="hint" id="cStatus"></p>
        </div>
        ${mode === 'plan' ? plansHtml : ''}
      `;
      document.getElementById('goBack').onclick = () => { location.hash = '#/cash/' + id; };
      view.querySelectorAll('#cMode .pay-opt').forEach((btn) => {
        btn.onclick = () => {
          mode = btn.dataset.v;
          if (mode !== 'plan') planId = '';
          draw();
        };
      });
      if (mode === 'article') bindBudgetPickers(view);
      view.querySelectorAll('[data-plan]').forEach((btn) => {
        btn.onclick = () => {
          planId = btn.dataset.plan;
          const picked = plans.find((p) => String(p.id) === String(planId));
          draw();
          if (picked) {
            const input = document.getElementById('cAmount');
            if (input) input.value = String(picked.left).replace('.', ',');
          }
        };
      });
      document.getElementById('cSave').onclick = async () => {
        const status = document.getElementById('cStatus');
        status.textContent = '';
        const payload = {
          amount: document.getElementById('cAmount').value,
          date: document.getElementById('cDate').value,
          mode: mode,
        };
        if (mode === 'plan') {
          if (!planId) {
            status.textContent = 'Выберите строку плана';
            return;
          }
          const picked = plans.find((p) => String(p.id) === String(planId));
          if (picked && !picked.budget_item_id) {
            status.textContent = 'У этой строки нет статьи бюджета';
            return;
          }
          payload.plan_id = planId;
        } else {
          const budgetId = (document.getElementById('cBudget') || {}).value;
          if (!budgetId) {
            status.textContent = 'Выберите статью';
            return;
          }
          payload.budget_item_id = budgetId;
        }
        try {
          await api('/tg/pay/api/cash/holders/' + id + '/payout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          location.hash = '#/cash/' + id;
        } catch (e) {
          status.textContent = e.message || 'Не сохранилось';
        }
      };
    }

    draw();
  }

  function esc(s) {
    return String(s || '').replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  boot();
})();
