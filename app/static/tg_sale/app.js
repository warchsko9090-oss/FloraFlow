(function () {
    function tgApp() {
        return (window.FFTg && window.FFTg.tgApp()) || (window.Telegram && window.Telegram.WebApp);
    }
    function getInitData() {
        return window.FFTg ? window.FFTg.getInitData() : "";
    }
    function waitTelegram() {
        return window.FFTg ? window.FFTg.waitTelegram() : Promise.resolve(tgApp());
    }
    const view = document.getElementById("view");
    const titleEl = document.getElementById("screenTitle");
    const state = { me: null, companies: [], allCompanies: [], invoices: [], screen: "list", draft: emptyDraft(), current: null, stockGroups: [], lastQ: "", lastInnLookup: "", orderHits: [], orderQ: "", priceMode: "retail", discountPct: 0 };

    function haptic(kind) {
        try { const tg = tgApp(); tg && tg.HapticFeedback && tg.HapticFeedback.impactOccurred(kind || "light"); } catch (_) {}
    }

    function invNo(inv) {
        return (inv && (inv.number || inv.id)) || "";
    }

    function notifySaved(inv) {
        const msg = invNo(inv) ? `Счёт №${invNo(inv)} сохранён` : "Счёт сохранён";
        const tg = tgApp();
        try { tg && tg.HapticFeedback && tg.HapticFeedback.notificationOccurred("success"); } catch (_) {}
        const toast = document.getElementById("toast");
        if (toast) {
            toast.textContent = msg;
            toast.classList.remove("hide");
            clearTimeout(notifySaved._t);
            notifySaved._t = setTimeout(() => toast.classList.add("hide"), 2800);
        }
        if (tg && typeof tg.showAlert === "function") {
            try { tg.showAlert(msg); } catch (_) {}
        }
    }

    (function bindPress() {
        const sel = "button, .btn, .list-item[data-open], .list-item[data-buh], .firm, .size-row, .order-hit";
        let cur = null;
        let downAt = 0;
        function clear() {
            if (!cur) return;
            const el = cur;
            cur = null;
            const wait = Math.max(0, 90 - (Date.now() - downAt));
            setTimeout(() => el.classList.remove("is-pressed"), wait);
        }
        document.addEventListener("pointerdown", (e) => {
            if (e.pointerType === "mouse" && e.button !== 0) return;
            const t = e.target;
            if (t.closest && t.closest("input, textarea, select")) return;
            const el = t.closest && t.closest(sel);
            if (!el || el.disabled || el.classList.contains("busy")) return;
            if (cur) cur.classList.remove("is-pressed");
            cur = el;
            downAt = Date.now();
            el.classList.add("is-pressed");
            haptic("light");
        }, { passive: true });
        window.addEventListener("pointerup", clear, { passive: true });
        window.addEventListener("pointercancel", clear, { passive: true });
    })();

    function armBusy(el) {
        if (!el) return () => {};
        el.classList.add("busy");
        return () => el.classList.remove("busy");
    }

    function emptyDraft() {
        return {
            company_id: null,
            client_id: null,
            buyer: { name: "", inn: "", kpp: "", ogrn: "", address: "", phone: "", bank: "", rs: "", bik: "", ks: "" },
            lines: [],
            order_id: null,
            order: null,
            anonymous: false,
        };
    }

    function vatIncluded(mode) {
        return mode === "included_20" || mode === "included_22";
    }

    function vatLabel(mode) {
        return vatIncluded(mode) ? "с НДС 22%" : "без НДС";
    }

    function money(n) {
        const x = Number(n || 0);
        return x.toLocaleString("ru-RU", { minimumFractionDigits: 0, maximumFractionDigits: 0 }) + " ₽";
    }

    function fmtDate(s) {
        if (!s) return "";
        const d = new Date(s);
        if (Number.isNaN(d.getTime())) return String(s);
        return d.toLocaleDateString("ru-RU");
    }

    async function api(path, opts) {
        if (window.FFTg) return window.FFTg.api(path, opts);
        throw new Error("Нет входа");
    }

    function setTitle(t) { titleEl.textContent = t; }

    function render() {
        if (state.me && state.me.accountant_only) {
            if (state.screen === "buh-view") renderBuhView();
            else renderBuhList();
            return;
        }
        if (state.screen === "list") renderList();
        else if (state.screen === "edit") renderEdit();
        else if (state.screen === "firms") renderFirms();
        else if (state.screen === "view") renderView();
        else if (state.screen === "pick-order") renderPickOrder();
    }

    function companyName(inv) {
        return inv.company_name || (inv.company && inv.company.short_name) || "";
    }

    function renderList() {
        setTitle("Выставить счёт");
        const rows = (state.invoices || []).map((inv) => `
            <div class="list-item" data-open="${inv.id}">
                <div class="row">
                    <div>
                        <div><b>№${invNo(inv)}</b> · ${esc(inv.anonymous ? "Без плательщика" : (inv.buyer_name || "Без клиента"))}</div>
                        <div class="muted">${esc(companyName(inv))} · ${fmtDate(inv.created_at)}${inv.order_id ? ` · заказ №${inv.order_id}` : ""}</div>
                    </div>
                    <div>
                        <span class="chip ${inv.status === "approved" ? "ok" : ""}">${inv.status === "approved" ? "согласован" : "черновик"}</span>
                        <div style="text-align:right;margin-top:4px;font-weight:700">${money(inv.amount)}</div>
                    </div>
                </div>
            </div>`).join("") || `<p class="muted">Пока нет счетов</p>`;
        view.innerHTML = `
            <div class="grid2" style="margin-bottom:12px">
                <button class="btn gold" id="btnNew">Новый счёт</button>
                <button class="btn ghost" id="btnFromOrder">На заказ</button>
            </div>
            ${state.me && (state.me.can_firms || state.me.can_edit_firms) ? `<button class="btn ghost" id="btnFirms" style="margin-bottom:12px">Фирмы</button>` : ""}
            <div class="card">${rows}</div>`;
        document.getElementById("btnNew").onclick = () => startNewInvoice();
        document.getElementById("btnFromOrder").onclick = () => startPickOrder();
        const bf = document.getElementById("btnFirms");
        if (bf) bf.onclick = () => { state.screen = "firms"; render(); };
        view.querySelectorAll("[data-open]").forEach((el) => {
            el.onclick = () => openInvoice(Number(el.dataset.open));
        });
    }

    function startNewInvoice() {
        state.draft = emptyDraft();
        if (state.companies[0]) state.draft.company_id = state.companies[0].id;
        state.current = null;
        state.screen = "edit";
        render();
    }

    function startPickOrder() {
        state.draft = emptyDraft();
        if (state.companies[0]) state.draft.company_id = state.companies[0].id;
        state.current = null;
        state.orderHits = [];
        state.orderQ = "";
        state.pickFrom = "list";
        state.screen = "pick-order";
        render();
    }

    function orderLinesHtml(ord, limit) {
        const rows = ord && ord.lines ? ord.lines : [];
        const shown = limit ? rows.slice(0, limit) : rows;
        const more = (ord && ord.more_count) || Math.max(0, rows.length - shown.length);
        const lis = shown.map((ln) =>
            `<li>${esc(ln.plant_name || "—")}${ln.size_name ? ` · ${esc(ln.size_name)}` : ""} ×${ln.qty} · ${money(ln.sum || (Number(ln.qty) * Number(ln.price)))}</li>`
        ).join("");
        if (!lis) return `<p class="muted">В заказе нет позиций</p>`;
        return `<ul class="order-lines">${lis}</ul>${more ? `<p class="muted">ещё ${more} поз.</p>` : ""}`;
    }

    function orderBannerHtml(d) {
        const ord = d.order;
        if (ord && ord.id) {
            return `
            <div class="card order-banner">
                <div class="row">
                    <div>
                        <div><b>Заказ №${ord.id}</b> · ${esc(ord.status_label || ord.status || "")} · ${money(ord.amount)}</div>
                        <div class="muted">${esc(ord.client_name || "")}${ord.invoice_number ? ` · счёт ERP ${esc(ord.invoice_number)}` : ""}</div>
                    </div>
                    <button type="button" class="btn sm ghost" id="orderClear">снять</button>
                </div>
                ${orderLinesHtml(ord)}
            </div>`;
        }
        return `<button type="button" class="btn ghost" id="btnPickOrder" style="margin-bottom:10px">Привязать заказ из базы</button>`;
    }

    function bindOrderBanner() {
        const pick = document.getElementById("btnPickOrder");
        if (pick) pick.onclick = () => {
            state.orderHits = [];
            state.orderQ = "";
            state.pickFrom = "edit";
            state.screen = "pick-order";
            render();
        };
        const clr = document.getElementById("orderClear");
        if (clr) clr.onclick = () => {
            state.draft.order_id = null;
            state.draft.order = null;
            render();
        };
    }

    function applyOrder(ord) {
        const d = state.draft;
        d.order_id = ord.id;
        d.order = ord;
        d.client_id = ord.client_id || null;
        const src = ord.buyer || {};
        d.buyer = {
            name: src.name || ord.client_name || "",
            inn: src.inn || ord.client_inn || "",
            kpp: src.kpp || "", ogrn: src.ogrn || "", address: src.address || "",
            phone: src.phone || "", bank: src.bank || "", rs: src.rs || "",
            bik: src.bik || "", ks: src.ks || "",
        };
        d.lines = (ord.lines || []).map((ln) => ({
            plant_id: ln.plant_id, size_id: ln.size_id,
            plant_name: ln.plant_name, size_name: ln.size_name,
            qty: ln.qty, price: ln.price, free_qty: ln.qty,
        }));
        state.screen = "edit";
        render();
    }

    function renderPickOrder() {
        setTitle("Счёт на заказ");
        const hits = state.orderHits || [];
        const q = state.orderQ || "";
        const list = !q
            ? `<p class="muted">Введите номер заказа, клиента или ИНН. Список сам не открывается — так не смешаются три заказа одного клиента.</p>`
            : (hits.map((ord) => `
                <button type="button" class="order-hit" data-oid="${ord.id}">
                    <div class="row">
                        <div>
                            <div><b>Заказ №${ord.id}</b> · ${esc(ord.status_label || "")}</div>
                            <div class="muted">${esc(ord.client_name || "Без клиента")}</div>
                        </div>
                        <div class="hit-sum">${money(ord.amount)}</div>
                    </div>
                    ${orderLinesHtml(ord, 4)}
                </button>`).join("") || `<p class="muted">Ничего не найдено</p>`);
        view.innerHTML = `
            <button class="btn ghost" id="back">← Назад</button>
            <input class="input" id="orderQ" placeholder="№ заказа, клиент или ИНН" value="${esc(q)}" inputmode="search">
            <div id="orderHits" class="order-hits">${list}</div>`;
        document.getElementById("back").onclick = () => {
            state.screen = (state.pickFrom === "edit" || state.draft.order_id) ? "edit" : "list";
            render();
        };
        const inp = document.getElementById("orderQ");
        inp.focus();
        let t = null;
        inp.oninput = () => {
            state.orderQ = inp.value;
            clearTimeout(t);
            t = setTimeout(() => searchOrders(inp.value), 220);
        };
        bindOrderHits();
        if (q) searchOrders(q);
    }

    function bindOrderHits() {
        view.querySelectorAll("[data-oid]").forEach((el) => {
            el.onclick = async () => {
                const id = Number(el.dataset.oid);
                const cached = (state.orderHits || []).find((x) => Number(x.id) === id);
                try {
                    const full = await api(`/tg/sale/api/orders/${id}`);
                    applyOrder(full);
                } catch (e) {
                    if (cached) applyOrder(cached);
                    else alert(e.message || "Не удалось открыть заказ");
                }
            };
        });
    }

    async function searchOrders(q) {
        const box = document.getElementById("orderHits");
        if (!box) return;
        const query = String(q || "").trim();
        state.orderQ = query;
        if (!query) {
            state.orderHits = [];
            box.innerHTML = `<p class="muted">Введите номер заказа, клиента или ИНН. Список сам не открывается — так не смешаются три заказа одного клиента.</p>`;
            return;
        }
        try {
            const data = await api(`/tg/sale/api/orders?q=${encodeURIComponent(query)}`);
            state.orderHits = data.orders || [];
            box.innerHTML = state.orderHits.map((ord) => `
                <button type="button" class="order-hit" data-oid="${ord.id}">
                    <div class="row">
                        <div>
                            <div><b>Заказ №${ord.id}</b> · ${esc(ord.status_label || "")}</div>
                            <div class="muted">${esc(ord.client_name || "Без клиента")}</div>
                        </div>
                        <div class="hit-sum">${money(ord.amount)}</div>
                    </div>
                    ${orderLinesHtml(ord, 4)}
                </button>`).join("") || `<p class="muted">Ничего не найдено</p>`;
            bindOrderHits();
        } catch (e) {
            box.innerHTML = `<p class="err">${esc(e.message || "Не удалось найти заказы")}</p>`;
        }
    }

    function buyerFields(b) {
        const f = (k, l) => `<div class="label">${l}</div><input class="input" data-b="${k}" value="${esc(b[k] || "")}">`;
        return `<div class="label">Название</div>
               <div class="buyer-wrap">
                 <input class="input" id="buyerName" data-b="name" value="${esc(b.name || "")}" placeholder="Начните вводить — найдём в базе" autocomplete="off">
                 <div id="buyerSuggest" class="buyer-suggest hide"></div>
               </div>
               <div class="muted">Если компания уже есть в ERP, выберите её из списка — подставим реквизиты</div>
               <div class="label">ИНН</div>
               <div class="inn-row">
                 <input class="input" data-b="inn" inputmode="numeric" value="${esc(b.inn || "")}" placeholder="10 или 12 цифр">
                 <button type="button" class="btn sm ghost" id="innLookup">По ИНН</button>
               </div>
               <div class="muted" id="innHint">${esc(b._hint || "Подставим название, КПП, ОГРН и адрес из ЕГРЮЛ")}</div>`
            + f("kpp", "КПП") + f("ogrn", "ОГРН") + f("address", "Адрес") + f("phone", "Телефон")
            + f("bank", "Банк") + f("rs", "Расчётный счёт") + f("bik", "БИК") + f("ks", "Корр. счёт");
    }

    function renderEdit() {
        setTitle(state.current ? `Счёт №${invNo(state.current)}` : "Новый счёт");
        const d = state.draft;
        const firms = state.companies.map((c) => `
            <button type="button" class="card firm ${Number(d.company_id) === Number(c.id) ? "on" : ""}" data-co="${c.id}">
                <div><b>${esc(c.short_name)}</b></div>
                <div class="muted">${vatLabel(c.vat_mode)}</div>
            </button>`).join("") || `<p class="muted">Сначала заполните фирмы (админ)</p>`;
        view.innerHTML = `
            <button class="btn ghost" id="back">← К списку</button>
            ${orderBannerHtml(d)}
            <label class="check-row">
                <input type="checkbox" id="anon"${d.anonymous ? " checked" : ""}>
                <span>Обезличенный счёт — в PDF только поставщик, без плательщика</span>
            </label>
            <div class="label">Клиент</div>
            <div class="card">
                <input type="file" id="buyerFile" accept=".pdf,.doc,.docx,image/*">
                <p class="muted" style="margin-top:8px">PDF, Word или фото реквизитов — либо ИНН ниже</p>
                <div id="parseErr" class="err hide"></div>
                ${buyerFields(d.buyer)}
            </div>
            <div class="label">Фирма</div>
            ${firms}
            <div class="label">Позиции</div>
            <div class="card">
                <div class="price-bar">
                    <div class="grid2">
                        <button type="button" class="btn sm ${state.priceMode === "wholesale" ? "" : "ghost"}" id="modeWholesale">Опт</button>
                        <button type="button" class="btn sm ${state.priceMode === "retail" ? "" : "ghost"}" id="modeRetail">Розница</button>
                    </div>
                    <div class="label" style="margin-top:10px">Скидка</div>
                    <div class="disc-chips" id="discChips">
                        ${[0,5,10,15,20,25].map((d) => `<button type="button" class="btn sm ghost disc-chip${Number(state.discountPct)===d?" on":""}" data-pct="${d}">${d}%</button>`).join("")}
                    </div>
                    <div class="inn-row" style="margin-top:8px">
                        <input class="input" id="discCustom" type="number" min="0" max="100" step="0.1" inputmode="decimal" value="${esc(String(state.discountPct || 0))}" placeholder="своя %">
                        <button type="button" class="btn sm" id="applyDisc">Ко всем</button>
                    </div>
                    <p class="muted" style="margin-top:6px">Скидка к прайсу (опт/розница). У позиции можно задать свою %.</p>
                </div>
                <input class="input" id="q" placeholder="Название или размер, например туя 160" value="${esc(state.lastQ || "")}">
                <div id="suggest" class="suggest"></div>
                <div id="linesBox"></div>
            </div>
            <div class="card row"><span class="muted">Итого</span><span class="tot" id="totVal">0 ₽</span></div>
            <div id="saveErr" class="err hide"></div>
            <button class="btn gold" id="save">Сохранить счёт</button>
            ${state.current ? `<div class="grid2" style="margin-top:8px">
                <button class="btn" id="pdf">Счёт в чат</button>
                <button class="btn ghost" id="approve">Согласовать</button>
            </div>
            <button class="btn danger" id="discard" style="margin-top:8px">Удалить</button>` : ""}`;
        document.getElementById("back").onclick = () => { state.screen = "list"; render(); };
        bindOrderBanner();
        const anon = document.getElementById("anon");
        if (anon) anon.onchange = () => { d.anonymous = anon.checked; };
        view.querySelectorAll("[data-co]").forEach((el) => {
            el.onclick = () => { d.company_id = Number(el.dataset.co); render(); };
        });
        view.querySelectorAll("[data-b]").forEach((el) => {
            el.oninput = () => {
                d.buyer[el.dataset.b] = el.value;
                // Ручной ввод имени/ИНН сбрасывает явный client_id —
                // дальше сервер ищет клиента по реквизитам.
                if (el.dataset.b === "name" || el.dataset.b === "inn") {
                    d.client_id = null;
                }
            };
        });
        bindInnLookup();
        bindBuyerSuggest();
        bindPriceBar();
        refreshLines();
        const q = document.getElementById("q");
        let t = null;
        q.oninput = () => {
            clearTimeout(t);
            t = setTimeout(() => searchStock(q.value), 220);
        };
        if (state.lastQ) searchStock(state.lastQ);
        document.getElementById("buyerFile").onchange = parseBuyer;
        document.getElementById("save").onclick = saveDraft;
        const pdf = document.getElementById("pdf");
        if (pdf) pdf.onclick = sendPdf;
        const ap = document.getElementById("approve");
        if (ap) ap.onclick = approveInv;
        const ds = document.getElementById("discard");
        if (ds) ds.onclick = discardInv;
    }

    function renderView() {
        const inv = state.current;
        setTitle(`Счёт №${invNo(inv)}`);
        view.innerHTML = `
            <button class="btn ghost" id="back">← К списку</button>
            <div class="card">
                <span class="chip ok">согласован</span>
                ${inv.anonymous ? `<span class="chip">без плательщика</span>` : ""}
                <h2 style="margin:10px 0 4px">${esc(inv.anonymous ? "Обезличенный счёт" : inv.buyer_name)}</h2>
                <p class="muted">${esc(companyName(inv))}${inv.order_id ? ` · заказ №${inv.order_id}` : ""}</p>
                <div class="tot" style="margin-top:10px">${money(inv.amount)}</div>
            </div>
            ${inv.order ? `<div class="card order-banner">${orderLinesHtml(inv.order)}</div>` : ""}
            <button class="btn gold" id="pdf">Счёт в чат</button>
            ${state.me && state.me.can_delete_approved ? `<button class="btn danger" id="discard" style="margin-top:8px">Удалить счёт${inv.from_existing_order ? "" : " и заказ"}</button>` : ""}`;
        document.getElementById("back").onclick = () => { state.screen = "list"; render(); };
        document.getElementById("pdf").onclick = sendPdf;
        const ds = document.getElementById("discard");
        if (ds) ds.onclick = discardInv;
    }

    function renderFirms() {
        setTitle("Фирмы");
        const rows = state.allCompanies.length ? state.allCompanies : state.companies;
        view.innerHTML = `<button class="btn ghost" id="back">← Назад</button>` + rows.map((c) => `
            <div class="card">
                <div class="chip gold">${vatLabel(c.vat_mode)}</div>
                ${!c.filled ? `<p class="muted" style="margin-top:6px">Не заполнена: нет ИНН / р/с / БИК — в счёте не показывается</p>` : ""}
                ${field("short_name", "Короткое имя", c)}
                ${field("legal_name", "Юридическое имя", c)}
                ${field("inn", "ИНН", c)}
                ${field("kpp", "КПП", c)}
                ${field("ogrn", "ОГРН", c)}
                ${field("legal_address", "Юр. адрес", c)}
                ${field("phone", "Телефон", c)}
                ${field("bank_name", "Банк", c)}
                ${field("bik", "БИК", c)}
                ${field("rs", "р/с", c)}
                ${field("ks", "к/с", c)}
                ${field("director", "Подпись", c)}
                <div class="label">НДС</div>
                <select class="input" data-k="vat_mode">
                    <option value="none" ${vatIncluded(c.vat_mode) ? "" : "selected"}>без НДС</option>
                    <option value="included_22" ${vatIncluded(c.vat_mode) ? "selected" : ""}>с НДС 22%</option>
                </select>
                <div class="label">Печать с подписью</div>
                <p class="muted">PNG с прозрачным фоном: круглая печать и подпись на одном файле. 800–1200 px (на счёте около 8×8 см), до 2 МБ. JPG или WebP тоже можно, лучше PNG.</p>
                <p class="muted">${c.has_stamp ? `Сейчас: ${esc(c.stamp_name || "загружена")}` : "Ещё не загружена"}</p>
                <input class="input" type="file" accept="image/png,image/jpeg,image/webp" data-stamp>
                <div class="grid2" style="margin-top:8px">
                    <button class="btn ghost" data-stamp-co="${c.id}">Загрузить печать</button>
                    ${c.has_stamp ? `<button class="btn danger" data-unstamp-co="${c.id}">Убрать</button>` : `<span></span>`}
                </div>
                <button class="btn" data-save-co="${c.id}" style="margin-top:10px">Сохранить</button>
            </div>`).join("");
        document.getElementById("back").onclick = () => { state.screen = "list"; render(); };
        view.querySelectorAll("[data-save-co]").forEach((btn) => {
            btn.onclick = () => saveCompany(Number(btn.dataset.saveCo), btn.closest(".card"));
        });
        view.querySelectorAll("[data-stamp-co]").forEach((btn) => {
            btn.onclick = () => uploadStamp(Number(btn.dataset.stampCo), btn.closest(".card"));
        });
        view.querySelectorAll("[data-unstamp-co]").forEach((btn) => {
            btn.onclick = () => clearStamp(Number(btn.dataset.unstampCo), btn);
        });
    }

    function field(k, l, c) {
        return `<div class="label">${l}</div><input class="input" data-k="${k}" value="${esc(c[k] || "")}">`;
    }

    async function saveCompany(id, card) {
        const done = armBusy(card.querySelector("[data-save-co]"));
        try {
            const payload = {};
            card.querySelectorAll("[data-k]").forEach((el) => { payload[el.dataset.k] = el.value; });
            await api(`/tg/sale/api/companies/${id}`, { method: "POST", body: JSON.stringify(payload) });
            haptic("medium");
            await reload();
            state.screen = "firms";
            render();
        } finally {
            done();
        }
    }

    async function uploadStamp(id, card) {
        const input = card.querySelector("[data-stamp]");
        if (!input || !input.files || !input.files[0]) {
            alert("Выберите файл: PNG 800–1200 px, печать и подпись на прозрачном фоне, до 2 МБ");
            return;
        }
        const done = armBusy(card.querySelector("[data-stamp-co]"));
        try {
            const fd = new FormData();
            fd.append("file", input.files[0]);
            const init = window.FFTg && window.FFTg.getInitData ? window.FFTg.getInitData() : "";
            if (init) fd.append("initData", init);
            await api(`/tg/sale/api/companies/${id}/stamp`, { method: "POST", body: fd });
            haptic("medium");
            await reload();
            state.screen = "firms";
            render();
        } catch (e) {
            alert(e.message || "Не удалось загрузить печать");
        } finally {
            done();
        }
    }

    async function clearStamp(id, btn) {
        if (!confirm("Убрать печать с этой фирмы?")) return;
        const done = armBusy(btn);
        try {
            await api(`/tg/sale/api/companies/${id}/stamp`, { method: "DELETE", body: "{}" });
            haptic("medium");
            await reload();
            state.screen = "firms";
            render();
        } catch (e) {
            alert(e.message || "Не удалось убрать печать");
        } finally {
            done();
        }
    }

    function applyBuyerClient(c) {
        const b = state.draft.buyer;
        state.draft.client_id = c.id || null;
        ["name", "inn", "kpp", "ogrn", "address", "phone", "bank", "rs", "bik", "ks"].forEach((k) => {
            if (c[k] != null) b[k] = String(c[k] || "");
        });
        const bits = [];
        if (c.inn) bits.push("ИНН " + c.inn);
        if (c.has_bank) bits.push("банк из карточки");
        b._hint = bits.length ? ("Из базы: " + bits.join(" · ")) : "Компания из базы, банковских реквизитов в карточке нет";
        if (c.inn) state.lastInnLookup = String(c.inn).replace(/\D/g, "");
        render();
    }

    function bindBuyerSuggest() {
        const input = document.getElementById("buyerName");
        const box = document.getElementById("buyerSuggest");
        if (!input || !box) return;
        let t = null;
        let seq = 0;
        async function search(q) {
            const my = ++seq;
            const query = String(q || "").trim();
            if (query.length < 2) {
                box.innerHTML = "";
                box.classList.add("hide");
                return;
            }
            try {
                const data = await api("/tg/sale/api/clients", {
                    method: "POST",
                    body: JSON.stringify({ q: query }),
                });
                if (my !== seq) return;
                const rows = data.clients || [];
                if (!rows.length) {
                    box.innerHTML = `<div class="muted" style="padding:10px 12px">В базе нет совпадений — введите реквизиты вручную</div>`;
                    box.classList.remove("hide");
                    return;
                }
                box.innerHTML = rows.map((c, i) => {
                    const sub = [c.inn ? `ИНН ${esc(c.inn)}` : "", c.has_bank ? "есть р/с" : ""].filter(Boolean).join(" · ");
                    return `<button type="button" data-cli="${i}">
                        <b>${esc(c.name)}</b>
                        ${sub ? `<span class="muted">${sub}</span>` : ""}
                    </button>`;
                }).join("");
                box.classList.remove("hide");
                box.querySelectorAll("[data-cli]").forEach((btn) => {
                    btn.onmousedown = (e) => e.preventDefault();
                    btn.onclick = () => applyBuyerClient(rows[Number(btn.dataset.cli)]);
                });
            } catch (_) {
                if (my !== seq) return;
                box.innerHTML = `<div class="muted" style="padding:10px 12px">Не удалось искать. Попробуйте ещё раз</div>`;
                box.classList.remove("hide");
            }
        }
        input.addEventListener("input", () => {
            clearTimeout(t);
            t = setTimeout(() => search(input.value), 220);
        });
        input.addEventListener("focus", () => {
            if (String(input.value || "").trim().length >= 2) search(input.value);
        });
        input.addEventListener("blur", () => {
            setTimeout(() => box.classList.add("hide"), 180);
        });
    }

    function bindInnLookup() {
        const btn = document.getElementById("innLookup");
        const innEl = view.querySelector("[data-b=inn]");
        if (btn) btn.onclick = (e) => { e.preventDefault(); lookupInn(true); };
        if (innEl) {
            innEl.addEventListener("blur", () => lookupInn(false));
            let innT = null;
            innEl.addEventListener("input", () => {
                clearTimeout(innT);
                innT = setTimeout(() => {
                    const inn = String(innEl.value || "").replace(/\D/g, "");
                    if (inn.length === 10 || inn.length === 12) lookupInn(false);
                }, 700);
            });
        }
    }

    async function lookupInn(force) {
        const b = state.draft.buyer;
        const inn = String(b.inn || "").replace(/\D/g, "");
        if (inn.length !== 10 && inn.length !== 12) return;
        if (!force && state.lastInnLookup === inn) return;
        const hint = document.getElementById("innHint");
        if (hint) hint.textContent = "Ищем реквизиты…";
        const done = armBusy(document.getElementById("innLookup"));
        try {
            const data = await api(`/tg/sale/api/lookup-inn?inn=${encodeURIComponent(inn)}`);
            const f = data.fields || {};
            const take = (k) => { if (!(b[k] || "").trim() && f[k]) b[k] = f[k]; };
            take("name"); take("kpp"); take("ogrn"); take("address"); take("phone");
            take("bank"); take("rs"); take("bik"); take("ks");
            if (f.inn) b.inn = f.inn;
            b._hint = data.hint || "";
            if (data.ok) state.lastInnLookup = inn;
            render();
        } catch (ex) {
            if (hint) hint.textContent = ex.message || "Не удалось запросить ЕГРЮЛ";
        } finally {
            done();
        }
    }

    function lineQty(it) {
        const ln = state.draft.lines.find((x) =>
            Number(x.plant_id) === Number(it.plant_id) && Number(x.size_id) === Number(it.size_id)
        );
        return ln ? Number(ln.qty || 0) : 0;
    }

    function paintSizeRows() {
        document.querySelectorAll(".size-row").forEach((btn) => {
            const g = state.stockGroups[Number(btn.dataset.g)];
            const it = g && g.sizes[Number(btn.dataset.s)];
            if (!it) return;
            const n = lineQty(it);
            btn.classList.toggle("on", n > 0);
            const mark = btn.querySelector(".addm");
            if (mark) mark.textContent = n ? String(n) : "+";
        });
    }

    function clampDisc(v) {
        let n = Number(v);
        if (!isFinite(n) || n < 0) n = 0;
        if (n > 100) n = 100;
        return n;
    }
    function calcLinePrice(ln, discOverride) {
        const wholesale = Number(ln.wholesale != null ? ln.wholesale : ln.price) || 0;
        const retail = Number(ln.retail != null ? ln.retail : ln.price) || 0;
        const base = state.priceMode === "wholesale" ? wholesale : retail;
        const disc = clampDisc(discOverride != null ? discOverride : (ln.discount_pct != null ? ln.discount_pct : state.discountPct));
        return Math.round(Math.max(0, base * (1 - disc / 100)) * 100) / 100;
    }
    function applyPricesToAllLines(setGlobalDisc) {
        const global = clampDisc(state.discountPct);
        state.draft.lines.forEach((ln) => {
            if (setGlobalDisc) ln.discount_pct = global;
            else if (ln.discount_pct == null) ln.discount_pct = global;
            ln.price = calcLinePrice(ln);
        });
        refreshLines();
    }
    function bindPriceBar() {
        const w = document.getElementById("modeWholesale");
        const r = document.getElementById("modeRetail");
        if (w) w.onclick = () => { state.priceMode = "wholesale"; applyPricesToAllLines(false); render(); };
        if (r) r.onclick = () => { state.priceMode = "retail"; applyPricesToAllLines(false); render(); };
        document.querySelectorAll(".disc-chip").forEach((btn) => {
            btn.onclick = () => {
                state.discountPct = clampDisc(btn.dataset.pct);
                const inp = document.getElementById("discCustom");
                if (inp) inp.value = String(state.discountPct);
                applyPricesToAllLines(true);
                render();
            };
        });
        const apply = document.getElementById("applyDisc");
        const custom = document.getElementById("discCustom");
        if (apply && custom) {
            apply.onclick = () => {
                state.discountPct = clampDisc(custom.value);
                applyPricesToAllLines(true);
                render();
            };
        }
    }

    function refreshLines() {
        const box = document.getElementById("linesBox");
        const tot = document.getElementById("totVal");
        if (!box) return;
        const d = state.draft;
        const html = d.lines.map((ln, i) => `
            <div class="list-item">
                <div><b>${esc(ln.plant_name)}</b> · ${esc(ln.size_name)}${ln.shop_attrs ? ` ${esc(ln.shop_attrs)}` : ""}</div>
                <div class="grid2" style="margin-top:8px">
                    <div>
                        <div class="label">Кол-во, шт</div>
                        <input class="input" data-qty="${i}" type="number" min="1" max="${ln.free_qty || 9999}" inputmode="numeric" value="${ln.qty}">
                        <div class="muted" style="margin-top:4px">свободно ${ln.free_qty || "—"}</div>
                    </div>
                    <div>
                        <div class="label">Цена, ₽</div>
                        <input class="input" data-price="${i}" type="number" min="0" step="1" inputmode="numeric" value="${ln.price}">
                    </div>
                </div>
                <div class="grid2" style="margin-top:8px">
                    <div>
                        <div class="label">Скидка %</div>
                        <input class="input" data-disc="${i}" type="number" min="0" max="100" step="0.1" inputmode="decimal" value="${ln.discount_pct != null ? ln.discount_pct : state.discountPct}">
                    </div>
                    <div class="muted" style="align-self:end;padding-bottom:8px">
                        прайс: опт ${money(ln.wholesale || 0)} / розн. ${money(ln.retail || 0)}
                    </div>
                </div>
                <div class="row" style="margin-top:8px">
                    <span></span>
                    <button class="btn sm danger" data-del="${i}">Удалить</button>
                </div>
            </div>`).join("");
        box.innerHTML = html || `<p class="muted">Нажмите размер в поиске, чтобы добавить</p>`;
        if (tot) {
            const sum = d.lines.reduce((s, ln) => s + Number(ln.qty) * Number(ln.price), 0);
            tot.textContent = money(sum);
        }
        box.querySelectorAll("[data-qty]").forEach((el) => {
            el.oninput = () => {
                d.lines[Number(el.dataset.qty)].qty = Number(el.value || 0);
                if (tot) {
                    const sum = d.lines.reduce((s, ln) => s + Number(ln.qty) * Number(ln.price), 0);
                    tot.textContent = money(sum);
                }
                paintSizeRows();
            };
        });
        box.querySelectorAll("[data-price]").forEach((el) => {
            el.oninput = () => {
                d.lines[Number(el.dataset.price)].price = Number(el.value || 0);
                if (tot) {
                    const sum = d.lines.reduce((s, ln) => s + Number(ln.qty) * Number(ln.price), 0);
                    tot.textContent = money(sum);
                }
            };
        });
        box.querySelectorAll("[data-disc]").forEach((el) => {
            el.oninput = () => {
                const ln = d.lines[Number(el.dataset.disc)];
                ln.discount_pct = clampDisc(el.value);
                ln.price = calcLinePrice(ln);
                const priceEl = box.querySelector(`[data-price="${el.dataset.disc}"]`);
                if (priceEl) priceEl.value = ln.price;
                if (tot) {
                    const sum = d.lines.reduce((s, x) => s + Number(x.qty) * Number(x.price), 0);
                    tot.textContent = money(sum);
                }
            };
        });
        box.querySelectorAll("[data-del]").forEach((el) => {
            el.onclick = () => {
                d.lines.splice(Number(el.dataset.del), 1);
                refreshLines();
                paintSizeRows();
            };
        });
    }

    function addStockLine(it) {
        const existing = state.draft.lines.find((ln) =>
            Number(ln.plant_id) === Number(it.plant_id) && Number(ln.size_id) === Number(it.size_id)
        );
        const wholesale = Number(it.wholesale != null ? it.wholesale : (it.wholesale_price != null ? it.wholesale_price : it.price)) || 0;
        const retail = Number(it.retail != null ? it.retail : (it.retail_price != null ? it.retail_price : it.price)) || 0;
        if (existing) {
            existing.qty = Number(existing.qty || 0) + 1;
            existing.wholesale = wholesale;
            existing.retail = retail;
            if (existing.discount_pct == null) existing.discount_pct = clampDisc(state.discountPct);
            existing.price = calcLinePrice(existing);
        } else {
            const ln = {
                plant_id: it.plant_id, plant_name: it.plant_name,
                size_id: it.size_id, size_name: it.size_name, shop_attrs: it.shop_attrs || "",
                qty: 1, wholesale, retail, discount_pct: clampDisc(state.discountPct),
                free_qty: it.free_qty || it.free || 0,
            };
            ln.price = calcLinePrice(ln);
            state.draft.lines.push(ln);
        }
        refreshLines();
        paintSizeRows();
    }

    async function searchStock(q) {
        const box = document.getElementById("suggest");
        if (!box) return;
        state.lastQ = q || "";
        if (!q || q.length < 2) { box.innerHTML = ""; state.stockGroups = []; return; }
        const data = await api(`/tg/sale/api/stock?q=${encodeURIComponent(q)}`);
        const groups = data.groups || [];
        state.stockGroups = groups;
        box.innerHTML = groups.map((g, gi) => {
            const initial = esc((g.plant_name || "?").charAt(0));
            const pic = g.photo_url
                ? `<img class="plant-pic" src="${esc(g.photo_url)}" alt="" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'"><div class="plant-pic ph" style="display:none">${initial}</div>`
                : `<div class="plant-pic ph">${initial}</div>`;
            const from = g.min_price ? `от ${money(g.min_price)}` : "без цены";
            const rows = (g.sizes || []).map((it, si) => `
                <button type="button" class="size-row ${it.is_seedling ? "seed" : ""}" data-g="${gi}" data-s="${si}">
                    <span class="sz">${esc(it.size_name)}${it.shop_attrs ? ` ${esc(it.shop_attrs)}` : ""}</span>
                    <span class="pr">${it.price ? money(it.price) : "—"}</span>
                    <span class="st">${it.free_qty || it.free} шт</span>
                    <span class="addm">+</span>
                </button>`).join("");
            return `<div class="plant-card">
                <div class="plant-head">${pic}<div>
                    <div class="hit-name">${esc(g.plant_name)}</div>
                    <div class="muted">${g.size_count} поз. · ${from}</div>
                </div></div>
                <div class="size-legend"><span>Размер</span><span>Цена</span><span>Остаток</span><span></span></div>
                ${rows}
            </div>`;
        }).join("") || `<div class="muted" style="padding:10px">Ничего не найдено</div>`;
        box.querySelectorAll("[data-g]").forEach((btn) => {
            btn.onclick = () => {
                const g = state.stockGroups[Number(btn.dataset.g)];
                const it = g && g.sizes[Number(btn.dataset.s)];
                if (it) addStockLine(it);
            };
        });
        paintSizeRows();
    }

    async function parseBuyer(e) {
        const file = e.target.files && e.target.files[0];
        if (!file) return;
        const err = document.getElementById("parseErr");
        err.classList.add("hide");
        const fd = new FormData();
        fd.append("file", file);
        try {
            const data = await api("/tg/sale/api/parse-buyer", { method: "POST", body: fd });
            Object.assign(state.draft.buyer, data.buyer || data.fields || {});
            render();
        } catch (ex) {
            err.textContent = ex.message;
            err.classList.remove("hide");
        }
    }

    function payloadFromDraft() {
        const b = state.draft.buyer;
        return {
            company_id: state.draft.company_id,
            order_id: state.draft.order_id || null,
            client_id: state.draft.client_id || null,
            anonymous: !!state.draft.anonymous,
            buyer_name: b.name,
            buyer_inn: b.inn,
            buyer_kpp: b.kpp,
            buyer_ogrn: b.ogrn,
            buyer_address: b.address,
            buyer_phone: b.phone,
            buyer_bank: b.bank,
            buyer_rs: b.rs,
            buyer_bik: b.bik,
            buyer_ks: b.ks,
            lines: state.draft.lines.map((ln) => ({
                plant_id: ln.plant_id, size_id: ln.size_id, qty: ln.qty, price: ln.price,
            })),
        };
    }

    function showSaveErr(msg) {
        const el = document.getElementById("saveErr");
        if (!el) { alert(msg); return; }
        el.textContent = msg;
        el.classList.remove("hide");
    }

    async function saveDraft() {
        const done = armBusy(document.getElementById("save"));
        try {
            const body = payloadFromDraft();
            let saved;
            if (state.current) {
                saved = await api(`/tg/sale/api/invoices/${state.current.id}`, { method: "POST", body: JSON.stringify(body) });
            } else {
                saved = await api("/tg/sale/api/invoices", { method: "POST", body: JSON.stringify(body) });
            }
            state.current = saved;
            await reload();
            render();
            notifySaved(saved);
        } catch (ex) {
            showSaveErr(ex.message);
        } finally {
            done();
        }
    }

    async function sendPdf() {
        if (!state.current) return;
        const done = armBusy(document.getElementById("pdf"));
        try {
            const data = await api(`/tg/sale/api/invoices/${state.current.id}/send-pdf`, { method: "POST", body: "{}" });
            if (!data.ok) {
                const err = data.error || "";
                alert(err === "no_telegram_id"
                    ? "Не вижу ваш Telegram. Закройте мини-приложение и откройте его кнопкой в чате с ботом."
                    : "Не удалось отправить счёт в чат.");
                return;
            }
            haptic("medium");
            const tg = tgApp();
            if (tg && typeof tg.showAlert === "function") {
                try {
                    tg.showAlert("Счёт отправил в чат с ботом.", () => { try { tg.close && tg.close(); } catch (_) {} });
                    return;
                } catch (_) {}
            }
            if (tg && tg.close) setTimeout(() => tg.close(), 400);
        } catch (e) {
            alert(e.message || "Не удалось отправить счёт в чат.");
        } finally {
            done();
        }
    }

    async function approveInv() {
        if (!state.current) return;
        const done = armBusy(document.getElementById("approve"));
        try {
            await api(`/tg/sale/api/invoices/${state.current.id}/approve`, { method: "POST", body: "{}" });
            haptic("medium");
            await reload();
            state.screen = "list";
            render();
        } catch (ex) {
            showSaveErr(ex.message);
        } finally {
            done();
        }
    }

    async function discardInv() {
        if (!state.current) return;
        const approved = state.current.status === "approved";
        const fromOrder = state.current.from_existing_order || (state.draft && state.draft.order_id);
        const msg = approved
            ? (fromOrder
                ? "Удалить согласованный счёт? Заказ в ERP останется."
                : "Удалить согласованный счёт и заказ в ERP? Резерв снимется.")
            : (fromOrder ? "Удалить черновик счёта? Заказ в ERP останется." : "Удалить черновик?");
        if (!confirm(msg)) return;
        try {
            await api(`/tg/sale/api/invoices/${state.current.id}/discard`, { method: "POST", body: "{}" });
        } catch (e) {
            alert(e.message || "Не удалось удалить");
            return;
        }
        state.current = null;
        await reload();
        state.screen = "list";
        render();
    }

    async function openInvoice(id) {
        const inv = (state.invoices || []).find((x) => x.id === id);
        if (!inv) return;
        const full = await api(`/tg/sale/api/invoices/${id}`);
        state.current = full;
        if (full.status === "approved") { state.screen = "view"; render(); return; }
        state.draft = {
            company_id: full.company_id,
            client_id: full.client_id || (full.order && full.order.client_id) || null,
            buyer: {
                name: full.buyer_name || "", inn: full.buyer_inn || "", kpp: full.buyer_kpp || "",
                ogrn: full.buyer_ogrn || "", address: full.buyer_address || "", phone: full.buyer_phone || "",
                bank: full.buyer_bank || "", rs: full.buyer_rs || "",
                bik: full.buyer_bik || "", ks: full.buyer_ks || "",
            },
            lines: (full.lines || []).map((ln) => Object.assign({
                free_qty: ln.free_qty || 0,
                wholesale: ln.wholesale != null ? ln.wholesale : ln.price,
                retail: ln.retail != null ? ln.retail : ln.price,
                discount_pct: ln.discount_pct != null ? ln.discount_pct : 0,
            }, ln)),
            order_id: full.order_id || null,
            order: full.order || null,
            anonymous: !!full.anonymous,
        };
        state.screen = "edit";
        render();
    }

    function esc(s) {
        return String(s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
    }

    async function loadBuh() {
        const data = await api("/tg/sale/api/buh/invoices");
        state.buhInvoices = data.invoices || [];
    }

    function kindLabel(kind) {
        if (kind === "advance") return "произвольный";
        if (kind === "balance") return "остаток";
        return "позиции";
    }

    function renderBuhList() {
        setTitle("Отгрузки");
        const rows = (state.buhInvoices || []).map((inv) => {
            const lines = (inv.lines || []).map((ln) =>
                `<li>${esc(ln.plant_name || "—")}${ln.size_name ? ` · ${esc(ln.size_name)}` : ""} ×${ln.qty}`
                + (ln.shipped_qty ? ` · отгр. ${ln.shipped_qty}` : "") + `</li>`
            ).join("");
            return `
            <div class="list-item" data-buh="${inv.id}">
                <div class="row">
                    <div>
                        <div><b>Счёт №${esc(inv.number)}</b> · ${esc(inv.buyer_name || "—")}</div>
                        <div class="muted">${esc(inv.company_name || "")} · заказ №${inv.order_id || "—"} · ${esc(kindLabel(inv.kind))}</div>
                    </div>
                    <div style="text-align:right">
                        <div style="font-weight:700">${money(inv.amount)}</div>
                        <div class="muted">заказ ${money(inv.order_sum)}</div>
                    </div>
                </div>
                ${lines ? `<ul class="order-lines">${lines}</ul>` : ""}
                ${inv.more_count ? `<p class="muted">ещё ${inv.more_count} поз.</p>` : ""}
            </div>`;
        }).join("") || `<p class="muted">Пока нет отгруженных заказов со счетами</p>`;
        view.innerHTML = `<p class="muted" style="margin-top:0">Только счета, привязанные к заказам с отгрузкой. Сумма счёта не меняет сумму заказа.</p><div class="card">${rows}</div>`;
        view.querySelectorAll("[data-buh]").forEach((el) => {
            el.onclick = () => openBuh(Number(el.dataset.buh));
        });
    }

    function shipDates(list) {
        const rows = list || [];
        if (!rows.length) return "—";
        return rows.map((s) => `${esc(s.date)} · ${s.qty} шт`).join("<br>");
    }

    function renderBuhView() {
        const d = state.buhCurrent;
        if (!d) { state.screen = "buh"; render(); return; }
        setTitle(`Счёт №${d.number}`);
        const invRows = (d.invoice_lines || []).map((ln) => `
            <tr>
                <td>${esc(ln.name)}</td>
                <td>${esc(ln.qty)} ${esc(ln.unit || "")}</td>
                <td>${money(ln.price)}</td>
                <td>${money(ln.sum)}</td>
            </tr>`).join("") || `<tr><td colspan="4" class="muted">В счёте нет строк</td></tr>`;
        const orderRows = (d.order_lines || []).map((ln) => `
            <tr>
                <td>${esc(ln.plant_name || "—")}${ln.size_name ? `<div class="muted">${esc(ln.size_name)}</div>` : ""}</td>
                <td>${ln.qty}</td>
                <td>${money(ln.price)}</td>
                <td>${money(ln.sum)}</td>
                <td>${ln.shipped_qty || 0}</td>
                <td>${shipDates(ln.shipments)}</td>
            </tr>`).join("") || `<tr><td colspan="6" class="muted">Нет позиций заказа</td></tr>`;
        const journal = (d.shipments || []).map((doc) => `
            <div class="buh-ship">
                <b>${esc(doc.date)}</b> · ${doc.qty} шт
                <ul class="order-lines">${(doc.rows || []).map((r) =>
                    `<li>${esc(r.plant_name || "—")}${r.size_name ? ` · ${esc(r.size_name)}` : ""} ×${r.qty}</li>`
                ).join("")}</ul>
            </div>`).join("") || `<p class="muted">Документов отгрузки нет</p>`;
        view.innerHTML = `
            <button class="btn ghost" id="back" style="margin-bottom:10px">← К списку</button>
            <div class="card">
                <div><b>${esc(d.buyer_name || "—")}</b></div>
                <div class="muted">${esc(d.company_name || "")} · заказ №${d.order_id || "—"} · ${esc(kindLabel(d.kind))}</div>
                <div style="margin-top:8px">Счёт: <b>${money(d.amount)}</b> · заказ: <b>${money(d.order_sum)}</b></div>
            </div>
            <h3 class="buh-h">Позиции счёта</h3>
            <div class="card" style="overflow:auto">
                <table class="buh-table">
                    <thead><tr><th>Наименование</th><th>Кол-во</th><th>Цена</th><th>Сумма</th></tr></thead>
                    <tbody>${invRows}</tbody>
                </table>
            </div>
            <h3 class="buh-h">Заказ и даты отгрузки</h3>
            <div class="card" style="overflow:auto">
                <table class="buh-table">
                    <thead><tr><th>Позиция</th><th>Заказ</th><th>Цена</th><th>Сумма</th><th>Отгр.</th><th>Даты</th></tr></thead>
                    <tbody>${orderRows}</tbody>
                </table>
            </div>
            <h3 class="buh-h">Журнал отгрузок</h3>
            <div class="card">${journal}</div>`;
        document.getElementById("back").onclick = () => { state.screen = "buh"; render(); };
    }

    async function openBuh(id) {
        const data = await api(`/tg/sale/api/buh/invoices/${id}`);
        state.buhCurrent = data;
        state.screen = "buh-view";
        render();
    }

    async function reload() {
        const [cos, invs] = await Promise.all([
            api("/tg/sale/api/companies"),
            api("/tg/sale/api/invoices"),
        ]);
        state.companies = cos.companies || cos.items || [];
        state.allCompanies = cos.all || [];
        state.invoices = invs.invoices || invs.items || [];
        if (!state.draft.company_id && state.companies[0]) state.draft.company_id = state.companies[0].id;
    }

    async function boot() {
        try {
            if (window.FFTg && window.FFTg.bootAuth) {
                state.me = await window.FFTg.bootAuth("/tg/sale/api/auth");
            } else {
                await waitTelegram();
                state.me = await api("/tg/sale/api/me");
            }
        } catch (e) {
            if (!/unauthorized|Нет входа|не передал|Подпись/i.test(String(e.message || ""))) throw e;
            await waitTelegram();
            if (window.FFTg) state.me = await window.FFTg.handshake("/tg/sale/api/auth");
        }
        if (state.me && state.me.accountant_only) {
            state.screen = "buh";
            await loadBuh();
            render();
            return;
        }
        await reload();
        render();
    }
    boot().catch((e) => {
        view.innerHTML = `<div class="card err">${esc(e.message)}</div>`;
    });
})();
