(function () {
    function tgApp() {
        return window.Telegram && window.Telegram.WebApp;
    }
    function getInitData() {
        const w = tgApp();
        if (w && w.initData) return w.initData;
        const hash = String(location.hash || "");
        const m = hash.match(/tgWebAppData=([^&]+)/);
        if (m) {
            try { return decodeURIComponent(m[1]); } catch (_) { return m[1]; }
        }
        return "";
    }
    function waitTelegram() {
        return new Promise((resolve) => {
            const start = Date.now();
            const max = 4000;
            const tick = () => {
                const w = tgApp();
                if (w) {
                    try { w.ready(); w.expand(); } catch (_) {}
                    if (w.initData || Date.now() - start > max) return resolve(w);
                } else if (Date.now() - start > max) {
                    return resolve(null);
                }
                setTimeout(tick, 50);
            };
            tick();
        });
    }
    const view = document.getElementById("view");
    const titleEl = document.getElementById("screenTitle");
    const state = { me: null, companies: [], allCompanies: [], invoices: [], screen: "list", draft: emptyDraft(), current: null, stockGroups: [], lastQ: "", lastInnLookup: "" };

    function haptic(kind) {
        try { const tg = tgApp(); tg && tg.HapticFeedback && tg.HapticFeedback.impactOccurred(kind || "light"); } catch (_) {}
    }

    function notifySaved(inv) {
        const msg = inv && inv.id ? `Счёт №${inv.id} сохранён` : "Счёт сохранён";
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
        const sel = "button, .btn, .list-item[data-open], .firm, .size-row";
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
            buyer: { name: "", inn: "", kpp: "", ogrn: "", address: "", phone: "", bank: "", rs: "", bik: "", ks: "" },
            lines: [],
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
        const initData = getInitData();
        const headers = Object.assign({}, (opts && opts.headers) || {});
        if (initData) {
            headers["X-Telegram-Init-Data"] = initData;
            headers.Authorization = "tma " + initData;
        }
        if (opts && opts.body && !(opts.body instanceof FormData)) headers["Content-Type"] = "application/json";
        const url = initData
            ? path + (path.includes("?") ? "&" : "?") + "initData=" + encodeURIComponent(initData)
            : path;
        const res = await fetch(url, Object.assign({}, opts, { headers }));
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || data.hint || "Ошибка");
        return data;
    }

    function setTitle(t) { titleEl.textContent = t; }

    function render() {
        if (state.screen === "list") renderList();
        else if (state.screen === "edit") renderEdit();
        else if (state.screen === "firms") renderFirms();
        else if (state.screen === "view") renderView();
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
                        <div><b>№${inv.id}</b> · ${esc(inv.buyer_name || "Без клиента")}</div>
                        <div class="muted">${esc(companyName(inv))} · ${fmtDate(inv.created_at)}</div>
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
                ${state.me && (state.me.can_firms || state.me.can_edit_firms) ? `<button class="btn ghost" id="btnFirms">Фирмы</button>` : `<span></span>`}
            </div>
            <div class="card">${rows}</div>`;
        document.getElementById("btnNew").onclick = () => {
            state.draft = emptyDraft();
            if (state.companies[0]) state.draft.company_id = state.companies[0].id;
            state.current = null;
            state.screen = "edit";
            render();
        };
        const bf = document.getElementById("btnFirms");
        if (bf) bf.onclick = () => { state.screen = "firms"; render(); };
        view.querySelectorAll("[data-open]").forEach((el) => {
            el.onclick = () => openInvoice(Number(el.dataset.open));
        });
    }

    function buyerFields(b) {
        const f = (k, l) => `<div class="label">${l}</div><input class="input" data-b="${k}" value="${esc(b[k] || "")}">`;
        return f("name", "Название")
            + `<div class="label">ИНН</div>
               <div class="inn-row">
                 <input class="input" data-b="inn" inputmode="numeric" value="${esc(b.inn || "")}" placeholder="10 или 12 цифр">
                 <button type="button" class="btn sm ghost" id="innLookup">По ИНН</button>
               </div>
               <div class="muted" id="innHint">${esc(b._hint || "Подставим название, КПП, ОГРН и адрес из ЕГРЮЛ")}</div>`
            + f("kpp", "КПП") + f("ogrn", "ОГРН") + f("address", "Адрес") + f("phone", "Телефон")
            + f("bank", "Банк") + f("rs", "Расчётный счёт") + f("bik", "БИК") + f("ks", "Корр. счёт");
    }

    function renderEdit() {
        setTitle(state.current ? `Счёт №${state.current.id}` : "Новый счёт");
        const d = state.draft;
        const firms = state.companies.map((c) => `
            <button type="button" class="card firm ${Number(d.company_id) === Number(c.id) ? "on" : ""}" data-co="${c.id}">
                <div><b>${esc(c.short_name)}</b></div>
                <div class="muted">${vatLabel(c.vat_mode)}</div>
            </button>`).join("") || `<p class="muted">Сначала заполните фирмы (админ)</p>`;
        view.innerHTML = `
            <button class="btn ghost" id="back">← К списку</button>
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
                <input class="input" id="q" placeholder="Название или размер, например туя 160" value="${esc(state.lastQ || "")}">
                <div id="suggest" class="suggest"></div>
                <div id="linesBox"></div>
            </div>
            <div class="card row"><span class="muted">Итого</span><span class="tot" id="totVal">0 ₽</span></div>
            <div id="saveErr" class="err hide"></div>
            <button class="btn gold" id="save">Сохранить счёт</button>
            ${state.current ? `<div class="grid2" style="margin-top:8px">
                <button class="btn" id="pdf">Открыть счёт</button>
                <button class="btn ghost" id="approve">Согласовать</button>
            </div>
            <button class="btn danger" id="discard" style="margin-top:8px">Удалить</button>` : ""}`;
        document.getElementById("back").onclick = () => { state.screen = "list"; render(); };
        view.querySelectorAll("[data-co]").forEach((el) => {
            el.onclick = () => { d.company_id = Number(el.dataset.co); render(); };
        });
        view.querySelectorAll("[data-b]").forEach((el) => {
            el.oninput = () => { d.buyer[el.dataset.b] = el.value; };
        });
        bindInnLookup();
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
        setTitle(`Счёт №${inv.id}`);
        view.innerHTML = `
            <button class="btn ghost" id="back">← К списку</button>
            <div class="card">
                <span class="chip ok">согласован</span>
                <h2 style="margin:10px 0 4px">${esc(inv.buyer_name)}</h2>
                <p class="muted">${esc(companyName(inv))}</p>
                <div class="tot" style="margin-top:10px">${money(inv.amount)}</div>
            </div>
            <button class="btn gold" id="pdf">Открыть счёт</button>`;
        document.getElementById("back").onclick = () => { state.screen = "list"; render(); };
        document.getElementById("pdf").onclick = sendPdf;
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
                <button class="btn" data-save-co="${c.id}" style="margin-top:10px">Сохранить</button>
            </div>`).join("");
        document.getElementById("back").onclick = () => { state.screen = "list"; render(); };
        view.querySelectorAll("[data-save-co]").forEach((btn) => {
            btn.onclick = () => saveCompany(Number(btn.dataset.saveCo), btn.closest(".card"));
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

    function refreshLines() {
        const box = document.getElementById("linesBox");
        const tot = document.getElementById("totVal");
        if (!box) return;
        const d = state.draft;
        const html = d.lines.map((ln, i) => `
            <div class="list-item">
                <div><b>${esc(ln.plant_name)}</b> · ${esc(ln.size_name)}</div>
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
        if (existing) existing.qty = Number(existing.qty || 0) + 1;
        else {
            state.draft.lines.push({
                plant_id: it.plant_id, size_id: it.size_id, plant_name: it.plant_name,
                size_name: it.size_name, qty: 1, price: it.price, free_qty: it.free_qty || it.free || 0,
            });
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
                    <span class="sz">${esc(it.size_name)}</span>
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
            haptic("medium");
            if (!data.ok) {
                window.open(`/tg/sale/api/invoices/${state.current.id}/pdf`, "_blank");
                return;
            }
            if (tgApp() && tgApp().close) setTimeout(() => tgApp().close(), 400);
        } catch (e) {
            window.open(`/tg/sale/api/invoices/${state.current.id}/pdf`, "_blank");
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
        if (!state.current || !confirm("Удалить черновик?")) return;
        await api(`/tg/sale/api/invoices/${state.current.id}/discard`, { method: "POST", body: "{}" });
        state.current = null;
        await reload();
        state.screen = "list";
        render();
    }

    async function openInvoice(id) {
        const inv = (state.invoices || []).find((x) => x.id === id);
        if (!inv) return;
        state.current = inv;
        if (inv.status === "approved") { state.screen = "view"; render(); return; }
        const full = await api(`/tg/sale/api/invoices/${id}`);
        state.current = full;
        state.draft = {
            company_id: full.company_id,
            buyer: {
                name: full.buyer_name || "", inn: full.buyer_inn || "", kpp: full.buyer_kpp || "",
                ogrn: full.buyer_ogrn || "", address: full.buyer_address || "", phone: full.buyer_phone || "",
                bank: full.buyer_bank || "", rs: full.buyer_rs || "",
                bik: full.buyer_bik || "", ks: full.buyer_ks || "",
            },
            lines: (full.lines || []).map((ln) => Object.assign({ free_qty: ln.free_qty || 0 }, ln)),
        };
        state.screen = "edit";
        render();
    }

    function esc(s) {
        return String(s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
    }

    async function reload() {
        const [me, cos, invs] = await Promise.all([
            api("/tg/sale/api/me"),
            api("/tg/sale/api/companies"),
            api("/tg/sale/api/invoices"),
        ]);
        state.me = me;
        state.companies = cos.companies || cos.items || [];
        state.allCompanies = cos.all || [];
        state.invoices = invs.invoices || invs.items || [];
        if (!state.draft.company_id && state.companies[0]) state.draft.company_id = state.companies[0].id;
    }

    async function boot() {
        await waitTelegram();
        try {
            await reload();
        } catch (e) {
            if (!/unauthorized|Нет входа/i.test(String(e.message || ""))) throw e;
            await new Promise((r) => setTimeout(r, 700));
            await waitTelegram();
            await reload();
        }
        render();
    }
    boot().catch((e) => {
        view.innerHTML = `<div class="card err">${esc(e.message)}</div>`;
    });
})();
