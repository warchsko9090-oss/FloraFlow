(function () {
    const tg = window.Telegram && window.Telegram.WebApp;
    if (tg) { tg.ready(); tg.expand(); }
    const initData = (tg && tg.initData) || "";
    const view = document.getElementById("view");
    const titleEl = document.getElementById("screenTitle");
    const state = { me: null, companies: [], invoices: [], screen: "list", draft: emptyDraft(), current: null, stockHits: [] };

    function emptyDraft() {
        return {
            company_id: null,
            buyer: { name: "", inn: "", kpp: "", address: "", bank: "", rs: "", bik: "", ks: "" },
            lines: [],
        };
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
        const headers = Object.assign({ "X-Telegram-Init-Data": initData }, (opts && opts.headers) || {});
        if (opts && opts.body && !(opts.body instanceof FormData)) headers["Content-Type"] = "application/json";
        const res = await fetch(path, Object.assign({}, opts, { headers }));
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
        return f("name", "Название") + f("inn", "ИНН") + f("kpp", "КПП") + f("address", "Адрес")
            + f("bank", "Банк") + f("rs", "Расчётный счёт") + f("bik", "БИК") + f("ks", "Корр. счёт");
    }

    function renderEdit() {
        setTitle(state.current ? `Счёт №${state.current.id}` : "Новый счёт");
        const d = state.draft;
        const firms = state.companies.map((c) => `
            <button type="button" class="card firm ${Number(d.company_id) === Number(c.id) ? "on" : ""}" data-co="${c.id}">
                <div><b>${esc(c.short_name)}</b></div>
                <div class="muted">${c.vat_mode === "included_20" ? "с НДС 20%" : "без НДС"}</div>
            </button>`).join("") || `<p class="muted">Сначала заполните фирмы (админ)</p>`;
        const lines = d.lines.map((ln, i) => `
            <div class="list-item">
                <div><b>${esc(ln.plant_name)}</b> · ${esc(ln.size_name)}</div>
                <div class="grid2" style="margin-top:8px">
                    <input class="input" data-qty="${i}" type="number" min="1" max="${ln.free_qty || 9999}" value="${ln.qty}">
                    <input class="input" data-price="${i}" type="number" min="0" step="1" value="${ln.price}">
                </div>
                <div class="row" style="margin-top:6px">
                    <span class="muted">свободно ${ln.free_qty || "—"}</span>
                    <button class="btn sm danger" data-del="${i}">убрать</button>
                </div>
            </div>`).join("");
        const sum = d.lines.reduce((s, ln) => s + Number(ln.qty) * Number(ln.price), 0);
        view.innerHTML = `
            <button class="btn ghost" id="back">← К списку</button>
            <div class="label">Клиент</div>
            <div class="card">
                <input type="file" id="buyerFile" accept=".pdf,.doc,.docx,image/*">
                <p class="muted" style="margin-top:8px">PDF, Word или фото реквизитов</p>
                <div id="parseErr" class="err hide"></div>
                ${buyerFields(d.buyer)}
            </div>
            <div class="label">Фирма</div>
            ${firms}
            <div class="label">Позиции</div>
            <div class="card">
                <input class="input" id="q" placeholder="Поиск по остаткам">
                <div id="suggest" class="suggest"></div>
                ${lines || `<p class="muted">Добавьте растение и размер</p>`}
            </div>
            <div class="card row"><span class="muted">Итого</span><span class="tot">${money(sum)}</span></div>
            <div id="saveErr" class="err hide"></div>
            <button class="btn gold" id="save">Сохранить черновик</button>
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
        view.querySelectorAll("[data-qty]").forEach((el) => {
            el.oninput = () => { d.lines[Number(el.dataset.qty)].qty = Number(el.value || 0); };
        });
        view.querySelectorAll("[data-price]").forEach((el) => {
            el.oninput = () => { d.lines[Number(el.dataset.price)].price = Number(el.value || 0); };
        });
        view.querySelectorAll("[data-del]").forEach((el) => {
            el.onclick = () => { d.lines.splice(Number(el.dataset.del), 1); render(); };
        });
        const q = document.getElementById("q");
        let t = null;
        q.oninput = () => {
            clearTimeout(t);
            t = setTimeout(() => searchStock(q.value), 220);
        };
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
        view.innerHTML = `<button class="btn ghost" id="back">← Назад</button>` + state.companies.map((c) => `
            <div class="card">
                <div class="chip gold">${c.vat_mode === "included_20" ? "с НДС 20%" : "без НДС"}</div>
                ${field("short_name", "Короткое имя", c)}
                ${field("legal_name", "Юридическое имя", c)}
                ${field("inn", "ИНН", c)}
                ${field("kpp", "КПП", c)}
                ${field("ogrn", "ОГРН", c)}
                ${field("legal_address", "Юр. адрес", c)}
                ${field("bank_name", "Банк", c)}
                ${field("bik", "БИК", c)}
                ${field("rs", "р/с", c)}
                ${field("ks", "к/с", c)}
                ${field("director", "Подпись", c)}
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
        const payload = {};
        card.querySelectorAll("[data-k]").forEach((el) => { payload[el.dataset.k] = el.value; });
        await api(`/tg/sale/api/companies/${id}`, { method: "POST", body: JSON.stringify(payload) });
        await reload();
        state.screen = "firms";
        render();
    }

    async function searchStock(q) {
        const box = document.getElementById("suggest");
        if (!box) return;
        if (!q || q.length < 2) { box.innerHTML = ""; state.stockHits = []; return; }
        const data = await api(`/tg/sale/api/stock?q=${encodeURIComponent(q)}`);
        state.stockHits = data.items || [];
        box.innerHTML = state.stockHits.map((it, i) =>
            `<button type="button" data-add="${i}">${esc(it.plant_name)} · ${esc(it.size_name)} · ${it.free_qty || it.free} шт · ${money(it.price)}</button>`
        ).join("") || `<div class="muted" style="padding:10px">Ничего не найдено</div>`;
        box.querySelectorAll("[data-add]").forEach((btn) => {
            btn.onclick = () => {
                const it = state.stockHits[Number(btn.dataset.add)];
                if (!it) return;
                const free = it.free_qty || it.free || 0;
                state.draft.lines.push({
                    plant_id: it.plant_id, size_id: it.size_id, plant_name: it.plant_name,
                    size_name: it.size_name, qty: 1, price: it.price, free_qty: free,
                });
                render();
            };
        });
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
            buyer_address: b.address,
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
        } catch (ex) {
            showSaveErr(ex.message);
        }
    }

    async function sendPdf() {
        if (!state.current) return;
        try {
            const data = await api(`/tg/sale/api/invoices/${state.current.id}/send-pdf`, { method: "POST", body: "{}" });
            if (!data.ok) {
                window.open(`/tg/sale/api/invoices/${state.current.id}/pdf`, "_blank");
                return;
            }
            if (tg && tg.close) setTimeout(() => tg.close(), 400);
        } catch (e) {
            window.open(`/tg/sale/api/invoices/${state.current.id}/pdf`, "_blank");
        }
    }

    async function approveInv() {
        if (!state.current) return;
        try {
            await api(`/tg/sale/api/invoices/${state.current.id}/approve`, { method: "POST", body: "{}" });
            await reload();
            state.screen = "list";
            render();
        } catch (ex) {
            showSaveErr(ex.message);
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
                address: full.buyer_address || "", bank: full.buyer_bank || "", rs: full.buyer_rs || "",
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
        state.invoices = invs.invoices || invs.items || [];
        if (!state.draft.company_id && state.companies[0]) state.draft.company_id = state.companies[0].id;
    }

    reload().then(render).catch((e) => {
        view.innerHTML = `<div class="card err">${esc(e.message)}</div>`;
    });
})();
