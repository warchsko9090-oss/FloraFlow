(function () {
    const CART_KEY = 'knyazhestvo_cart';
    const METRICA_ID = 110500921;
    let catalog = [];
    let cart = {};

    const formatPrice = n => Math.round(Number(n) || 0).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
    const esc = s => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
    const itemFreeQty = item => Math.max(0, item?.free_qty ?? item?.stock ?? 0);
    const isOnRequest = item => Boolean(item?.on_request);

    function trackGoal(goalId) {
        if (typeof window.ym === 'function' && goalId) {
            window.ym(METRICA_ID, 'reachGoal', goalId);
        }
    }

    function isPinePlant(name) {
        return String(name || '').toLowerCase().includes('сосн');
    }

    function parseSizeSpec(spec) {
        const raw = String(spec || '').trim();
        if (!raw) return { kind: 'other', label: '—' };
        const dual = raw.match(/^([\d][\d\-]*)\s*[*×xX]\s*([\d][\d\-]*)$/);
        if (dual) return { kind: 'dual', a: dual[1], b: dual[2] };
        const single = raw.match(/^([\d][\d\-]*)$/);
        if (single) return { kind: 'single', a: single[1] };
        return { kind: 'other', label: raw };
    }

    function sizeDimensions(item) {
        const parsed = parseSizeSpec(item.spec);
        const pine = isPinePlant(item.name);
        if (parsed.kind === 'dual') {
            if (pine) return { width: parsed.a, height: parsed.b, mode: 'dual' };
            return { label: item.spec, mode: 'label' };
        }
        if (parsed.kind === 'single') {
            if (pine) return { width: parsed.a, mode: 'width' };
            return { height: parsed.a, mode: 'height' };
        }
        return { label: parsed.label || item.spec, mode: 'label' };
    }

    function sizeSortValue(item) {
        const d = sizeDimensions(item);
        const num = s => {
            const m = String(s || '').match(/(\d+)/);
            return m ? parseInt(m[1], 10) : 0;
        };
        if (d.mode === 'dual') return num(d.width) * 100000 + num(d.height);
        if (d.mode === 'width') return num(d.width);
        if (d.mode === 'height') return num(d.height);
        return 999999;
    }

    function formatSizeLabel(item) {
        if (item?.is_seedling) return 'Саженец';
        const d = sizeDimensions(item);
        if (d.mode === 'dual') return `Ш ${d.width} × В ${d.height} см`;
        if (d.mode === 'width') return `Ш ${d.width} см`;
        if (d.mode === 'height') return `В ${d.height} см`;
        return d.label;
    }

    function resolveTableMode(variants, plantName) {
        const pine = isPinePlant(plantName);
        const hasDual = variants.some(v => sizeDimensions(v).mode === 'dual');
        const hasHeight = variants.some(v => sizeDimensions(v).mode === 'height');
        if (pine && hasDual) return 'dual';
        if (pine) return 'width';
        if (hasHeight) return 'height';
        return 'label';
    }

    function stockLabel(itemOrQty) {
        if (typeof itemOrQty === 'object' && itemOrQty !== null) {
            if (isOnRequest(itemOrQty)) {
                return '<span class="on-request-badge">По запросу</span>';
            }
            return stockLabel(itemFreeQty(itemOrQty));
        }
        const n = itemOrQty;
        return n < 20
            ? `<span class="text-brand-low text-xs font-medium">${n} шт</span>`
            : `<span class="text-brand-muted text-xs">${n} шт</span>`;
    }

    function loadCart() {
        try {
            cart = JSON.parse(localStorage.getItem(CART_KEY) || '{}');
        } catch {
            cart = {};
        }
    }

    function saveCart() {
        localStorage.setItem(CART_KEY, JSON.stringify(cart));
    }

    function getItem(id) {
        return catalog.find(i => i.id === id);
    }

    function syncCartWithCatalog() {
        let changed = false;
        Object.keys(cart).forEach(id => {
            const item = getItem(id);
            const qty = parseInt(cart[id], 10) || 0;
            if (!item || qty <= 0 || isOnRequest(item)) {
                delete cart[id];
                changed = true;
                return;
            }
            const capped = Math.min(qty, itemFreeQty(item));
            if (capped !== qty) {
                cart[id] = capped;
                changed = true;
            }
            if (capped <= 0) {
                delete cart[id];
                changed = true;
            }
        });
        if (changed) saveCart();
    }

    function renderControls(item) {
        if (isOnRequest(item)) {
            return `<button type="button" data-on-request-open="${esc(item.id)}" class="on-request-btn w-full h-full min-h-[2.25rem] px-2 rounded-sm text-[10px] font-semibold tracking-wide uppercase transition-colors" title="Оставить заявку">По запросу</button>`;
        }
        const qty = cart[item.id] || 0;
        const max = itemFreeQty(item);
        const borderClass = qty > 0 ? 'border-brand-brass/60' : 'border-brand-border';
        return `<div class="w-full h-full flex items-center border ${borderClass} rounded-sm overflow-hidden">
            <button type="button" data-cart-delta="${esc(item.id)}" data-delta="-1" class="w-7 h-full hover:bg-brand-bgSoft text-brand-brass"><i class="fa-solid fa-minus text-[9px]"></i></button>
            <input type="number" value="${qty}" min="0" max="${max}" data-cart-input="${esc(item.id)}" class="w-7 text-center text-xs font-semibold border-none focus:outline-none bg-transparent text-brand-cream tabular-nums" aria-label="Количество">
            <button type="button" data-cart-delta="${esc(item.id)}" data-delta="1" class="w-7 h-full hover:bg-brand-bgSoft text-brand-brass"><i class="fa-solid fa-plus text-[9px]"></i></button>
        </div>`;
    }

    function updateUI(options = {}) {
        const scope = options.scope ? document.querySelector(options.scope) : document;
        catalog.forEach(item => {
            scope.querySelectorAll(`[id="controls-${CSS.escape(item.id)}"]`).forEach(el => {
                el.innerHTML = renderControls(item);
            });
        });

        let total = 0;
        let count = 0;
        let plantTotal = 0;
        let plantCount = 0;
        const plantId = options.plantId;
        const isSeedlingFilter = options.isSeedling;

        for (const [id, qty] of Object.entries(cart)) {
            if (qty <= 0) continue;
            const item = getItem(id);
            if (!item) continue;
            count += qty;
            total += item.price * qty;
            if (plantId != null && item.plant_id === plantId) {
                if (isSeedlingFilter == null || Boolean(item.is_seedling) === Boolean(isSeedlingFilter)) {
                    plantCount += qty;
                    plantTotal += item.price * qty;
                }
            }
        }

        const totalEl = document.getElementById('cart-total');
        if (totalEl) totalEl.textContent = formatPrice(total) + ' ₽';

        const plantSummary = document.getElementById('plant-cart-summary');
        if (plantSummary) {
            plantSummary.textContent = `${plantCount} ${plantCount === 1 ? 'позиция' : 'поз.'} · ${formatPrice(plantTotal)} ₽`;
        }

        const cartOpen = count > 0;
        document.getElementById('cart-panel')?.classList.toggle('translate-y-full', !cartOpen);
        document.getElementById('scroll-top-btn')?.classList.toggle('cart-visible', cartOpen);
        document.body.classList.toggle('shop-cart-open', cartOpen);

        if (typeof options.onUpdate === 'function') options.onUpdate({ total, count, plantTotal, plantCount });

        if (document.getElementById('checkout-modal') && !document.getElementById('checkout-modal').classList.contains('hidden')) {
            const reviewStep = document.getElementById('checkout-step-review');
            if (reviewStep && !reviewStep.classList.contains('hidden')) {
                renderCheckoutReview();
            }
        }
    }

    function getCartEntries() {
        return Object.entries(cart)
            .filter(([, qty]) => qty > 0)
            .map(([id, qty]) => ({ item: getItem(id), qty, id }))
            .filter(row => row.item);
    }

    function renderCheckoutReview() {
        const list = document.getElementById('checkout-review-list');
        const totalEl = document.getElementById('checkout-review-total');
        if (!list) return;

        const rows = getCartEntries();
        if (!rows.length) {
            list.innerHTML = '<p class="text-brand-muted text-sm font-serif italic py-6 text-center">Добавьте позиции в КП — укажите количество в таблице размеров</p>';
            if (totalEl) totalEl.textContent = '0 ₽';
            const nextBtn = document.getElementById('checkout-next-btn');
            if (nextBtn) nextBtn.disabled = true;
            return;
        }

        const nextBtn = document.getElementById('checkout-next-btn');
        if (nextBtn) nextBtn.disabled = false;

        let total = 0;
        list.innerHTML = rows.map(({ item, qty, id }) => {
            const lineSum = item.price * qty;
            total += lineSum;
            const max = itemFreeQty(item);
            return `<div class="checkout-review-row border border-brand-border rounded-sm p-3 bg-brand-bgSoft/40" data-review-id="${esc(id)}">
                <div class="flex items-start justify-between gap-3 mb-2">
                    <div class="min-w-0">
                        <p class="font-serif font-semibold text-brand-cream leading-snug">${esc(item.name)}</p>
                        <p class="text-xs text-brand-muted mt-0.5">${esc(formatSizeLabel(item))}</p>
                    </div>
                    <button type="button" data-review-remove="${esc(id)}" class="shrink-0 w-8 h-8 flex items-center justify-center rounded-sm border border-brand-border text-brand-muted hover:text-brand-low hover:border-brand-low/40 transition-colors" title="Удалить">
                        <i class="fa-solid fa-trash-can text-xs"></i>
                    </button>
                </div>
                <div class="flex items-center justify-between gap-3">
                    <div class="flex items-center border border-brand-border rounded-sm overflow-hidden h-9">
                        <button type="button" data-review-delta="${esc(id)}" data-delta="-1" class="w-9 h-full hover:bg-brand-card text-brand-brass"><i class="fa-solid fa-minus text-[10px]"></i></button>
                        <input type="number" value="${qty}" min="0" max="${max}" data-review-input="${esc(id)}" class="w-10 text-center text-sm font-semibold border-none focus:outline-none bg-transparent text-brand-cream">
                        <button type="button" data-review-delta="${esc(id)}" data-delta="1" class="w-9 h-full hover:bg-brand-card text-brand-brass"><i class="fa-solid fa-plus text-[10px]"></i></button>
                    </div>
                    <div class="text-right">
                        <p class="text-[10px] text-brand-muted uppercase tracking-wider">${formatPrice(item.price)} ₽ × ${qty}</p>
                        <p class="font-semibold text-brand-brass">${formatPrice(lineSum)} ₽</p>
                    </div>
                </div>
            </div>`;
        }).join('');

        if (totalEl) totalEl.textContent = formatPrice(total) + ' ₽';
    }

    function showCheckoutStep(step) {
        const review = document.getElementById('checkout-step-review');
        const form = document.getElementById('checkout-step-form');
        if (!review || !form) return;
        const isReview = step === 'review';
        review.classList.toggle('hidden', !isReview);
        form.classList.toggle('hidden', isReview);
    }

    function openCheckoutModal() {
        renderCheckoutReview();
        showCheckoutStep('review');
        const modal = document.getElementById('checkout-modal');
        if (!modal) return;
        modal.classList.remove('hidden');
        modal.classList.add('is-open');
        document.body.style.overflow = 'hidden';
        modal.scrollTop = 0;
        trackGoal('kp_step_1_request');
    }

    function closeCheckoutModal() {
        const modal = document.getElementById('checkout-modal');
        if (!modal) return;
        modal.classList.remove('is-open');
        modal.classList.add('hidden');
        document.body.style.overflow = '';
        showCheckoutStep('review');
    }

    function goToCheckoutForm() {
        if (!getCartEntries().length) {
            renderCheckoutReview();
            return;
        }
        showCheckoutStep('form');
        trackGoal('kp_step_2_contacts');
    }

    function bindCheckoutTriggers() {
        document.addEventListener('click', e => {
            const openBtn = e.target.closest('[data-open-checkout]');
            if (openBtn) {
                e.preventDefault();
                openCheckoutModal();
                openBtn.closest('.shop-open-checkout')?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
            }
        });
    }

    function backToCheckoutReview() {
        renderCheckoutReview();
        showCheckoutStep('review');
    }

    function bindCheckoutReviewEvents() {
        document.addEventListener('click', e => {
            const removeBtn = e.target.closest('[data-review-remove]');
            if (removeBtn) {
                delete cart[removeBtn.dataset.reviewRemove];
                saveCart();
                updateUI(window.Shop._uiOptions || {});
                if (!getCartEntries().length) closeCheckoutModal();
                return;
            }
            const deltaBtn = e.target.closest('[data-review-delta]');
            if (deltaBtn) {
                updateCart(deltaBtn.dataset.reviewDelta, parseInt(deltaBtn.dataset.delta, 10) || 0);
            }
        });
        document.addEventListener('change', e => {
            const input = e.target.closest('[data-review-input]');
            if (input) setCart(input.dataset.reviewInput, input.value);
        });
    }

    function updateCart(id, delta) {
        const item = getItem(id);
        if (!item || isOnRequest(item)) return;
        cart[id] = Math.max(0, Math.min((cart[id] || 0) + delta, itemFreeQty(item)));
        if (cart[id] <= 0) delete cart[id];
        saveCart();
        updateUI(window.Shop._uiOptions || {});
    }

    function setCart(id, val) {
        const item = getItem(id);
        if (!item || isOnRequest(item)) return;
        const qty = Math.max(0, Math.min(parseInt(val, 10) || 0, itemFreeQty(item)));
        if (qty <= 0) delete cart[id];
        else cart[id] = qty;
        saveCart();
        updateUI(window.Shop._uiOptions || {});
    }

    function clearCart() {
        cart = {};
        saveCart();
        updateUI(window.Shop._uiOptions || {});
    }

    function prepareOrderSubmit(e) {
        const container = document.getElementById('order-qty-fields');
        if (!container) return false;
        container.innerHTML = '';
        let hasItems = false;
        for (const [id, qty] of Object.entries(cart)) {
            if (qty <= 0) continue;
            const item = getItem(id);
            if (!item) continue;
            hasItems = true;
            const input = document.createElement('input');
            input.type = 'hidden';
            input.name = `qty_${item.plant_id}_${item.size_id}`;
            input.value = qty;
            container.appendChild(input);
        }
        if (!hasItems) {
            e.preventDefault();
            alert('Добавьте позиции в КП');
            return false;
        }
        return true;
    }

    function bindCartEvents(root) {
        (root || document).addEventListener('click', e => {
            const onReqBtn = e.target.closest('[data-on-request-open]');
            if (onReqBtn) {
                e.preventDefault();
                openOnRequestModal(onReqBtn.dataset.onRequestOpen);
                return;
            }
            const deltaBtn = e.target.closest('[data-cart-delta]');
            if (deltaBtn) {
                updateCart(deltaBtn.dataset.cartDelta, parseInt(deltaBtn.dataset.delta, 10) || 0);
            }
        });
        (root || document).addEventListener('change', e => {
            const input = e.target.closest('[data-cart-input]');
            if (input) setCart(input.dataset.cartInput, input.value);
        });
    }

    function initScrollTopButton() {
        const btn = document.getElementById('scroll-top-btn');
        if (!btn) return;
        const toggle = () => btn.classList.toggle('visible', window.scrollY > 320);
        btn.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));
        window.addEventListener('scroll', toggle, { passive: true });
        toggle();
    }

    async function initBgSlideshow() {
        const container = document.getElementById('bg-slides');
        if (!container) return;
        let bgSlideTimer = null;
        try {
            const res = await fetch('/public/client/sidebar-slides');
            const data = await res.json();
            const urls = (data.items || []).filter(Boolean);
            if (!urls.length) return;

            const slides = [];
            const appendSlide = (url, active) => {
                const slide = document.createElement('div');
                slide.className = 'bg-slide' + (active ? ' active' : '');
                slide.style.backgroundImage = `url("${url}")`;
                container.appendChild(slide);
                slides.push(slide);
                return slide;
            };

            appendSlide(urls[0], true);

            const interval = data.interval_ms || 7000;
            if (urls.length > 1) {
                let idx = 0;
                if (bgSlideTimer) clearInterval(bgSlideTimer);
                bgSlideTimer = setInterval(() => {
                    if (slides.length < 2) return;
                    slides[idx].classList.remove('active');
                    idx = (idx + 1) % slides.length;
                    slides[idx].classList.add('active');
                }, interval);

                urls.slice(1).forEach((url, i) => {
                    const img = new Image();
                    img.onload = () => appendSlide(url, false);
                    img.onerror = () => {};
                    img.src = url;
                });
            }
        } catch (err) {
            console.warn('bg slideshow:', err);
        }
    }

    function deferBgSlideshow() {
        const run = () => initBgSlideshow();
        if ('requestIdleCallback' in window) {
            requestIdleCallback(run, { timeout: 2500 });
        } else {
            setTimeout(run, 400);
        }
    }

    let onRequestItemId = null;

    function openOnRequestModal(itemId) {
        const item = getItem(itemId);
        if (!item || !isOnRequest(item)) return;
        onRequestItemId = itemId;
        const modal = document.getElementById('on-request-modal');
        const subtitle = document.getElementById('on-request-modal-subtitle');
        const errEl = document.getElementById('on-request-error');
        const okEl = document.getElementById('on-request-success');
        const form = document.getElementById('on-request-form');
        if (!modal || !form) return;

        document.getElementById('on-request-plant-id').value = item.plant_id;
        document.getElementById('on-request-size-id').value = item.size_id;
        if (subtitle) {
            subtitle.textContent = `${item.name} · ${formatSizeLabel(item)}`;
        }
        errEl?.classList.add('hidden');
        okEl?.classList.add('hidden');
        form.querySelectorAll('input:not([type=hidden]), textarea').forEach(el => { el.disabled = false; });
        document.getElementById('on-request-submit')?.classList.remove('hidden');
        modal.classList.remove('hidden');
        document.body.style.overflow = 'hidden';
        document.getElementById('on-request-name')?.focus();
    }

    function closeOnRequestModal() {
        const modal = document.getElementById('on-request-modal');
        if (!modal) return;
        modal.classList.add('hidden');
        document.body.style.overflow = '';
        onRequestItemId = null;
        const form = document.getElementById('on-request-form');
        form?.reset();
        document.getElementById('on-request-error')?.classList.add('hidden');
        document.getElementById('on-request-success')?.classList.add('hidden');
    }

    async function submitOnRequestForm(e) {
        e.preventDefault();
        const form = e.target;
        const errEl = document.getElementById('on-request-error');
        const okEl = document.getElementById('on-request-success');
        const submitBtn = document.getElementById('on-request-submit');
        errEl?.classList.add('hidden');
        okEl?.classList.add('hidden');

        const consent = document.getElementById('on-request-consent');
        if (consent && !consent.checked) {
            if (errEl) {
                errEl.textContent = 'Подтвердите согласие на обработку персональных данных';
                errEl.classList.remove('hidden');
            }
            consent.focus();
            return;
        }

        const payload = {
            plant_id: form.plant_id.value,
            size_id: form.size_id.value,
            customer_name: form.customer_name.value.trim(),
            phone: form.phone.value.trim(),
            message: form.message.value.trim(),
            pd_consent: '1',
        };

        submitBtn.disabled = true;
        try {
            const res = await fetch('/shop/on-request', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
                body: JSON.stringify(payload),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok || data.status !== 'ok') {
                throw new Error(data.message || 'Не удалось отправить заявку');
            }
            if (okEl) {
                okEl.textContent = 'Заявка отправлена. Менеджер свяжется с вами.';
                okEl.classList.remove('hidden');
            }
            form.querySelectorAll('input:not([type=hidden]), textarea').forEach(el => { el.disabled = true; });
            submitBtn.classList.add('hidden');
            setTimeout(closeOnRequestModal, 2200);
        } catch (err) {
            if (errEl) {
                errEl.textContent = err.message || 'Ошибка отправки';
                errEl.classList.remove('hidden');
            }
        } finally {
            submitBtn.disabled = false;
        }
    }

    function initOnRequestModal() {
        const modal = document.getElementById('on-request-modal');
        if (!modal) return;
        document.getElementById('on-request-form')?.addEventListener('submit', submitOnRequestForm);
        document.getElementById('on-request-modal-close')?.addEventListener('click', closeOnRequestModal);
        modal.addEventListener('click', e => {
            if (e.target === modal) closeOnRequestModal();
        });
        document.addEventListener('keydown', e => {
            if (e.key === 'Escape' && !modal.classList.contains('hidden')) {
                closeOnRequestModal();
            }
        });
    }

    window.Shop = {
        init(catalogData, uiOptions) {
            catalog = catalogData || [];
            window.Shop._uiOptions = uiOptions || {};
            loadCart();
            syncCartWithCatalog();
            bindCartEvents();
            bindCheckoutReviewEvents();
            bindCheckoutTriggers();
            initOnRequestModal();
            updateUI(window.Shop._uiOptions);
        },
        getCart: () => ({ ...cart }),
        updateCart,
        setCart,
        clearCart,
        updateUI,
        prepareOrderSubmit,
        openCheckoutModal,
        closeCheckoutModal,
        goToCheckoutForm,
        backToCheckoutReview,
        renderCheckoutReview,
        initScrollTopButton,
        initBgSlideshow,
        deferBgSlideshow,
        formatPrice,
        esc,
        itemFreeQty,
        isOnRequest,
        isPinePlant,
        parseSizeSpec,
        sizeDimensions,
        sizeSortValue,
        formatSizeLabel,
        resolveTableMode,
        stockLabel,
        renderControls,
        openOnRequestModal,
        closeOnRequestModal,
        initOnRequestModal,
        sortVariants(variants) {
            return [...variants].sort((a, b) =>
                sizeSortValue(a) - sizeSortValue(b) ||
                String(a.spec).localeCompare(String(b.spec), 'ru')
            );
        },
    };
})();
