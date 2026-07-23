/** Фоновое слайд-шоу (как в каталоге /shop) — /public/client/sidebar-slides */
(function (global) {
    async function initSlideshow(containerId, apiUrl, slideClass) {
        const container = document.getElementById(containerId);
        if (!container) return;
        const slideCls = slideClass || 'bg-slide';
        let slideTimer = null;
        try {
            const res = await fetch(apiUrl);
            const data = await res.json();
            const urls = (data.items || []).filter(Boolean);
            if (!urls.length) return;

            const slides = [];
            const appendSlide = (url, active) => {
                const slide = document.createElement('div');
                slide.className = slideCls + (active ? ' active' : '');
                slide.style.backgroundImage = 'url("' + url.replace(/"/g, '\\"') + '")';
                container.appendChild(slide);
                slides.push(slide);
                return slide;
            };

            const existing = container.querySelector('.' + slideCls + '.active');
            if (existing) {
                slides.push(existing);
            } else {
                appendSlide(urls[0], true);
            }

            const interval = data.interval_ms || 7000;
            if (urls.length > 1) {
                let idx = 0;
                if (slideTimer) clearInterval(slideTimer);
                slideTimer = setInterval(function () {
                    if (slides.length < 2) return;
                    slides[idx].classList.remove('active');
                    idx = (idx + 1) % slides.length;
                    slides[idx].classList.add('active');
                }, interval);

                const preloadUrls = existing ? urls.slice(1) : urls.slice(1);
                preloadUrls.forEach(function (url) {
                    const img = new Image();
                    img.onload = function () { appendSlide(url, false); };
                    img.onerror = function () {};
                    img.src = url;
                });
            }
        } catch (err) {
            console.warn('slideshow:', containerId, err);
        }
    }

    function initBgSlideshow() {
        return initSlideshow('bg-slides', '/public/client/sidebar-slides', 'bg-slide');
    }

    function initInlineSlideshow(containerId, apiUrl) {
        return initSlideshow(containerId, apiUrl, 'bg-slide');
    }

    function deferSlideshow(run) {
        if ('requestIdleCallback' in global) {
            global.requestIdleCallback(run, { timeout: 2500 });
        } else {
            global.setTimeout(run, 400);
        }
    }

    function deferBgSlideshow() {
        deferSlideshow(function () { initBgSlideshow(); });
    }

    function deferInlineSlideshow(containerId, apiUrl) {
        deferSlideshow(function () { initInlineSlideshow(containerId, apiUrl); });
    }

    global.LandingBg = {
        initBgSlideshow: initBgSlideshow,
        initInlineSlideshow: initInlineSlideshow,
        deferBgSlideshow: deferBgSlideshow,
        deferInlineSlideshow: deferInlineSlideshow,
    };
})(window);
