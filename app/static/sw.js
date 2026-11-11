const CACHE_VERSION = 'v72'; // Меняй эту цифру, если обновил дизайн, чтобы сбросить кэш у всех
const STATIC_CACHE = `static-${CACHE_VERSION}`;
const DYNAMIC_CACHE = `dynamic-${CACHE_VERSION}`;

// Страницы и API с интенсивно меняющимся состоянием — всегда только из сети, без кэша.
// /api/cache-manifest сюда же — его нельзя кэшировать, иначе клиент будет думать,
// что версия не менялась, и пропустит прогрев.
const NO_CACHE_PATHS = [
    '/digging/planning', '/documents', '/logs', '/digging',
    '/stock', '/stock/export',
    '/api/visual_stock', '/api/field_recount', '/api/stock_info',
    '/api/inventory', '/api/feed',
    '/api/cache-manifest',
];

// Список файлов, которые нужны всегда (ОБОЛОЧКА)
const ASSETS_TO_CACHE = [
    '/offline',
    '/static/icon-192.png',
    '/static/vendor/fonts/inter-google.css',
    '/static/vendor/bootstrap.min.css',
    '/static/vendor/select2.min.css',
    '/static/vendor/select2-bootstrap-5-theme.min.css',
    '/static/vendor/fontawesome/css/all.min.css',
    '/static/vendor/nprogress.css',
    '/static/vendor/jquery.min.js',
    '/static/vendor/bootstrap.bundle.min.js',
    '/static/vendor/select2.min.js',
    '/static/vendor/select2.ru.js',
    '/static/vendor/htmx.min.js',
    '/static/vendor/nprogress.js',
];

// 1. УСТАНОВКА: Скачиваем критически важные файлы
self.addEventListener('install', (event) => {
    self.skipWaiting();
    event.waitUntil(
        caches.open(STATIC_CACHE).then((cache) => {
            console.log('[SW] Caching shell assets');
            return cache.addAll(ASSETS_TO_CACHE);
        })
    );
});

// 2. АКТИВАЦИЯ: Удаляем старые кэши
self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys().then((keys) => {
            return Promise.all(
                keys.map((key) => {
                    if (key !== STATIC_CACHE && key !== DYNAMIC_CACHE) {
                        console.log('[SW] Removing old cache', key);
                        return caches.delete(key);
                    }
                })
            );
        })
    );
    return self.clients.claim();
});

// 3. ПЕРЕХВАТ ЗАПРОСОВ
self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // Игнорируем POST запросы, чат, админку И ПУБЛИЧНУЮ ВИТРИНУ КЛИЕНТА
    if (event.request.method !== 'GET' || 
        url.pathname.startsWith('/chat/') || 
        url.pathname.startsWith('/admin/') || 
        url.pathname.startsWith('/shop') ||
        url.pathname.startsWith('/tg/')) {
        return;
    }

    // Для страниц с drag-and-drop и живым состоянием (план, документы, логи)
    // всегда обращаемся в сеть и НИКОГДА не кэшируем — чтобы избежать устаревших данных
    const isNoCachePath = NO_CACHE_PATHS.some(
        p => url.pathname === p || url.pathname.startsWith(p + '/') || url.pathname.startsWith(p + '?')
    );
    if (isNoCachePath) {
        event.respondWith(fetch(event.request, { credentials: 'same-origin' }));
        return;
    }

    // СТРАТЕГИЯ 1: Кэш (Cache First) -> Сеть
    // Оболочка: свои копии CSS/JS/шрифтов/картинок. CDN больше не трогаем.
    if (url.pathname.startsWith('/static/')) {
        
        event.respondWith(
            caches.match(event.request).then((cachedResponse) => {
                if (cachedResponse) return cachedResponse;
                return fetch(event.request).then((networkResponse) => {
                    return caches.open(STATIC_CACHE).then((cache) => {
                        cache.put(event.request, networkResponse.clone());
                        return networkResponse;
                    });
                });
            })
        );
        return;
    }

    // СТРАТЕГИЯ 2: Сеть (Network First) -> Кэш -> Офлайн страница
    // Для HTML страниц (Заказы, Склад)
    event.respondWith(
        fetch(event.request, { credentials: 'same-origin' })
            .then((networkResponse) => {
                // Если ответ успешный, сохраняем копию страницы в кэш
                // Но не кэшируем редиректы на /login (это мешает при смене сессии/логине)
                const shouldCache = networkResponse && networkResponse.ok &&
                    !networkResponse.redirected &&
                    !networkResponse.url.includes('/login') &&
                    !networkResponse.url.includes('/logout');

                if (shouldCache) {
                    return caches.open(DYNAMIC_CACHE).then((cache) => {
                        cache.put(event.request, networkResponse.clone());
                        return networkResponse;
                    });
                }
                return networkResponse;
            })
            .catch(() => {
                // Если сети нет, ищем в кэше
                return caches.match(event.request).then((cachedResponse) => {
                    if (cachedResponse) return cachedResponse;
                    
                    // Если это запрос от HTMX (частичное обновление), не отдаем целую страницу
                    if (event.request.headers.get('HX-Request')) {
                        return new Response('<div class="alert alert-danger m-3">Нет связи с сервером!</div>', {
                            headers: { 'Content-Type': 'text/html' }
                        });
                    }

                    // БЕЗОПАСНАЯ ПРОВЕРКА ЗАГОЛОВКА ACCEPT
                    const acceptHeader = event.request.headers.get('accept');
                    if (acceptHeader && acceptHeader.includes('text/html')) {
                        return caches.match('/offline');
                    }
                    return new Response('', { status: 503, statusText: 'offline' });
                });
            })
    );
});

// 4. WEB PUSH — приём уведомлений от сервера и показ системного баннера.
//    Сервер шлёт payload вида { title, body, url, tag, task_id }.
self.addEventListener('push', (event) => {
    let data = {};
    try {
        if (event.data) {
            data = event.data.json();
        }
    } catch (e) {
        try { data = { title: 'Новая задача', body: event.data ? event.data.text() : '' }; }
        catch (_) { data = { title: 'Новая задача' }; }
    }

    const title = data.title || 'Новая задача';
    const options = {
        body: data.body || '',
        icon: '/static/icon-192.png',
        badge: '/static/icon-192.png',
        tag: data.tag || ('task-' + (data.task_id || Date.now())),
        renotify: true,
        data: { url: data.url || '/', task_id: data.task_id || null },
    };

    event.waitUntil(self.registration.showNotification(title, options));
});

// 5. КЛИК ПО УВЕДОМЛЕНИЮ — открыть приложение на нужной странице.
self.addEventListener('notificationclick', (event) => {
    event.notification.close();
    const targetUrl = (event.notification.data && event.notification.data.url) || '/';

    event.waitUntil((async () => {
        const allClients = await clients.matchAll({ type: 'window', includeUncontrolled: true });
        // Если уже открыта вкладка приложения — переключимся на неё и навигируем.
        for (const client of allClients) {
            try {
                if ('focus' in client) {
                    await client.focus();
                    if ('navigate' in client) {
                        try { await client.navigate(targetUrl); } catch (_) { /* same-origin only */ }
                    }
                    return;
                }
            } catch (e) { /* ignore */ }
        }
        // Иначе — откроем новое окно/таб (на iOS PWA — фокусируем приложение).
        if (clients.openWindow) {
            try { await clients.openWindow(targetUrl); } catch (_) { /* ignore */ }
        }
    })());
});