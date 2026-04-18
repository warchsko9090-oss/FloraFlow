const CACHE_VERSION = 'v2'; // Меняй эту цифру, если обновил дизайн, чтобы сбросить кэш у всех
const STATIC_CACHE = `static-${CACHE_VERSION}`;
const DYNAMIC_CACHE = `dynamic-${CACHE_VERSION}`;

// Список файлов, которые нужны всегда (ОБОЛОЧКА)
const ASSETS_TO_CACHE = [
    '/offline', // Наша страница ошибки
    '/static/icon-192.png',
    'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap',
    'https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css',
    'https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/css/select2.min.css',
    'https://cdn.jsdelivr.net/npm/select2-bootstrap-5-theme@1.3.0/dist/select2-bootstrap-5-theme.min.css',
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css',
    'https://unpkg.com/nprogress@0.2.0/nprogress.css',
    'https://code.jquery.com/jquery-3.7.0.min.js',
    'https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js',
    'https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/js/select2.min.js',
    'https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/js/i18n/ru.js',
    'https://unpkg.com/htmx.org@1.9.10',
    'https://unpkg.com/nprogress@0.2.0/nprogress.js'
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
        url.pathname.startsWith('/shop')) {
        return;
    }

    // СТРАТЕГИЯ 1: Кэш (Cache First) -> Сеть
    // Для статики (картинки, скрипты, шрифты)
    if (url.pathname.startsWith('/static/') || 
        url.hostname.includes('cdn') || 
        url.hostname.includes('unpkg') || 
        url.hostname.includes('fonts') ||
        url.hostname.includes('cdnjs')) {
        
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
                });
            })
    );
});