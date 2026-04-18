const CACHE_NAME = 'floraflow-v1';

// Событие установки (пока просто активируем)
self.addEventListener('install', (event) => {
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    return self.clients.claim();
});

// Пока не перехватываем запросы (оставим пустым для безопасности на Этапе 1)
// На следующем этапе мы добавим сюда логику офлайна.
self.addEventListener('fetch', (event) => {
    // Pass through - просто пропускаем запросы в интернет
});