# gunicorn.conf.py — конфиг для деплоя на Amvera (и любой другой PaaS с reverse-proxy)

import multiprocessing
import os

# --- ТАЙМАУТЫ И ПАРАЛЛЕЛИЗМ ---
# SQLite-бэкенд плохо дружит с большим числом write-воркеров, поэтому 2 воркера + треды дают
# приличную производительность для чтений и один активный writer в каждый момент.
workers = int(os.environ.get('GUNICORN_WORKERS', '2'))
threads = int(os.environ.get('GUNICORN_THREADS', '4'))
worker_class = 'gthread'
timeout = int(os.environ.get('GUNICORN_TIMEOUT', '120'))
graceful_timeout = 30
keepalive = 5

# --- СЕТЬ / ПРОКСИ ---
# Amvera кладёт приложение за reverse-proxy, нужно доверять X-Forwarded-* от любого источника.
forwarded_allow_ips = '*'
proxy_allow_ips = '*'

# --- ЛОГИ В STDOUT/STDERR (Amvera их собирает автоматически) ---
accesslog = '-'
errorlog = '-'
loglevel = os.environ.get('LOG_LEVEL', 'info').lower()
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(L)ss'

# --- ЖИЗНЕННЫЙ ЦИКЛ ВОРКЕРОВ ---
# Перезапускаем воркеров каждые max_requests запросов (страховка от утечек памяти).
max_requests = 1000
max_requests_jitter = 100
preload_app = False  # С preload_app=True SQLite-соединения наследуются от мастера — не надо.
