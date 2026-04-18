# gunicorn.conf.py
timeout = 120
workers = 2
threads = 4
worker_class = 'gthread'