from app import create_app

app = create_app()

# AI-обучение — опциональный модуль; если файла train_ai.py нет, сайт стартует без него.
try:
    from train_ai import seed_knowledge
    _HAS_TRAINING_SCRIPT = True
except Exception as _e:
    _HAS_TRAINING_SCRIPT = False
    app.logger.info(f'train_ai.py не подключен ({_e}) — авто-обучение пропущено.')

if _HAS_TRAINING_SCRIPT:
    with app.app_context():
        try:
            seed_knowledge()
        except Exception as e:
            app.logger.warning(f'Ошибка при запуске обучения: {e}')


if __name__ == '__main__':
    # Локальный запуск (python run.py). В проде работает gunicorn -c gunicorn.conf.py run:app.
    app.run(host='0.0.0.0', port=5000, debug=True)
