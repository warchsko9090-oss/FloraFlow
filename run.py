from app import create_app

app = create_app()

# Попытка импорта скрипта обучения.
# Если файл train_ai.py не создан - сайт все равно запустится, просто не обучится.
try:
    from train_ai import seed_knowledge
    HAS_TRAINING_SCRIPT = True
except ImportError:
    HAS_TRAINING_SCRIPT = False
    print("ВНИМАНИЕ: Файл train_ai.py не найден. Авто-обучение пропущено.")

if __name__ == '__main__' or True: # Для Gunicorn
    with app.app_context():
        # ВКЛЮЧАЕМ ОБУЧЕНИЕ для загрузки новых примеров
        if HAS_TRAINING_SCRIPT:
            try:
                seed_knowledge()
            except Exception as e:
                print(f"Ошибка при запуске обучения: {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)