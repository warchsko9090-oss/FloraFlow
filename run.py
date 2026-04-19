from app import create_app
from app.models import db  # <--- Добавили импорт БД

app = create_app()

# Попытка импорта скрипта обучения.
try:
    from train_ai import seed_knowledge
    HAS_TRAINING_SCRIPT = True
except ImportError:
    HAS_TRAINING_SCRIPT = False
    print("ВНИМАНИЕ: Файл train_ai.py не найден. Авто-обучение пропущено.")

if __name__ == '__main__' or True: # Для Gunicorn
    with app.app_context():
        from app import create_app
        from app.models import db # <--- ДОБАВИТЬ ИМПОРТ DB

        app = create_app()

        try:
            from train_ai import seed_knowledge
            HAS_TRAINING_SCRIPT = True
        except ImportError:
            HAS_TRAINING_SCRIPT = False

        if __name__ == '__main__' or True: # Для Gunicorn
            with app.app_context():
                db.create_all()  # <--- Добавили эту строку (создаст новые таблицы)
                # ВКЛЮЧАЕМ ОБУЧЕНИЕ для загрузки новых примеров
                if HAS_TRAINING_SCRIPT:
                try:
                    seed_knowledge()
                except Exception as e:
                    pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
