"""Миграции nullable поле/партия у позиций заказа (для активного менеджера продаж)."""

from __future__ import annotations

from sqlalchemy import inspect, text

from app.models import db


def ensure_order_item_batch_nullable(logger=None):
    """Разрешает OrderItem без field_id/year (назначение позже менеджером питомника)."""
    insp = inspect(db.engine)
    if not insp.has_table('order_item'):
        return
    cols = {c['name']: c for c in insp.get_columns('order_item')}
    dialect = db.engine.dialect.name

    def _drop_not_null(column: str) -> None:
        meta = cols.get(column)
        if not meta or meta.get('nullable'):
            return
        if dialect == 'postgresql':
            db.session.execute(text(f'ALTER TABLE order_item ALTER COLUMN {column} DROP NOT NULL'))
            db.session.commit()
            if logger:
                logger.info('order_item.%s is now nullable', column)
        elif dialect == 'sqlite':
            # SQLite не умеет DROP NOT NULL без пересоздания таблицы — новые INSERT с NULL
            # работают, если ORM не шлёт NOT NULL constraint на уровне приложения.
            if logger:
                logger.info('sqlite: skip ALTER NULL for order_item.%s (ORM nullable=True)', column)

    try:
        _drop_not_null('field_id')
        cols = {c['name']: c for c in insp.get_columns('order_item')}
        _drop_not_null('year')
    except Exception as exc:
        db.session.rollback()
        if logger:
            logger.warning('ensure_order_item_batch_nullable skipped: %s', exc)
