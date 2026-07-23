"""Инкрементальные миграции схемы без Alembic (SQLite и PostgreSQL)."""
from __future__ import annotations

from sqlalchemy import inspect, text

from app.models import db

# (table, column, postgresql_type, sqlite_type)
_LEGACY_COLUMNS: list[tuple[str, str, str, str]] = [
    ('time_log', 'is_day_off', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('expense', 'target_month', 'INTEGER', 'INTEGER'),
    ('expense', 'target_year', 'INTEGER', 'INTEGER'),
    ('"order"', 'is_barter', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('"order"', 'reserve_ack_at', 'TIMESTAMP', 'DATETIME'),
    ('"order"', 'created_by_user_id', 'INTEGER', 'INTEGER'),
    ('employee', 'official_budget_item_id', 'INTEGER', 'INTEGER'),
    ('employee', 'unofficial_budget_item_id', 'INTEGER', 'INTEGER'),
    ('payment', 'payment_type', "VARCHAR(20) DEFAULT 'cashless'", "VARCHAR(20) DEFAULT 'cashless'"),
    ('expense', 'barter_order_id', 'INTEGER', 'INTEGER'),
    ('client', 'fixed_balance', 'NUMERIC(15, 2)', 'NUMERIC(15, 2)'),
    ('client', 'fixed_balance_date', 'DATE', 'DATE'),
    ('tg_task', 'completed_at', 'TIMESTAMP', 'DATETIME'),
    ('tg_task', 'completed_by_id', 'INTEGER', 'INTEGER'),
    ('tg_task', 'dedup_key', 'VARCHAR(255)', 'VARCHAR(255)'),
    ('tg_task', 'first_seen_at', 'TIMESTAMP', 'DATETIME'),
    ('tg_task', 'last_seen_at', 'TIMESTAMP', 'DATETIME'),
    ('tg_task', 'severity', 'VARCHAR(20)', 'VARCHAR(20)'),
    ('tg_task', 'created_by_id', 'INTEGER', 'INTEGER'),
    ('tg_task', 'updated_at', 'TIMESTAMP', 'DATETIME'),
    ('tg_task', 'reassigned_from_id', 'INTEGER', 'INTEGER'),
    ('tg_task', 'reassigned_at', 'TIMESTAMP', 'DATETIME'),
    ('tg_task', 'source', 'VARCHAR(20)', 'VARCHAR(20)'),
    ('field', 'map_x', 'DOUBLE PRECISION', 'FLOAT'),
    ('field', 'map_y', 'DOUBLE PRECISION', 'FLOAT'),
    ('field', 'map_w', 'DOUBLE PRECISION', 'FLOAT'),
    ('field', 'map_h', 'DOUBLE PRECISION', 'FLOAT'),
    ('field', 'map_shape', 'TEXT', 'TEXT'),
    ('field', 'map_color', 'VARCHAR(20)', 'VARCHAR(20)'),
    ('field', 'map_z', 'INTEGER', 'INTEGER'),
    ('field', 'map_layout', 'VARCHAR(20)', 'VARCHAR(20)'),
    ('map_settings', 'bg_fit', 'VARCHAR(20)', 'VARCHAR(20)'),
    ('map_settings', 'bg_offset_x', 'DOUBLE PRECISION', 'FLOAT'),
    ('map_settings', 'bg_offset_y', 'DOUBLE PRECISION', 'FLOAT'),
    ('map_settings', 'bg_scale', 'DOUBLE PRECISION', 'FLOAT'),
    ('map_settings', 'bg_rotation', 'DOUBLE PRECISION', 'FLOAT'),
    ('map_settings', 'canvas_width', 'INTEGER', 'INTEGER'),
    ('map_settings', 'canvas_aspect', 'VARCHAR(20)', 'VARCHAR(20)'),
    ('competitor_row', 'pack_type', 'VARCHAR(16)', 'VARCHAR(16)'),
    ('competitor_row', 'form', 'VARCHAR(16)', 'VARCHAR(16)'),
    ('competitor_row', 'source_excerpt', 'TEXT', 'TEXT'),
    ('competitor_row', 'confidence', 'DOUBLE PRECISION', 'FLOAT'),
    ('competitor_row', 'is_rejected', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('competitor_row', 'reject_reasons', 'TEXT', 'TEXT'),
    ('competitor_snapshot', 'raw_ai_response', 'TEXT', 'TEXT'),
    ('budget_item', 'is_vium_source', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('payment_invoice', 'vium_intake_mode', 'VARCHAR(10)', 'VARCHAR(10)'),
    ('document', 'project_id', 'INTEGER', 'INTEGER'),
    ('document', 'supplier_id', 'INTEGER', 'INTEGER'),
    ('document_row', 'purchase_price', 'NUMERIC(10, 2)', 'NUMERIC(10,2)'),
    ('project', 'potting_stock_field_id', 'INTEGER', 'INTEGER'),
    ('stock_purchase_lot', 'supplier_id', 'INTEGER', 'INTEGER'),
    ('stock_purchase_lot', 'purchase_price', 'NUMERIC(10, 2)', 'NUMERIC(10,2)'),
    ('stock_purchase_lot', 'document_id', 'INTEGER', 'INTEGER'),
    ('stock_purchase_lot', 'document_row_id', 'INTEGER', 'INTEGER'),
    ('stock_purchase_lot', 'created_at', 'TIMESTAMP', 'DATETIME'),
    ('shop_plant_card', 'is_hot', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('shop_plant_card', 'display_order', 'INTEGER DEFAULT 0', 'INTEGER DEFAULT 0'),
    ('shop_plant_card', 'is_hidden', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('shop_plant_card', 'seedling_visible', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('shop_plant_card', 'seedling_on_request', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('shop_plant_card', 'seedling_root_system', "VARCHAR(160) DEFAULT ''", "VARCHAR(160) DEFAULT ''"),
    ('shop_plant_card', 'seedling_pruning', "VARCHAR(160) DEFAULT ''", "VARCHAR(160) DEFAULT ''"),
    ('shop_plant_card', 'seedling_is_hot', 'BOOLEAN DEFAULT FALSE', 'BOOLEAN DEFAULT 0'),
    ('shop_plant_card', 'seedling_display_order', 'INTEGER DEFAULT 0', 'INTEGER DEFAULT 0'),
    ('foreign_employee_profile', 'registration_end_date', 'DATE', 'DATE'),
]

_LEGACY_INDEXES = [
    'CREATE INDEX IF NOT EXISTS idx_docrow_document ON document_row (document_id)',
    'CREATE INDEX IF NOT EXISTS idx_document_date ON document (date)',
    'CREATE INDEX IF NOT EXISTS idx_document_type_date ON document (doc_type, date)',
    'CREATE INDEX IF NOT EXISTS idx_expense_date_budget ON expense (date, budget_item_id)',
    'CREATE INDEX IF NOT EXISTS idx_expense_employee ON expense (employee_id)',
    'CREATE INDEX IF NOT EXISTS idx_expense_invoice ON expense (invoice_id)',
    'CREATE INDEX IF NOT EXISTS idx_timelog_date ON time_log (date)',
    'CREATE INDEX IF NOT EXISTS idx_order_created_by ON "order" (created_by_user_id)',
    'CREATE INDEX IF NOT EXISTS idx_tgtask_dedup ON tg_task (dedup_key)',
    'CREATE INDEX IF NOT EXISTS idx_tgtask_source ON tg_task (source)',
    'CREATE INDEX IF NOT EXISTS idx_tgtask_status_deadline ON tg_task (status, deadline)',
    'CREATE INDEX IF NOT EXISTS idx_purchase_lot_batch ON stock_purchase_lot (plant_id, field_id, year)',
    'CREATE INDEX IF NOT EXISTS idx_purchase_lot_pos ON stock_purchase_lot (plant_id, size_id, field_id, year)',
]


def _table_name_key(table: str) -> str:
    return table.strip('"')


def _existing_columns(table: str) -> set[str]:
    insp = inspect(db.engine)
    name = _table_name_key(table)
    if not insp.has_table(name):
        return set()
    return {c['name'] for c in insp.get_columns(name)}


def _add_column(table: str, column: str, col_type: str, *, if_not_exists: bool) -> None:
    suffix = ' IF NOT EXISTS' if if_not_exists else ''
    db.session.execute(text(f'ALTER TABLE {table} ADD COLUMN{suffix} {column} {col_type}'))


def ensure_legacy_schema(logger=None) -> None:
    """Добавляет недостающие колонки и индексы (PostgreSQL на Amvera и локальный SQLite).

    На Postgres ставит короткий lock_timeout, чтобы ALTER не вешал весь пул
    воркеров при конкурирующих запросах (типичный симптом: логи тихие, страницы
    висят до таймаута прокси). Каждая колонка коммитится отдельно.
    """
    dialect = db.engine.dialect.name
    is_pg = dialect == 'postgresql'

    def _reset_lock_timeout():
        if not is_pg:
            return
        try:
            db.session.execute(text("SET LOCAL lock_timeout = '3s'"))
        except Exception as exc:
            if logger:
                logger.warning('legacy schema: lock_timeout not set — %s', exc)

    _reset_lock_timeout()

    for table, column, pg_type, sqlite_type in _LEGACY_COLUMNS:
        try:
            if column in _existing_columns(table):
                continue
            col_type = pg_type if is_pg else sqlite_type
            _add_column(table, column, col_type, if_not_exists=is_pg)
            db.session.commit()
            if logger:
                logger.info('legacy schema: added %s.%s', _table_name_key(table), column)
            _reset_lock_timeout()
        except Exception as exc:
            if logger:
                logger.warning('legacy schema: skip %s.%s — %s', _table_name_key(table), column, exc)
            db.session.rollback()
            _reset_lock_timeout()

    for stmt in _LEGACY_INDEXES:
        try:
            db.session.execute(text(stmt))
            db.session.commit()
        except Exception as exc:
            if logger:
                logger.warning('legacy schema: skip index — %s', exc)
            db.session.rollback()
