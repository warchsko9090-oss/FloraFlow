from sqlalchemy import func
from app.models import db, Order, OrderItem, StockBalance


def get_reserved_map(plant_id=None, size_id=None):
    """Build a dict {(plant_id, size_id, field_id, year): reserved_qty}.

    Optional plant_id/size_id narrow the query.
    """
    q = (
        db.session.query(
            OrderItem.plant_id,
            OrderItem.size_id,
            OrderItem.field_id,
            OrderItem.year,
            func.sum(OrderItem.quantity - OrderItem.shipped_quantity),
        )
        .join(Order)
        .filter(
            Order.status != "canceled",
            Order.status != "ghost",
            Order.is_deleted == False,
        )
    )
    if plant_id is not None:
        q = q.filter(OrderItem.plant_id == plant_id)
    if size_id is not None:
        q = q.filter(OrderItem.size_id == size_id)

    q = q.group_by(OrderItem.plant_id, OrderItem.size_id, OrderItem.field_id, OrderItem.year)
    return {(r[0], r[1], r[2], r[3]): int(r[4] or 0) for r in q.all()}
