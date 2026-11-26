# Amvera shop door (устарело как отдельный stub)

**Актуальный failover 1:1:** деплой **корня FloraFlow** в проект Amvera `kdoor` с конфигом [`../amvera.door.yml`](../amvera.door.yml) и entrypoint [`../wsgi_door.py`](../wsgi_door.py).

Еженедельный снимок (вс 00:00): [`../scripts/sync-failover-weekly.ps1`](../scripts/sync-failover-weekly.ps1) — БД/цены + фото с `MANIFEST` sha256, заливка на `/internal/failover-sync`, опционально push кода.

Поведение:

1. Прокси на дом (`UPSTREAM_ORIGIN`), пока дом отвечает.
2. Если дом недоступен — локальная витрина FloraFlow с `SHOP_READONLY=1` (тот же UI, без шт / КП / заявок).

Снимок данных:

```bash
python scripts/export-shop-failover-snapshot.py
# → amvera-shop-door/data-snapshot/failover.db + media
# Залить в Amvera Data как /data/failover.db и папки медиа
```

Env на Amvera: см. комментарии в `amvera.door.yml`.

Старый `stub.html` / `FAILOVER_STUB=1` без полного shop **не использовать** для продакшен-failover.
