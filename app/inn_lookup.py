"""Реквизиты по ИНН: своя карточка клиента, затем ЕГРЮЛ через DaData."""
from __future__ import annotations

import os
import re

import requests

from app.models import Client


def inn_digits(value: str | None) -> str:
    return re.sub(r'\D+', '', str(value or ''))[:12]


def _empty_fields() -> dict:
    return {
        'name': '', 'inn': '', 'kpp': '', 'ogrn': '',
        'address': '', 'phone': '', 'bank': '', 'rs': '', 'bik': '', 'ks': '',
    }


def _from_client(client: Client, inn: str) -> dict:
    return {
        'name': client.name or '',
        'inn': inn_digits(client.inn) or inn,
        'kpp': (client.kpp or '').strip(),
        'ogrn': (client.ogrn or '').strip(),
        'address': (client.address or '').strip(),
        'phone': (client.phone or '').strip(),
        'bank': (client.bank_name or '').strip(),
        'rs': (client.rs or '').strip(),
        'bik': (client.bik or '').strip(),
        'ks': (client.ks or '').strip(),
        'client_id': client.id,
    }


def _find_client(inn: str) -> Client | None:
    if len(inn) not in (10, 12):
        return None
    for client in Client.query.filter(Client.inn.isnot(None)).all():
        if inn_digits(client.inn) == inn:
            return client
    return None


def _dadata_token() -> str:
    return (
        os.environ.get('DADATA_API_KEY')
        or os.environ.get('DADATA_TOKEN')
        or ''
    ).strip()


def _dadata_headers() -> dict:
    return {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Token {_dadata_token()}',
    }


def _from_dadata(inn: str) -> tuple[dict, str, str]:
    """(fields, status, error). status — ACTIVE / LIQUIDATED / …"""
    token = _dadata_token()
    if not token:
        return {}, '', 'no_key'
    try:
        resp = requests.post(
            'https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party',
            json={'query': inn},
            headers=_dadata_headers(),
            timeout=8,
        )
        if resp.status_code != 200:
            return {}, '', f'http_{resp.status_code}'
        suggestions = (resp.json() or {}).get('suggestions') or []
        if not suggestions:
            return {}, '', 'not_found'
        item = suggestions[0]
        data = item.get('data') or {}
        name = (
            ((data.get('name') or {}).get('short_with_opf'))
            or ((data.get('name') or {}).get('full_with_opf'))
            or item.get('value')
            or ''
        )
        addr = data.get('address') or {}
        address = (
            addr.get('unrestricted_value')
            or addr.get('value')
            or ((addr.get('data') or {}).get('source'))
            or ''
        )
        state = ((data.get('state') or {}).get('status') or '').upper()
        fields = _empty_fields()
        fields.update({
            'name': str(name).strip(),
            'inn': inn_digits(data.get('inn')) or inn,
            'kpp': str(data.get('kpp') or '').strip(),
            'ogrn': str(data.get('ogrn') or '').strip(),
            'address': str(address).strip(),
        })
        return fields, state, ''
    except Exception:
        return {}, '', 'network'


def _from_dadata_bank(bik: str) -> dict:
    """Название банка и корсчёт по БИК. В ЕГРЮЛ этого нет."""
    bik = re.sub(r'\D+', '', str(bik or ''))[:9]
    if len(bik) != 9 or not _dadata_token():
        return {}
    try:
        resp = requests.post(
            'https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/bank',
            json={'query': bik},
            headers=_dadata_headers(),
            timeout=8,
        )
        if resp.status_code != 200:
            return {}
        suggestions = (resp.json() or {}).get('suggestions') or []
        if not suggestions:
            return {}
        item = suggestions[0]
        data = item.get('data') or {}
        name = (
            ((data.get('name') or {}).get('payment'))
            or ((data.get('name') or {}).get('short'))
            or item.get('value')
            or ''
        )
        out = {}
        if str(name).strip():
            out['bank'] = str(name).strip()
        ks = str(data.get('correspondent_account') or '').strip()
        if ks:
            out['ks'] = re.sub(r'\D+', '', ks)[:20]
        out['bik'] = bik
        return out
    except Exception:
        return {}


def _fill_empty(dest: dict, src: dict) -> dict:
    out = dict(dest)
    for key, val in src.items():
        if key == 'client_id':
            continue
        if not (out.get(key) or '').strip() and val:
            out[key] = val
    return out


def lookup_requisites(inn_raw: str) -> dict:
    inn = inn_digits(inn_raw)
    if len(inn) not in (10, 12):
        return {
            'ok': False,
            'error': 'bad_inn',
            'hint': 'ИНН — 10 цифр (юрлицо) или 12 (ИП)',
            'fields': _empty_fields(),
            'egrul_ready': bool(_dadata_token()),
        }
    fields = _empty_fields()
    fields['inn'] = inn
    sources = []
    client = _find_client(inn)
    if client:
        fields = _fill_empty(fields, _from_client(client, inn))
        sources.append('db')
    egrul, status, err = _from_dadata(inn)
    if egrul:
        fields = _fill_empty(fields, egrul)
        sources.append('egrul')
    bik = re.sub(r'\D+', '', fields.get('bik') or '')[:9]
    if len(bik) == 9 and (not (fields.get('bank') or '').strip() or not (fields.get('ks') or '').strip()):
        bank = _from_dadata_bank(bik)
        if bank:
            before = (fields.get('bank') or '').strip()
            fields = _fill_empty(fields, bank)
            if not before and (fields.get('bank') or '').strip():
                sources.append('bik')
    hint = ''
    core = [s for s in sources if s != 'bik']
    if status in ('LIQUIDATED', 'BANKRUPT'):
        hint = 'В ЕГРЮЛ организация не действующая — проверьте реквизиты'
    elif not core:
        if err == 'no_key':
            hint = 'ЕГРЮЛ не подключен: в Amvera нет DADATA_API_KEY'
        elif err == 'not_found':
            hint = 'По этому ИНН в ЕГРЮЛ ничего не нашли'
        elif err:
            hint = 'ЕГРЮЛ сейчас не ответил, заполните вручную или вставьте файл'
        else:
            hint = 'Реквизиты не найдены'
    elif 'egrul' in core and 'db' in core:
        hint = 'Карточка клиента + недостающее из ЕГРЮЛ'
    elif core == ['db']:
        hint = 'Подставили из вашей базы клиентов'
    elif 'egrul' in core:
        hint = 'Подставили из ЕГРЮЛ'
    else:
        hint = 'Подставили реквизиты'
    if 'bik' in sources:
        hint = hint.rstrip('.') + '. Название банка подставили по БИК'
    return {
        'ok': bool(sources),
        'fields': fields,
        'source': '+'.join(sources),
        'status': status,
        'hint': hint,
        'egrul_ready': bool(_dadata_token()),
        'client_id': client.id if client else None,
    }
