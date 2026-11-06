"""Rebuild today's ops digest with fixed Gemini (no Flask migrations)."""
import os
import sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

for name in ('gemini.env', 'backup-pg.env'):
    p = Path.home() / '.floraflow' / name
    if not p.exists():
        continue
    for line in p.read_text(encoding='utf-8').splitlines():
        if '=' in line and not line.strip().startswith('#'):
            k, v = line.split('=', 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k == 'FLORAFLOW_BACKUP_DATABASE_URL':
                os.environ['DATABASE_URL'] = v
            elif k:
                os.environ.setdefault(k, v)

os.environ.setdefault('DISABLE_SCHEDULER', '1')

from flask import Flask
from app.models import db, OpsDigestLog
from app.ai_ops import (
    _load_day_messages, _build_channel_transcripts, _DIGEST_PROMPT,
    CHANNEL_LABELS, _llm_raw, _parse_llm_digest, _looks_like_ops_summary,
    _fallback_multi_summary, _heuristic_actions, _filter_noisy_actions,
    reconcile_and_upsert_tasks, digest_recipient_ids, _format_tg_html,
    _split_chunks,
)
from app.utils import msk_now
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_pre_ping': True}
db.init_app(app)

day = date(2026, 11, 6)
with app.app_context():
    rows = _load_day_messages(day)
    by_ch = _build_channel_transcripts(rows)
    message_count = len(rows)
    blocks = []
    for ch in ('sales', 'hr', 'production', 'itr'):
        if ch in by_ch and by_ch[ch].strip():
            blocks.append(f'=== {CHANNEL_LABELS.get(ch, ch)} ===\n{by_ch[ch]}')
    prompt = _DIGEST_PROMPT.format(
        day=day.isoformat(),
        message_count=message_count,
        transcript_block='\n\n'.join(blocks),
    )
    raw, err = _llm_raw(prompt)
    print('llm_err', err)
    print('raw_len', len(raw or ''))
    if raw and _looks_like_ops_summary(raw):
        summary, actions = _parse_llm_digest(raw)
    else:
        summary = _fallback_multi_summary(day, by_ch, message_count, reason=err or 'fail')
        actions = []
    heur = _heuristic_actions(by_ch)
    if not actions:
        actions = heur
    else:
        actions = list(actions) + [
            h for h in heur
            if (h.get('type') or '') in ('purchase_request', 'potting_report')
        ]
    actions = _filter_noisy_actions(actions)
    print('summary_head:\n', summary[:800])
    print('actions', len(actions))
    if '[авто без LLM]' in summary:
        print('STILL FALLBACK — abort update')
        raise SystemExit(2)

    reconcile = reconcile_and_upsert_tasks(actions, day=day)
    print('reconcile', reconcile)

    existing = OpsDigestLog.query.filter_by(digest_date=day).first()
    actions_json = json.dumps(actions, ensure_ascii=False)[:50000]
    if existing:
        existing.message_count = message_count
        existing.summary_text = summary[:20000]
        existing.actions_json = actions_json
        existing.sent_at = msk_now()
    else:
        db.session.add(OpsDigestLog(
            digest_date=day,
            message_count=message_count,
            summary_text=summary[:20000],
            actions_json=actions_json,
            recipient_ids=','.join(digest_recipient_ids()),
            sent_at=msk_now(),
        ))
    db.session.commit()
    print('DB updated')

    # Try send if bot token present
    token = (os.environ.get('TG_BOT_TOKEN') or '').strip()
    if not token:
        print('no TG_BOT_TOKEN — skip send; deploy + /api/telegram/ops_digest?force=1')
    else:
        from app.telegram import send_chat_message
        payload = _format_tg_html(day, summary, message_count)
        for chat_id in digest_recipient_ids():
            for i, chunk in enumerate(_split_chunks(payload)):
                prefix = f'(часть {i + 1})\n' if i else ''
                ok, e = send_chat_message(chat_id, prefix + chunk)
                print('send', chat_id, ok, e)
