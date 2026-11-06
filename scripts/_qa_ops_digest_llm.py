"""Reproduce today's ops digest LLM call locally."""
import os
import sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# keys
for name in ('gemini.env', 'groq.env', 'backup-pg.env'):
    p = Path.home() / '.floraflow' / name
    if not p.exists():
        continue
    for line in p.read_text(encoding='utf-8').splitlines():
        if '=' in line and not line.strip().startswith('#'):
            k, v = line.split('=', 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k == 'FLORAFLOW_BACKUP_DATABASE_URL':
                os.environ['DATABASE_URL'] = v
            elif k and k not in os.environ:
                os.environ[k] = v

print('GEMINI', bool(os.environ.get('GEMINI_API_KEY')))
print('GROQ', bool(os.environ.get('GROQ_API_KEY')))
print('DB', bool(os.environ.get('DATABASE_URL')))

# Avoid long migrations hanging: set a short statement timeout via URL? skip create_app migrations by using direct import after minimal flask

from flask import Flask
from app.models import db
from app import models  # noqa
from app.ai_ops import (
    _load_day_messages, _build_channel_transcripts, _DIGEST_PROMPT,
    _DIGEST_SYSTEM, _llm_raw, CHANNEL_LABELS, _parse_llm_digest,
)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_pre_ping': True}
db.init_app(app)

day = date(2026, 11, 6)
with app.app_context():
    rows = _load_day_messages(day)
    by_ch = _build_channel_transcripts(rows)
    print('rows', len(rows), 'channels', {k: len(v) for k, v in by_ch.items()})
    blocks = []
    for ch in ('sales', 'hr', 'production', 'itr'):
        if ch in by_ch and by_ch[ch].strip():
            blocks.append(f'=== {CHANNEL_LABELS.get(ch, ch)} ===\n{by_ch[ch]}')
    transcript_block = '\n\n'.join(blocks)
    prompt = _DIGEST_PROMPT.format(
        day=day.isoformat(),
        message_count=len(rows),
        transcript_block=transcript_block,
    )
    print('prompt_chars', len(prompt))

    # Try providers with explicit errors
    from app.ai_ops import _digest_provider_order
    print('providers', _digest_provider_order())

    raw = _llm_raw(prompt)
    print('raw_len', len(raw or ''))
    print('raw_head', (raw or '')[:500])
    if raw:
        summary, actions = _parse_llm_digest(raw)
        print('summary_head', summary[:400])
        print('actions', len(actions))
