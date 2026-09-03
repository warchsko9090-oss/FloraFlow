"""Проверка антиспама заявок с сайта. Без сети и без БД."""
from __future__ import annotations

import os
import time
import unittest

os.environ['SECRET_KEY'] = 'test-secret'
os.environ['SHOP_ANTISPAM'] = '1'
os.environ['GROQ_API_KEY'] = ''

from app.shop_antispam import heuristic_score, inspect_inquiry, issue_form_token


def _token(age_sec=5):
    return issue_form_token(issued_at=int(time.time()) - age_sec)


class HeuristicCases(unittest.TestCase):
    def test_screenshot_indonesian_dropped(self):
        score, reasons = heuristic_score(
            'Robertfow', '89942842643', 'Hai, saya ingin tahu harga Anda.',
        )
        self.assertGreaterEqual(score, 3, reasons)

    def test_screenshot_names_empty_comment_dropped(self):
        for name in (
            'Robertfow', 'AzinoEn', 'Williamsoary', 'Hiltonmub',
            'industrialzct', 'announcements-voq', 'ofisnaoktdi',
        ):
            score, reasons = heuristic_score(name, '89942842643', '')
            self.assertGreaterEqual(score, 3, f'{name}: {score} {reasons}')

    def test_real_russian_client_allowed(self):
        score, _ = heuristic_score(
            'Иван Петров', '89101234567', 'Нужна туя 2м, перезвоните',
        )
        self.assertLessEqual(score, 0)

    def test_empty_russian_landing_allowed(self):
        score, _ = heuristic_score('тест Влад', '89000000000', '')
        self.assertLessEqual(score, 0)

    def test_english_landscape_allowed(self):
        score, _ = heuristic_score(
            'Joanna Riggs', '+447700900123', 'Need 20 thuja for a garden in Moscow',
        )
        self.assertLessEqual(score, 0)

    def test_common_latin_first_name_not_handle(self):
        score, reasons = heuristic_score('Alexander', '89101234567', '')
        self.assertNotIn('latin-handle', reasons)
        self.assertLess(score, 3)


class InspectInquiry(unittest.TestCase):
    def test_honeypot_drops(self):
        v = inspect_inquiry('Иван', '89101234567', 'перезвоните', honeypot='http://x', form_token=_token())
        self.assertTrue(v.drop)
        self.assertEqual(v.via, 'honeypot')

    def test_missing_token_drops(self):
        v = inspect_inquiry('Иван', '89101234567', 'перезвоните', form_token='')
        self.assertTrue(v.drop)
        self.assertEqual(v.via, 'token')

    def test_too_fast_drops(self):
        v = inspect_inquiry('Иван', '89101234567', 'перезвоните', form_token=issue_form_token())
        self.assertTrue(v.drop)
        self.assertEqual(v.reason, 'too-fast')

    def test_real_client_passes(self):
        v = inspect_inquiry(
            'Иван Петров', '89101234567', 'Нужна туя 2м',
            form_token=_token(),
        )
        self.assertFalse(v.drop)

    def test_bot_from_site_dropped_without_ai(self):
        v = inspect_inquiry(
            'Robertfow', '89942842643', 'Hai, saya ingin tahu harga Anda.',
            form_token=_token(),
        )
        self.assertTrue(v.drop)


if __name__ == '__main__':
    unittest.main()
