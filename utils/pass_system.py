import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import aiosqlite
import pytz

from utils.business import BUSINESSES_CATALOG, PURCHASEABLE_BUSINESS_KEYS, add_business
from utils.case_system import CASE_DEFS, add_user_case
from utils.inventory import add_item
from utils.sqlite_utils import DB_WRITE_LOCK, connect_sqlite

DB_PATH = "database.db"
BALANCES_FILE = "balances.json"
MSK_TZ = pytz.timezone("Europe/Moscow")

CURRENT_PASS_SEASON = {
    "id": "banana_pass_fire_war",
    "title": "BANANA PASS: Сквозь огонь войны",
    "subtitle": "Сезон, посвященный Победе над немецко-фашистскими захватчиками",
    "start": MSK_TZ.localize(datetime(2026, 5, 9, 0, 0, 0)),
    "end": MSK_TZ.localize(datetime(2026, 5, 29, 11, 0, 0)),
    "next_season_title": "Летняя пора",
    "premium_cost_bananas": 5000,
    "xp_per_level": 120,
    "max_level": 45,
    "daily_standard_quests": 10,
    "daily_premium_quests": 20,
    "daily_elite_quests": 5,
    "weekly_quests": 3,
}

SEASON_ITEM_POOL = [
    ("season_collectible", 'Фронтовая каска "Победа"', 0),
    ("season_collectible", 'Полевая фляга "Май 45"', 0),
    ("season_collectible", 'Охотничья панама разведчика', 0),
    ("season_collectible", 'Плащ-палатка командира', 0),
    ("prize_bonus", 'Медаль "За Отвагу" (+100% к /приз)', 100),
    ("prize_bonus", 'Катюша (+300% к /приз)', 300),
    ("prize_bonus", 'Орден Победы (+500% к /приз)', 500),
    ("business_talisman", "Танк Т-34 (+1500% к доходу бизнеса)", 1500),
    ("business_talisman", "Звание Героя Войны (+5000% к доходу бизнеса)", 5000),
]

PASS_QUEST_POOL: Dict[str, Dict] = {
    "claim_prize_1": {"event": "claim_prize", "target": 1, "xp": 40, "title": "Заберите /приз 1 раз", "elite": False},
    "claim_prize_2": {"event": "claim_prize", "target": 2, "xp": 65, "title": "Заберите /приз 2 раза", "elite": False},
    "claim_prize_3": {"event": "claim_prize", "target": 3, "xp": 105, "title": "Заберите /приз 3 раза", "elite": True},
    "open_case_1": {"event": "open_case", "target": 1, "xp": 45, "title": "Откройте 1 кейс", "elite": False},
    "open_case_2": {"event": "open_case", "target": 2, "xp": 75, "title": "Откройте 2 кейса", "elite": False},
    "open_case_4": {"event": "open_case", "target": 4, "xp": 140, "title": "Откройте 4 кейса", "elite": True},
    "casino_play_2": {"event": "casino_play", "target": 2, "xp": 50, "title": "Сыграйте в /казино 2 раза", "elite": False},
    "casino_play_4": {"event": "casino_play", "target": 4, "xp": 85, "title": "Сыграйте в /казино 4 раза", "elite": False},
    "casino_play_7": {"event": "casino_play", "target": 7, "xp": 150, "title": "Сыграйте в /казино 7 раз", "elite": True},
    "duel_play_1": {"event": "duel_play", "target": 1, "xp": 45, "title": "Сыграйте 1 дуэль на 10.000.000₽", "elite": False},
    "duel_play_2": {"event": "duel_play", "target": 2, "xp": 75, "title": "Сыграйте 2 дуэли на 10.000.000₽", "elite": False},
    "duel_play_4": {"event": "duel_play", "target": 4, "xp": 135, "title": "Сыграйте 4 дуэли на 10.000.000₽", "elite": True},
    "duel_create_1": {"event": "duel_play", "target": 1, "xp": 45, "title": "Сыграйте 1 дуэль на 10.000.000₽", "elite": False},
    "duel_create_2": {"event": "duel_play", "target": 2, "xp": 75, "title": "Сыграйте 2 дуэли на 10.000.000₽", "elite": False},
    "duel_create_4": {"event": "duel_play", "target": 4, "xp": 135, "title": "Сыграйте 4 дуэли на 10.000.000₽", "elite": True},
    "business_upgrade_1": {"event": "business_upgrade", "target": 1, "xp": 60, "title": "Улучшите бизнес 1 раз", "elite": False},
    "business_upgrade_2": {"event": "business_upgrade", "target": 2, "xp": 95, "title": "Улучшите бизнес 2 раза", "elite": False},
    "business_upgrade_4": {"event": "business_upgrade", "target": 4, "xp": 160, "title": "Улучшите бизнес 4 раза", "elite": True},
    "collect_business_income_1": {"event": "collect_business_income", "target": 1, "xp": 55, "title": "Соберите доход с бизнесов 1 раз", "elite": False},
    "collect_business_income_3": {"event": "collect_business_income", "target": 3, "xp": 90, "title": "Соберите доход с бизнесов 3 раза", "elite": False},
    "collect_business_income_sum_50000000": {"event": "collect_business_income_money", "target": 50_000_000, "xp": 145, "title": "Соберите доход бизнеса на 50.000.000₽", "elite": False},
    "buy_business_1": {"event": "buy_business", "target": 1, "xp": 70, "title": "Купите 1 бизнес", "elite": False},
    "buy_business_2": {"event": "buy_business", "target": 2, "xp": 130, "title": "Купите 2 бизнеса", "elite": True},
    "buy_bananas_1": {"event": "buy_bananas", "target": 1, "xp": 40, "title": "Купите бананы 1 раз", "elite": False},
    "buy_bananas_3": {"event": "buy_bananas", "target": 3, "xp": 85, "title": "Купите бананы 3 раза", "elite": False},
    "sell_bananas_1": {"event": "sell_bananas", "target": 1, "xp": 45, "title": "Продайте 100+ бананов 1 раз", "elite": False},
    "sell_bananas_3": {"event": "sell_bananas", "target": 3, "xp": 90, "title": "Продайте 100+ бананов 3 раза", "elite": False},
    "transfer_money_1": {"event": "transfer_money", "target": 1, "xp": 35, "title": "Сделайте 1 перевод", "elite": False},
    "transfer_money_4": {"event": "transfer_money", "target": 4, "xp": 95, "title": "Сделайте 4 перевода", "elite": False},
    "charity_1": {"event": "charity", "target": 1, "xp": 40, "title": "Пожертвуйте в /благо 1 раз", "elite": False},
    "charity_3": {"event": "charity", "target": 3, "xp": 100, "title": "Пожертвуйте в /благо 3 раза", "elite": True},
    "apply_item_1": {"event": "apply_item", "target": 1, "xp": 45, "title": "Примените 1 предмет", "elite": False},
    "apply_item_3": {"event": "apply_item", "target": 3, "xp": 110, "title": "Примените 3 предмета", "elite": True},
    "salvage_item_1": {"event": "salvage_item", "target": 1, "xp": 40, "title": "Распылите 1 предмет", "elite": False},
    "salvage_item_3": {"event": "salvage_item", "target": 3, "xp": 95, "title": "Распылите 3 предмета", "elite": False},
    "buy_vip_1": {"event": "buy_vip", "target": 1, "xp": 120, "title": "Купите VIP-статус", "elite": True},
}

WEEKLY_PASS_QUEST_POOL: Dict[str, Dict] = {
    "weekly_collect_business_income_5": {"event": "collect_business_income", "target": 5, "xp": 220, "title": "Соберите доход с бизнесов 5 раз"},
    "weekly_collect_business_income_sum_50000000": {"event": "collect_business_income_money", "target": 50_000_000, "xp": 260, "title": "Соберите доход с бизнесов на 50.000.000₽"},
    "weekly_collect_business_income_sum_100000000": {"event": "collect_business_income_money", "target": 100_000_000, "xp": 340, "title": "Соберите доход с бизнесов на 100.000.000₽"},
    "weekly_duel_play_5": {"event": "duel_play", "target": 5, "xp": 220, "title": "Сыграйте 5 дуэлей на 10.000.000₽"},
    "weekly_open_case_8": {"event": "open_case", "target": 8, "xp": 240, "title": "Откройте 8 кейсов"},
    "weekly_business_upgrade_6": {"event": "business_upgrade", "target": 6, "xp": 260, "title": "Улучшите бизнес 6 раз"},
}


def _generate_standard_rewards() -> Dict[int, Dict]:
    return {
        1: {"type": "vip_days", "days": 7, "title": "VIP на 7 дней"},
        2: {"type": "money", "amount": 1_000_000, "title": "1.000.000₽"},
        3: {"type": "bananas", "amount": 50, "title": "50 бананов"},
        4: {"type": "case", "case_type": "homeless", "title": "Кейс Бомжа"},
        5: {"type": "money", "amount": 1_500_000, "title": "1.500.000₽"},
        6: {"type": "item", "item_type": "season_collectible", "item_name": 'Фронтовая каска "Победа"', "item_value": 0, "title": 'Фронтовая каска "Победа"'},
        7: {"type": "bananas", "amount": 80, "title": "80 бананов"},
        8: {"type": "case", "case_type": "standard", "title": "Стандартный кейс"},
        9: {"type": "money", "amount": 2_000_000, "title": "2.000.000₽"},
        10: {"type": "vip_days", "days": 14, "title": "VIP на 14 дней"},
        11: {"type": "bananas", "amount": 120, "title": "120 бананов"},
        12: {"type": "item", "item_type": "prize_bonus", "item_name": 'Медаль "За Отвагу" (+100% к /приз)', "item_value": 100, "title": 'Медаль "За Отвагу"'},
        13: {"type": "money", "amount": 2_500_000, "title": "2.500.000₽"},
        14: {"type": "case", "case_type": "standard", "title": "Стандартный кейс"},
        15: {"type": "bananas", "amount": 160, "title": "160 бананов"},
        16: {"type": "money", "amount": 3_000_000, "title": "3.000.000₽"},
        17: {"type": "item", "item_type": "season_collectible", "item_name": 'Полевая фляга "Май 45"', "item_value": 0, "title": 'Полевая фляга "Май 45"'},
        18: {"type": "case", "case_type": "standard", "title": "Стандартный кейс"},
        19: {"type": "money", "amount": 3_500_000, "title": "3.500.000₽"},
        20: {"type": "business", "business_key": "grand_espresso", "fallback_money": 2_500_000, "title": "Бизнес ветерана"},
        21: {"type": "bananas", "amount": 220, "title": "220 бананов"},
        22: {"type": "vip_days", "days": 14, "title": "VIP на 14 дней"},
        23: {"type": "money", "amount": 4_500_000, "title": "4.500.000₽"},
        24: {"type": "item", "item_type": "prize_bonus", "item_name": "Катюша (+300% к /приз)", "item_value": 300, "title": "Катюша"},
        25: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        26: {"type": "bananas", "amount": 280, "title": "280 бананов"},
        27: {"type": "money", "amount": 5_500_000, "title": "5.500.000₽"},
        28: {"type": "item", "item_type": "season_collectible", "item_name": "Охотничья панама разведчика", "item_value": 0, "title": "Панама разведчика"},
        29: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        30: {"type": "vip_days", "days": 30, "title": "VIP на 30 дней"},
        31: {"type": "bananas", "amount": 350, "title": "350 бананов"},
        32: {"type": "money", "amount": 7_000_000, "title": "7.000.000₽"},
        33: {"type": "item", "item_type": "business_talisman", "item_name": "Танк Т-34 (+1500% к доходу бизнеса)", "item_value": 1500, "title": "Танк Т-34"},
        34: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        35: {"type": "money", "amount": 8_500_000, "title": "8.500.000₽"},
        36: {"type": "bananas", "amount": 450, "title": "450 бананов"},
        37: {"type": "item", "item_type": "season_collectible", "item_name": "Плащ-палатка командира", "item_value": 0, "title": "Плащ-палатка командира"},
        38: {"type": "money", "amount": 10_000_000, "title": "10.000.000₽"},
        39: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        40: {"type": "business", "business_key": "golden_croissant", "fallback_money": 5_000_000, "title": "Бизнес штаба"},
        41: {"type": "bananas", "amount": 600, "title": "600 бананов"},
        42: {"type": "vip_days", "days": 30, "title": "VIP на 30 дней"},
        43: {"type": "money", "amount": 12_500_000, "title": "12.500.000₽"},
        44: {"type": "item", "item_type": "prize_bonus", "item_name": "Орден Победы (+500% к /приз)", "item_value": 500, "title": "Орден Победы"},
        45: {"type": "business", "business_key": "gourmania", "fallback_money": 9_000_000, "title": "Легендарный бизнес фронта"},
    }


def _generate_premium_rewards() -> Dict[int, Dict]:
    return {
        1: {"type": "money", "amount": 2_000_000, "title": "2.000.000₽"},
        2: {"type": "bananas", "amount": 120, "title": "120 бананов"},
        3: {"type": "vip_days", "days": 14, "title": "VIP на 14 дней"},
        4: {"type": "case", "case_type": "standard", "title": "Стандартный кейс"},
        5: {"type": "item", "item_type": "prize_bonus", "item_name": 'Фронтовой жетон Banana Pass (+75% к /приз)', "item_value": 75, "title": "Фронтовой жетон"},
        6: {"type": "money", "amount": 3_000_000, "title": "3.000.000₽"},
        7: {"type": "bananas", "amount": 180, "title": "180 бананов"},
        8: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        9: {"type": "money", "amount": 4_000_000, "title": "4.000.000₽"},
        10: {"type": "business", "business_key": "fashion_house", "fallback_money": 4_000_000, "title": "Элитный бизнес снабжения"},
        11: {"type": "vip_days", "days": 30, "title": "VIP на 30 дней"},
        12: {"type": "bananas", "amount": 240, "title": "240 бананов"},
        13: {"type": "item", "item_type": "season_collectible", "item_name": "Офицерский планшет Победы", "item_value": 0, "title": "Офицерский планшет"},
        14: {"type": "money", "amount": 5_500_000, "title": "5.500.000₽"},
        15: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        16: {"type": "bananas", "amount": 320, "title": "320 бананов"},
        17: {"type": "item", "item_type": "prize_bonus", "item_name": 'Медаль "За Отвагу" (+100% к /приз)', "item_value": 100, "title": 'Медаль "За Отвагу"'},
        18: {"type": "money", "amount": 7_000_000, "title": "7.000.000₽"},
        19: {"type": "vip_days", "days": 30, "title": "VIP на 30 дней"},
        20: {"type": "business", "business_key": "gourmania", "fallback_money": 7_000_000, "title": "Бизнес командования"},
        21: {"type": "bananas", "amount": 420, "title": "420 бананов"},
        22: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        23: {"type": "money", "amount": 8_500_000, "title": "8.500.000₽"},
        24: {"type": "item", "item_type": "prize_bonus", "item_name": "Катюша (+300% к /приз)", "item_value": 300, "title": "Катюша"},
        25: {"type": "bananas", "amount": 520, "title": "520 бананов"},
        26: {"type": "money", "amount": 10_000_000, "title": "10.000.000₽"},
        27: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        28: {"type": "item", "item_type": "business_talisman", "item_name": "Танк Т-34 (+1500% к доходу бизнеса)", "item_value": 1500, "title": "Танк Т-34"},
        29: {"type": "vip_days", "days": 30, "title": "VIP на 30 дней"},
        30: {"type": "business", "business_key": "global_market", "fallback_money": 10_000_000, "title": "Бизнес тыла"},
        31: {"type": "bananas", "amount": 700, "title": "700 бананов"},
        32: {"type": "money", "amount": 12_000_000, "title": "12.000.000₽"},
        33: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        34: {"type": "item", "item_type": "season_collectible", "item_name": "Парадная фуражка маршала", "item_value": 0, "title": "Парадная фуражка"},
        35: {"type": "money", "amount": 14_000_000, "title": "14.000.000₽"},
        36: {"type": "bananas", "amount": 900, "title": "900 бананов"},
        37: {"type": "vip_days", "days": 90, "title": "VIP на 90 дней"},
        38: {"type": "item", "item_type": "prize_bonus", "item_name": "Орден Победы (+500% к /приз)", "item_value": 500, "title": "Орден Победы"},
        39: {"type": "case", "case_type": "special", "title": "Особый кейс"},
        40: {"type": "business", "business_key": "fuel_giant", "fallback_money": 15_000_000, "title": "Топливная сеть фронта"},
        41: {"type": "bananas", "amount": 1200, "title": "1.200 бананов"},
        42: {"type": "money", "amount": 18_000_000, "title": "18.000.000₽"},
        43: {"type": "item", "item_type": "season_collectible", "item_name": "Знамя Победы Banana Pass", "item_value": 0, "title": "Знамя Победы"},
        44: {"type": "case", "case_type": "victory_day", "title": "Кейс День Победы"},
        45: {"type": "item", "item_type": "business_talisman", "item_name": "Звание Героя Войны (+5000% к доходу бизнеса)", "item_value": 5000, "title": "Звание Героя Войны"},
    }


STANDARD_PASS_REWARDS = _generate_standard_rewards()
PREMIUM_PASS_REWARDS = _generate_premium_rewards()


def _now_msk() -> datetime:
    return datetime.now(MSK_TZ)


def _today_str() -> str:
    return _now_msk().date().isoformat()


def _week_key() -> str:
    now = _now_msk().date()
    iso_year, iso_week, _iso_weekday = now.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _season_end_left_text() -> str:
    delta = CURRENT_PASS_SEASON["end"] - _now_msk()
    if delta.total_seconds() <= 0:
        return "сезон завершён"
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    return f"{delta.days}д. {hours}ч. {minutes}м."


def is_pass_active() -> bool:
    now = _now_msk()
    return CURRENT_PASS_SEASON["start"] <= now <= CURRENT_PASS_SEASON["end"]


def load_balances_data() -> Dict:
    try:
        with open(BALANCES_FILE, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception:
        return {}


def save_balances_data(data: Dict) -> None:
    with open(BALANCES_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def ensure_balance_record(balances: Dict, user_id: int) -> Dict:
    uid = str(user_id)
    if uid not in balances:
        balances[uid] = {
            "wallet": 0,
            "bank": 0,
            "won_total": 0,
            "lost_total": 0,
            "sent_total": 0,
            "received_total": 0,
            "bananas": 0,
            "business_income_today": 0,
            "vip_until": None,
        }
    balances[uid].setdefault("bananas", 0)
    balances[uid].setdefault("vip_until", None)
    return balances[uid]


async def init_pass_schema() -> None:
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS banana_pass_users (
                user_id INTEGER NOT NULL,
                season_id TEXT NOT NULL,
                premium_active INTEGER NOT NULL DEFAULT 0,
                xp INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, season_id)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS banana_pass_claims (
                user_id INTEGER NOT NULL,
                season_id TEXT NOT NULL,
                level INTEGER NOT NULL,
                track TEXT NOT NULL,
                claimed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, season_id, level, track)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS banana_pass_daily_quests (
                user_id INTEGER NOT NULL,
                season_id TEXT NOT NULL,
                quest_date TEXT NOT NULL,
                slot INTEGER NOT NULL,
                quest_id TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                target INTEGER NOT NULL,
                xp_reward INTEGER NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                notified INTEGER NOT NULL DEFAULT 0,
                is_elite INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, season_id, quest_date, slot)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS banana_pass_weekly_quests (
                user_id INTEGER NOT NULL,
                season_id TEXT NOT NULL,
                week_key TEXT NOT NULL,
                slot INTEGER NOT NULL,
                quest_id TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                target INTEGER NOT NULL,
                xp_reward INTEGER NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                notified INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, season_id, week_key, slot)
            )
            """
        )
        columns = {row[1] for row in await (await db.execute("PRAGMA table_info(banana_pass_daily_quests)")).fetchall()}
        if "is_elite" not in columns:
            await db.execute("ALTER TABLE banana_pass_daily_quests ADD COLUMN is_elite INTEGER NOT NULL DEFAULT 0")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_banana_pass_users_season ON banana_pass_users(season_id, user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_banana_pass_daily_date ON banana_pass_daily_quests(user_id, season_id, quest_date)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_banana_pass_weekly_date ON banana_pass_weekly_quests(user_id, season_id, week_key)")
        await db.commit()


async def ensure_pass_user(user_id: int) -> Dict:
    season_id = CURRENT_PASS_SEASON["id"]
    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            await db.execute(
                """
                INSERT INTO banana_pass_users (user_id, season_id, premium_active, xp, updated_at)
                VALUES (?, ?, 0, 0, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id, season_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                """,
                (user_id, season_id),
            )
            await db.commit()
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT user_id, season_id, premium_active, xp FROM banana_pass_users WHERE user_id = ? AND season_id = ?",
                (user_id, season_id),
            )
            row = await cur.fetchone()
            return dict(row) if row else {"user_id": user_id, "season_id": season_id, "premium_active": 0, "xp": 0}
        finally:
            await db.close()


def _split_quest_pool() -> Tuple[List[str], List[str]]:
    elite = [quest_id for quest_id, quest in PASS_QUEST_POOL.items() if quest.get("elite")]
    regular = [quest_id for quest_id, quest in PASS_QUEST_POOL.items() if not quest.get("elite")]
    return regular, elite


def _weekly_quest_ids() -> List[str]:
    return list(WEEKLY_PASS_QUEST_POOL.keys())


async def ensure_daily_quests(user_id: int) -> List[Dict]:
    season_id = CURRENT_PASS_SEASON["id"]
    quest_date = _today_str()
    user_data = await ensure_pass_user(user_id)
    is_premium = bool(int(user_data.get("premium_active", 0) or 0))
    regular_count = CURRENT_PASS_SEASON["daily_premium_quests"] if is_premium else CURRENT_PASS_SEASON["daily_standard_quests"]
    elite_count = CURRENT_PASS_SEASON["daily_elite_quests"]
    regular_pool, elite_pool = _split_quest_pool()

    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT user_id, season_id, quest_date, slot, quest_id, progress, target, xp_reward, completed, notified, is_elite
                FROM banana_pass_daily_quests
                WHERE user_id = ? AND season_id = ? AND quest_date = ?
                ORDER BY slot
                """,
                (user_id, season_id, quest_date),
            )
            rows = [dict(row) for row in await cur.fetchall()]
            expected_count = regular_count + elite_count
            known_daily_ids = set(PASS_QUEST_POOL.keys())
            if len(rows) == expected_count and all(str(row.get("quest_id")) in known_daily_ids for row in rows):
                return rows
            if rows:
                await db.execute(
                    "DELETE FROM banana_pass_daily_quests WHERE user_id = ? AND season_id = ? AND quest_date = ?",
                    (user_id, season_id, quest_date),
                )

            rng = random.Random(f"{season_id}:{user_id}:{quest_date}:{'premium' if is_premium else 'standard'}")
            chosen_regular = rng.sample(regular_pool, k=min(regular_count, len(regular_pool)))
            chosen_elite = rng.sample(elite_pool, k=min(elite_count, len(elite_pool)))
            slot = 1
            for quest_id in chosen_regular:
                quest = PASS_QUEST_POOL[quest_id]
                await db.execute(
                    """
                    INSERT INTO banana_pass_daily_quests
                    (user_id, season_id, quest_date, slot, quest_id, progress, target, xp_reward, completed, notified, is_elite, updated_at)
                    VALUES (?, ?, ?, ?, ?, 0, ?, ?, 0, 0, 0, CURRENT_TIMESTAMP)
                    """,
                    (user_id, season_id, quest_date, slot, quest_id, int(quest["target"]), int(quest["xp"])),
                )
                slot += 1
            for quest_id in chosen_elite:
                quest = PASS_QUEST_POOL[quest_id]
                await db.execute(
                    """
                    INSERT INTO banana_pass_daily_quests
                    (user_id, season_id, quest_date, slot, quest_id, progress, target, xp_reward, completed, notified, is_elite, updated_at)
                    VALUES (?, ?, ?, ?, ?, 0, ?, ?, 0, 0, 1, CURRENT_TIMESTAMP)
                    """,
                    (user_id, season_id, quest_date, slot, quest_id, int(quest["target"]), int(quest["xp"])),
                )
                slot += 1
            await db.commit()
            cur = await db.execute(
                """
                SELECT user_id, season_id, quest_date, slot, quest_id, progress, target, xp_reward, completed, notified, is_elite
                FROM banana_pass_daily_quests
                WHERE user_id = ? AND season_id = ? AND quest_date = ?
                ORDER BY slot
                """,
                (user_id, season_id, quest_date),
            )
            return [dict(row) for row in await cur.fetchall()]
        finally:
            await db.close()


async def ensure_weekly_quests(user_id: int) -> List[Dict]:
    season_id = CURRENT_PASS_SEASON["id"]
    week_key = _week_key()
    weekly_count = int(CURRENT_PASS_SEASON.get("weekly_quests", 0) or 0)
    weekly_pool = _weekly_quest_ids()

    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT user_id, season_id, week_key, slot, quest_id, progress, target, xp_reward, completed, notified
                FROM banana_pass_weekly_quests
                WHERE user_id = ? AND season_id = ? AND week_key = ?
                ORDER BY slot
                """,
                (user_id, season_id, week_key),
            )
            rows = [dict(row) for row in await cur.fetchall()]
            expected_count = min(weekly_count, len(weekly_pool))
            known_weekly_ids = set(WEEKLY_PASS_QUEST_POOL.keys())
            if len(rows) == expected_count and all(str(row.get("quest_id")) in known_weekly_ids for row in rows):
                return rows
            if rows:
                await db.execute(
                    "DELETE FROM banana_pass_weekly_quests WHERE user_id = ? AND season_id = ? AND week_key = ?",
                    (user_id, season_id, week_key),
                )

            rng = random.Random(f"{season_id}:{user_id}:{week_key}:weekly")
            chosen_weekly = rng.sample(weekly_pool, k=expected_count)
            slot = 1
            for quest_id in chosen_weekly:
                quest = WEEKLY_PASS_QUEST_POOL[quest_id]
                await db.execute(
                    """
                    INSERT INTO banana_pass_weekly_quests
                    (user_id, season_id, week_key, slot, quest_id, progress, target, xp_reward, completed, notified, updated_at)
                    VALUES (?, ?, ?, ?, ?, 0, ?, ?, 0, 0, CURRENT_TIMESTAMP)
                    """,
                    (user_id, season_id, week_key, slot, quest_id, int(quest["target"]), int(quest["xp"])),
                )
                slot += 1
            await db.commit()
            cur = await db.execute(
                """
                SELECT user_id, season_id, week_key, slot, quest_id, progress, target, xp_reward, completed, notified
                FROM banana_pass_weekly_quests
                WHERE user_id = ? AND season_id = ? AND week_key = ?
                ORDER BY slot
                """,
                (user_id, season_id, week_key),
            )
            return [dict(row) for row in await cur.fetchall()]
        finally:
            await db.close()


def get_level_from_xp(xp: int) -> int:
    xp_per_level = int(CURRENT_PASS_SEASON["xp_per_level"])
    max_level = int(CURRENT_PASS_SEASON["max_level"])
    return max(1, min(max_level, xp // xp_per_level + 1))


def get_unlocked_level_count(xp: int) -> int:
    xp_per_level = int(CURRENT_PASS_SEASON["xp_per_level"])
    max_level = int(CURRENT_PASS_SEASON["max_level"])
    return max(0, min(max_level, xp // xp_per_level))


def get_reward_track(track: str) -> Dict[int, Dict]:
    return PREMIUM_PASS_REWARDS if track == "premium" else STANDARD_PASS_REWARDS


async def get_claimed_rewards_map(user_id: int) -> Dict[Tuple[int, str], bool]:
    season_id = CURRENT_PASS_SEASON["id"]
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        cur = await db.execute(
            "SELECT level, track FROM banana_pass_claims WHERE user_id = ? AND season_id = ?",
            (user_id, season_id),
        )
        rows = await cur.fetchall()
    return {(int(level), str(track)): True for level, track in rows}


async def get_pass_profile(user_id: int) -> Dict:
    user_data = await ensure_pass_user(user_id)
    quests = await ensure_daily_quests(user_id)
    weekly_quests = await ensure_weekly_quests(user_id)
    xp = int(user_data.get("xp", 0) or 0)
    return {
        "season": CURRENT_PASS_SEASON,
        "xp": xp,
        "level": get_level_from_xp(xp),
        "unlocked_levels": get_unlocked_level_count(xp),
        "premium_active": bool(int(user_data.get("premium_active", 0) or 0)),
        "quests": quests,
        "weekly_quests": weekly_quests,
        "claims": await get_claimed_rewards_map(user_id),
        "active": is_pass_active(),
    }


async def buy_pass_premium(user_id: int) -> Tuple[bool, str]:
    if not is_pass_active():
        return False, "Сейчас сезон BANANA PASS не активен."
    user_data = await ensure_pass_user(user_id)
    if int(user_data.get("premium_active", 0) or 0) == 1:
        return False, "Premium-доступ уже активирован."
    balances = load_balances_data()
    bal = ensure_balance_record(balances, user_id)
    cost = int(CURRENT_PASS_SEASON["premium_cost_bananas"])
    if int(bal.get("bananas", 0) or 0) < cost:
        return False, f"Нужно {cost} бананов для покупки Premium."
    bal["bananas"] = int(bal.get("bananas", 0) or 0) - cost
    save_balances_data(balances)
    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            await db.execute(
                "UPDATE banana_pass_users SET premium_active = 1, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND season_id = ?",
                (user_id, CURRENT_PASS_SEASON["id"]),
            )
            await db.execute(
                "DELETE FROM banana_pass_daily_quests WHERE user_id = ? AND season_id = ? AND quest_date = ?",
                (user_id, CURRENT_PASS_SEASON["id"], _today_str()),
            )
            await db.commit()
        finally:
            await db.close()
    await ensure_daily_quests(user_id)
    return True, f"Premium BANANA PASS активирован за {cost} бананов. Открыто 20 обычных заданий и 5 элитных."


async def award_pass_reward(user_id: int, reward: Dict) -> str:
    reward_type = reward["type"]
    if reward_type == "money":
        balances = load_balances_data()
        bal = ensure_balance_record(balances, user_id)
        bal["wallet"] = int(bal.get("wallet", 0) or 0) + int(reward["amount"])
        save_balances_data(balances)
        return f"{reward['amount']:,}₽".replace(",", ".")
    if reward_type == "bananas":
        balances = load_balances_data()
        bal = ensure_balance_record(balances, user_id)
        bal["bananas"] = int(bal.get("bananas", 0) or 0) + int(reward["amount"])
        save_balances_data(balances)
        return f"{reward['amount']} бананов"
    if reward_type == "vip_days":
        balances = load_balances_data()
        bal = ensure_balance_record(balances, user_id)
        now = datetime.now()
        current_vip = bal.get("vip_until")
        start_dt = now
        if current_vip:
            try:
                vip_dt = datetime.fromisoformat(str(current_vip))
                if vip_dt > now:
                    start_dt = vip_dt
            except Exception:
                pass
        bal["vip_until"] = (start_dt + timedelta(days=int(reward["days"]))).isoformat()
        save_balances_data(balances)
        return f"VIP на {reward['days']} дней"
    if reward_type == "case":
        case_id = await add_user_case(user_id, reward["case_type"])
        case_name = CASE_DEFS.get(reward["case_type"], {}).get("name", reward["case_type"])
        return f"Кейс «{case_name}» #{case_id}"
    if reward_type == "item":
        await add_item(user_id, reward["item_type"], reward["item_name"], int(reward.get("item_value", 0) or 0))
        return reward["item_name"]
    if reward_type == "business":
        if reward["business_key"] in BUSINESSES_CATALOG:
            try:
                await add_business(user_id, reward["business_key"])
                return BUSINESSES_CATALOG[reward["business_key"]]["name"]
            except ValueError:
                pass
        balances = load_balances_data()
        bal = ensure_balance_record(balances, user_id)
        bal["wallet"] = int(bal.get("wallet", 0) or 0) + int(reward.get("fallback_money", 0) or 0)
        save_balances_data(balances)
        return f"{int(reward.get('fallback_money', 0)):,}₽".replace(",", ".")
    return reward.get("title", "Награда")


async def claim_pass_reward(user_id: int, track: str) -> Tuple[bool, str]:
    if track not in {"standard", "premium"}:
        return False, "Неизвестный тип награды."
    profile = await get_pass_profile(user_id)
    if not profile["active"]:
        return False, "Сезон BANANA PASS сейчас не активен."
    if track == "premium" and not profile["premium_active"]:
        return False, "Premium-доступ не активирован."
    reward_map = get_reward_track(track)
    claims = profile["claims"]
    unlocked_levels = int(profile["unlocked_levels"])
    for level in range(1, unlocked_levels + 1):
        if (level, track) in claims:
            continue
        reward = reward_map.get(level)
        if not reward:
            continue
        reward_title = await award_pass_reward(user_id, reward)
        async with DB_WRITE_LOCK:
            db = await connect_sqlite()
            try:
                await db.execute(
                    "INSERT INTO banana_pass_claims (user_id, season_id, level, track, claimed_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    (user_id, CURRENT_PASS_SEASON["id"], level, track),
                )
                await db.commit()
            finally:
                await db.close()
        return True, f"Получена награда {track.title()} за уровень {level}: {reward_title}"
    return False, "Сейчас нет доступных наград для получения."


async def record_pass_progress(user_id: int, event_type: str, amount: int = 1, bot=None) -> List[str]:
    if not is_pass_active() or amount <= 0:
        return []
    quests = await ensure_daily_quests(user_id)
    completed_messages: List[str] = []
    xp_total = 0
    season_id = CURRENT_PASS_SEASON["id"]
    quest_date = _today_str()

    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            for quest in quests:
                quest_meta = PASS_QUEST_POOL.get(str(quest["quest_id"]))
                if not quest_meta or quest_meta["event"] != event_type or int(quest.get("completed", 0) or 0) == 1:
                    continue
                new_progress = min(int(quest["target"]), int(quest["progress"]) + int(amount))
                completed = 1 if new_progress >= int(quest["target"]) else 0
                await db.execute(
                    """
                    UPDATE banana_pass_daily_quests
                    SET progress = ?, completed = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND season_id = ? AND quest_date = ? AND slot = ?
                    """,
                    (new_progress, completed, user_id, season_id, quest_date, int(quest["slot"])),
                )
                if completed:
                    xp_total += int(quest["xp_reward"])
                    quest_type_label = "ЭЛИТНЫЙ" if int(quest.get("is_elite", 0) or 0) == 1 else "ЕЖЕДНЕВНЫЙ"
                    completed_messages.append(
                        f"✅ BANANA PASS: {quest_type_label} КВЕСТ ВЫПОЛНЕН\n{quest_meta['title']}\nНаграда: {quest['xp_reward']} XP"
                    )
            if xp_total > 0:
                await db.execute(
                    "UPDATE banana_pass_users SET xp = xp + ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND season_id = ?",
                    (xp_total, user_id, season_id),
                )
            await db.commit()
        finally:
            await db.close()

    if bot and completed_messages:
        for text in completed_messages:
            try:
                await bot.api.messages.send(peer_id=user_id, random_id=0, message=text)
            except Exception:
                pass
    return completed_messages


def format_reward_short(track: str, level: int) -> str:
    reward = get_reward_track(track).get(level)
    return reward.get("title", "—") if reward else "—"


async def build_pass_text(user_id: int) -> str:
    profile = await get_pass_profile(user_id)
    xp = int(profile["xp"])
    level = int(profile["level"])
    unlocked = int(profile["unlocked_levels"])
    xp_per_level = int(CURRENT_PASS_SEASON["xp_per_level"])
    progress_in_level = xp % xp_per_level
    claims = profile["claims"]
    standard_available = sum(1 for lvl in range(1, unlocked + 1) if lvl in STANDARD_PASS_REWARDS and (lvl, "standard") not in claims)
    premium_available = sum(1 for lvl in range(1, unlocked + 1) if lvl in PREMIUM_PASS_REWARDS and (lvl, "premium") not in claims)
    premium_status = "???????" if profile["premium_active"] else f"?? ??????? ({CURRENT_PASS_SEASON['premium_cost_bananas']} ???????)"
    lines = [
        f"🍌 {CURRENT_PASS_SEASON['title']}",
        CURRENT_PASS_SEASON["subtitle"],
        "",
        f"🕯 До конца сезона: {_season_end_left_text()}",
        f"🏁 Уровень пропуска: {level}/{CURRENT_PASS_SEASON['max_level']}",
        f"⭐ XP: {xp}",
        f"📈 Прогресс уровня: {progress_in_level}/{xp_per_level}",
        f"🎟 Standart: активен",
        f"?? Premium: {premium_status}",
        f"🧭 Следующий сезон: {CURRENT_PASS_SEASON['next_season_title']}",
        "",
        f"🎁 Можно забрать Standard: {standard_available}",
        f"🎁 Можно забрать Premium: {premium_available}",
        "",
        "Ближайшие уровни:",
    ]
    preview_start = max(1, level)
    preview_end = min(CURRENT_PASS_SEASON["max_level"], preview_start + 5)
    for lvl in range(preview_start, preview_end + 1):
        lines.append(f"{lvl}. S: {format_reward_short('standard', lvl)} | P: {format_reward_short('premium', lvl)}")
    lines.append("")
    lines.append("Команды: /pass, /квесты, /pass premium, /pass claim")
    return "\n".join(lines)


async def build_quests_text(user_id: int) -> str:
    profile = await get_pass_profile(user_id)
    regular = [quest for quest in profile["quests"] if int(quest.get("is_elite", 0) or 0) == 0]
    elite = [quest for quest in profile["quests"] if int(quest.get("is_elite", 0) or 0) == 1]
    weekly = await ensure_weekly_quests(user_id)
    completed_regular = sum(1 for quest in regular if int(quest.get("completed", 0) or 0) == 1)
    completed_elite = sum(1 for quest in elite if int(quest.get("completed", 0) or 0) == 1)
    completed_weekly = sum(1 for quest in weekly if int(quest.get("completed", 0) or 0) == 1)

    now = _now_msk()
    next_day = now.date() + timedelta(days=1)
    daily_refresh_at = MSK_TZ.localize(datetime.combine(next_day, time.min))
    daily_delta = daily_refresh_at - now
    daily_hours = max(0, int(daily_delta.total_seconds())) // 3600
    daily_minutes = (max(0, int(daily_delta.total_seconds())) % 3600) // 60
    daily_refresh_info = f"{daily_refresh_at.strftime('%d.%m %H:%M')} МСК (через {daily_hours}ч. {daily_minutes}м.)"

    days_until_monday = (7 - now.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    next_monday = now.date() + timedelta(days=days_until_monday)
    weekly_refresh_at = MSK_TZ.localize(datetime.combine(next_monday, time.min))
    weekly_delta = weekly_refresh_at - now
    weekly_days = max(0, int(weekly_delta.total_seconds())) // 86400
    weekly_hours = (max(0, int(weekly_delta.total_seconds())) % 86400) // 3600
    weekly_minutes = (max(0, int(weekly_delta.total_seconds())) % 3600) // 60
    weekly_refresh_info = f"{weekly_refresh_at.strftime('%d.%m %H:%M')} МСК (через {weekly_days}д. {weekly_hours}ч. {weekly_minutes}м.)"

    lines = [
        "🗂 Задания BANANA PASS",
        f"📅 Сегодня: {_today_str()}",
        f"🕯 До конца сезона: {_season_end_left_text()}",
        "",
        f"🎟 Обычные задания: {completed_regular}/{len(regular)}",
        f"⏱ Обновление: {daily_refresh_info}",
    ]
    for quest in regular:
        meta = PASS_QUEST_POOL.get(str(quest["quest_id"]), {})
        done = "✅" if int(quest.get("completed", 0) or 0) == 1 else "⏳"
        lines.append(f"{done} {meta.get('title', quest['quest_id'])} | {quest['progress']}/{quest['target']} | {quest['xp_reward']} XP")

    lines.append("")
    lines.append(f"🔥 Элитные задания: {completed_elite}/{len(elite)}")
    lines.append(f"⏱ Обновление: {daily_refresh_info}")
    for quest in elite:
        meta = PASS_QUEST_POOL.get(str(quest["quest_id"]), {})
        done = "✅" if int(quest.get("completed", 0) or 0) == 1 else "🔥"
        lines.append(f"{done} {meta.get('title', quest['quest_id'])} | {quest['progress']}/{quest['target']} | {quest['xp_reward']} XP")

    if weekly:
        lines.append("")
        lines.append(f"📆 Недельные задания: {completed_weekly}/{len(weekly)}")
        lines.append(f"⏱ Обновление: {weekly_refresh_info}")
        for quest in weekly:
            meta = WEEKLY_PASS_QUEST_POOL.get(str(quest["quest_id"]), {})
            done = "✅" if int(quest.get("completed", 0) or 0) == 1 else "◆"
            lines.append(f"{done} {meta.get('title', quest['quest_id'])} | {quest['progress']}/{quest['target']} | {quest['xp_reward']} XP")
    return "\n".join(lines)
