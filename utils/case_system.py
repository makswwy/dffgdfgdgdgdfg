import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiosqlite

from utils.business import BUSINESSES_CATALOG, PURCHASEABLE_BUSINESS_KEYS, add_business
from utils.inventory import add_item
from utils.sqlite_utils import DB_WRITE_LOCK, connect_sqlite

DB_PATH = "database.db"

CASE_DEFS = {
    "daily": {"name": "Ежедневный кейс", "money_cost": 0, "banana_cost": 0, "daily": True},
    "homeless": {"name": "Кейс Бомжа", "money_cost": 7_500_000, "banana_cost": 0, "daily": False},
    "standard": {"name": "Стандартный кейс", "money_cost": 30_000_000, "banana_cost": 0, "daily": False},
    "special": {"name": "Особый кейс", "money_cost": 0, "banana_cost": 3_000, "daily": False},
    "victory_day": {"name": "Кейс День Победы", "money_cost": 225_000_000, "banana_cost": 1_500, "daily": False},
}

CASE_CHANCES = {
    "daily": [
        ("Игровая валюта (150.000-1.500.000₽)", 92.0),
        ("VIP на 1 день", 4.0),
        ("Редкий предмет (+10% к доходу /приз)", 4.0),
    ],
    "homeless": [
        ("Игровая валюта (750.000-6.000.000₽)", 94.0),
        ("Случайный бизнес из 1-3 бизнесов списка", 3.0),
        ("VIP на 7 дней", 1.5),
        ("Редкий предмет (+10% к доходу /приз)", 1.3),
        ("Эпический предмет (+25% к доходу /приз)", 0.2),
    ],
    "standard": [
        ("Игровая валюта (4.500.000-60.000.000₽)", 85.0),
        ("Бананы (150-1.500)", 5.0),
        ("Случайный бизнес из 4-6 бизнесов списка", 4.0),
        ("VIP на 30 дней", 2.0),
        ("Редкий предмет (+10% к доходу /приз)", 1.8),
        ("Эпический предмет (+25% к доходу /приз)", 1.2),
        ("Легендарный предмет (+50% к доходу /приз)", 0.7),
        ('Талисман "Золотой Телец" (+500% к доходу бизнеса)', 0.3),
    ],
    "special": [
        ("Игровая валюта (150.000.000-1.500.000.000₽)", 84.0),
        ("Случайный бизнес из 12-19 бизнесов списка", 6.0),
        ("VIP на 90 дней", 3.0),
        ("Редкий предмет (+10% к доходу /приз)", 2.0),
        ("Эпический предмет (+25% к доходу /приз)", 2.0),
        ("Легендарный предмет (+50% к доходу /приз)", 1.5),
        ('Талисман "Золотой Телец" (+500% к доходу бизнеса)', 1.5),
    ],
    "victory_day": [
        ("Игровая валюта (75.000.000-750.000.000₽)", 60.0),
        ("Бананы (750-6.000)", 15.0),
        ("Случайный бизнес из 9-15 бизнесов списка", 5.0),
        ("Катюша (+300% к доходу /приз)", 5.0),
        ('Медаль "За Отвагу" (+100% к доходу /приз)', 4.0),
        ("Орден Победы (+500% к доходу /приз)", 3.0),
        ("Танк Т-34 (+1500% к доходу бизнеса)", 2.5),
        ("Звание Героя Войны (+5000% к доходу бизнеса)", 0.5),
        ("VIP на 30 дней", 5.0),
    ],
}


def _format_percent(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else str(value).rstrip("0").rstrip(".")


def _build_case_chances_text() -> str:
    lines = ["Шансы выпадения по кейсам:"]
    ordered_case_types = ["daily", "homeless", "standard", "special", "victory_day"]
    for index, case_type in enumerate(ordered_case_types, start=1):
        lines.append("")
        lines.append(f"{index}. {CASE_DEFS[case_type]['name']}")
        for label, chance in CASE_CHANCES[case_type]:
            lines.append(f"- {label} — {_format_percent(chance)}%")
    return "\n".join(lines)


CASE_CHANCES_TEXT = _build_case_chances_text()


def _business_reward_text(business_key: str) -> str:
    business_meta = BUSINESSES_CATALOG.get(business_key, {})
    return str(business_meta.get("name", business_key))


def _get_case_business_keys(case_type: str) -> List[str]:
    business_ranges = {
        "homeless": (0, 3),
        "standard": (3, 6),
        "special": (11, 19),
        "victory_day": (8, 15),
    }
    start, end = business_ranges.get(case_type, (0, len(PURCHASEABLE_BUSINESS_KEYS)))
    selected = PURCHASEABLE_BUSINESS_KEYS[start:end]
    return [key for key in selected if key in BUSINESSES_CATALOG]


def _pick_weighted_business(case_type: str) -> Dict:
    available_keys = _get_case_business_keys(case_type)
    if not available_keys:
        available_keys = [key for key in PURCHASEABLE_BUSINESS_KEYS if key in BUSINESSES_CATALOG]

    max_price = max(
        max(1, int(BUSINESSES_CATALOG.get(key, {}).get("price", 1) or 1))
        for key in available_keys
    )
    weights = []
    for key in available_keys:
        price = max(1, int(BUSINESSES_CATALOG.get(key, {}).get("price", 1) or 1))
        weight = max(1, int(max_price / price))
        weights.append(max(1, weight))

    selected_key = random.choices(available_keys, weights=weights, k=1)[0]
    return {"type": "business", "business_key": selected_key, "text": _business_reward_text(selected_key)}


def _pick_case_reward(case_type: str) -> Dict:
    roll = random.random() * 100

    if case_type == "daily":
        if roll < 92:
            return {"type": "money", "amount": random.randint(150_000, 1_500_000), "text": "Игровая валюта"}
        if roll < 96:
            return {"type": "vip_days", "days": 1, "text": "VIP на 1 день"}
        return {"type": "item", "item_type": "prize_bonus", "name": "Редкий предмет (+10% к доходу /приз)", "value": 10}

    if case_type == "homeless":
        if roll < 94:
            return {"type": "money", "amount": random.randint(750_000, 6_000_000), "text": "Игровая валюта"}
        if roll < 97:
            return _pick_weighted_business(case_type)
        if roll < 98.5:
            return {"type": "vip_days", "days": 7, "text": "VIP на 7 дней"}
        if roll < 99.8:
            return {"type": "item", "item_type": "prize_bonus", "name": "Редкий предмет (+10% к доходу /приз)", "value": 10}
        return {"type": "item", "item_type": "prize_bonus", "name": "Эпический предмет (+25% к доходу /приз)", "value": 25}

    if case_type == "standard":
        if roll < 85:
            return {"type": "money", "amount": random.randint(4_500_000, 60_000_000), "text": "Игровая валюта"}
        if roll < 90:
            return {"type": "bananas", "amount": random.randint(150, 1_500), "text": "Бананы"}
        if roll < 94:
            return _pick_weighted_business(case_type)
        if roll < 96:
            return {"type": "vip_days", "days": 30, "text": "VIP на 30 дней"}
        if roll < 97.8:
            return {"type": "item", "item_type": "prize_bonus", "name": "Редкий предмет (+10% к доходу /приз)", "value": 10}
        if roll < 99.0:
            return {"type": "item", "item_type": "prize_bonus", "name": "Эпический предмет (+25% к доходу /приз)", "value": 25}
        if roll < 99.7:
            return {"type": "item", "item_type": "prize_bonus", "name": "Легендарный предмет (+50% к доходу /приз)", "value": 50}
        return {"type": "item", "item_type": "business_talisman", "name": 'Талисман "Золотой Телец" (+500% к доходу бизнеса)', "value": 500}

    if case_type == "victory_day":
        if roll < 60:
            return {"type": "money", "amount": random.randint(75_000_000, 750_000_000), "text": "Игровая валюта"}
        if roll < 75:
            return {"type": "bananas", "amount": random.randint(750, 6_000), "text": "Бананы"}
        if roll < 80:
            return _pick_weighted_business(case_type)
        if roll < 85:
            return {"type": "item", "item_type": "prize_bonus", "name": "Катюша (+300% к доходу /приз)", "value": 300}
        if roll < 89:
            return {"type": "item", "item_type": "prize_bonus", "name": 'Медаль "За Отвагу" (+100% к доходу /приз)', "value": 100}
        if roll < 92:
            return {"type": "item", "item_type": "prize_bonus", "name": "Орден Победы (+500% к доходу /приз)", "value": 500}
        if roll < 94.5:
            return {"type": "item", "item_type": "business_talisman", "name": "Танк Т-34 (+1500% к доходу бизнеса)", "value": 1500}
        if roll < 95:
            return {"type": "item", "item_type": "business_talisman", "name": "Звание Героя Войны (+5000% к доходу бизнеса)", "value": 5000}
        return {"type": "vip_days", "days": 30, "text": "VIP на 30 дней"}

    if roll < 84:
        return {"type": "money", "amount": random.randint(150_000_000, 1_500_000_000), "text": "Игровая валюта"}
    if roll < 90:
        return _pick_weighted_business(case_type)
    if roll < 93:
        return {"type": "vip_days", "days": 90, "text": "VIP на 90 дней"}
    if roll < 95:
        return {"type": "item", "item_type": "prize_bonus", "name": "Редкий предмет (+10% к доходу /приз)", "value": 10}
    if roll < 97:
        return {"type": "item", "item_type": "prize_bonus", "name": "Эпический предмет (+25% к доходу /приз)", "value": 25}
    if roll < 98.5:
        return {"type": "item", "item_type": "prize_bonus", "name": "Легендарный предмет (+50% к доходу /приз)", "value": 50}
    return {"type": "item", "item_type": "business_talisman", "name": 'Талисман "Золотой Телец" (+500% к доходу бизнеса)', "value": 500}


async def get_daily_remaining(user_id: int) -> Optional[timedelta]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT created_at FROM cases_log WHERE user_id = ? AND case_type = 'daily' ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    last = datetime.fromisoformat(str(row[0]).replace(" ", "T"))
    next_time = last + timedelta(days=1)
    now = datetime.now()
    if now >= next_time:
        return None
    return next_time - now


async def log_case_open(user_id: int, case_type: str, reward_type: str, reward_value: str) -> None:
    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            await db.execute(
                "INSERT INTO cases_log (user_id, case_type, reward_type, reward_value) VALUES (?, ?, ?, ?)",
                (user_id, case_type, reward_type, reward_value),
            )
            await db.commit()
        finally:
            await db.close()


async def add_user_case(user_id: int, case_type: str) -> int:
    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            cur = await db.execute(
                "INSERT INTO user_cases (user_id, case_type) VALUES (?, ?)",
                (user_id, case_type),
            )
            await db.commit()
            return int(cur.lastrowid)
        finally:
            await db.close()


async def get_user_cases(user_id: int) -> List[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, case_type, created_at FROM user_cases WHERE user_id = ? ORDER BY id",
            (user_id,),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["meta"] = CASE_DEFS.get(row["case_type"], {"name": row["case_type"]})
    return rows


async def get_opened_cases_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM cases_log WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def get_user_case_by_id(user_id: int, case_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, case_type, created_at FROM user_cases WHERE user_id = ? AND id = ?",
            (user_id, case_id),
        )
        row = await cur.fetchone()
    if not row:
        return None
    result = dict(row)
    result["meta"] = CASE_DEFS.get(result["case_type"], {"name": result["case_type"]})
    return result


async def remove_user_case(user_id: int, case_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM user_cases WHERE user_id = ? AND id = ?", (user_id, case_id))
        await db.commit()


async def open_case(case_type: str, user_id: int) -> Tuple[Dict, str]:
    reward = _pick_case_reward(case_type)
    message = ""
    if reward["type"] == "item":
        await add_item(user_id, reward["item_type"], reward["name"], reward["value"])
        message = reward["name"]
    elif reward["type"] == "business":
        try:
            await add_business(user_id, reward["business_key"])
            message = reward["text"]
        except ValueError:
            fallback_amount = int(BUSINESSES_CATALOG.get(reward["business_key"], {}).get("price", 0) or 0)
            reward = {"type": "money", "amount": fallback_amount, "text": "Игровая валюта"}
            message = f'{fallback_amount:,}₽'.replace(",", ".")
    elif reward["type"] == "money":
        message = f'{reward["amount"]:,}₽'.replace(",", ".")
    elif reward["type"] == "bananas":
        message = f'{reward["amount"]:,} бананов'.replace(",", ".")
    elif reward["type"] == "vip_days":
        message = reward["text"]
    await log_case_open(user_id, case_type, reward["type"], message)
    return reward, message
