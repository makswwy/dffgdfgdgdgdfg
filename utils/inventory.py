from typing import List, Dict, Optional
import aiosqlite
from utils.sqlite_utils import DB_WRITE_LOCK, connect_sqlite

DB_PATH = "database.db"


def _repair_mojibake_text(text: str) -> str:
    value = str(text or "")
    suspicious_markers = ("Р", "С", "Ð", "Ñ")
    if not any(marker in value for marker in suspicious_markers):
        return value

    candidates = [value]
    for encoding in ("latin1", "cp1252"):
        for candidate in list(candidates):
            try:
                repaired = candidate.encode(encoding).decode("utf-8")
                if repaired not in candidates:
                    candidates.append(repaired)
            except Exception:
                pass

    best = value
    best_score = value.count("Р") + value.count("С") + value.count("Ð") + value.count("Ñ")
    for candidate in candidates[1:]:
        score = candidate.count("Р") + candidate.count("С") + candidate.count("Ð") + candidate.count("Ñ")
        if score < best_score:
            best = candidate
            best_score = score
    return best


def _canonical_item_name(item_type: str, item_name: str, item_value: int) -> str:
    name_lower = item_name.lower()

    if item_type == "business_talisman":
        if item_value >= 5000 or "героя" in name_lower:
            return "Звание Героя Войны (+5000% к доходу бизнеса)"
        if item_value >= 1500 or "т-34" in name_lower:
            return "Танк Т-34 (+1500% к доходу бизнеса)"
        return 'Талисман "Золотой Телец" (+500% к доходу бизнеса)'

    if item_type == "prize_bonus":
        if item_value >= 500:
            if "орден" in name_lower or "побед" in name_lower:
                return "Орден Победы (+500% к доходу /приз)"
            return "Орден Победы (+500% к доходу /приз)"
        if item_value >= 300:
            return "Катюша (+300% к доходу /приз)"
        if item_value >= 100:
            return 'Медаль "За Отвагу" (+100% к доходу /приз)'
        if item_value >= 50:
            return "Легендарный предмет (+50% к доходу /приз)"
        if item_value >= 25:
            return "Эпический предмет (+25% к доходу /приз)"
        if item_value >= 10:
            return "Редкий предмет (+10% к доходу /приз)"

    return item_name


def _normalize_inventory_item(row: Dict) -> Dict:
    item = dict(row)
    item_type = str(item.get("item_type", "") or "")
    item_value = int(item.get("item_value", 0) or 0)
    repaired_name = _repair_mojibake_text(str(item.get("item_name", "") or ""))
    item["item_name"] = _canonical_item_name(item_type, repaired_name, item_value)
    return item


def normalize_public_item(item: Dict) -> Dict:
    return _normalize_inventory_item(item)


async def get_inventory(user_id: int) -> List[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, item_type, item_name, item_value FROM inventory WHERE user_id = ? ORDER BY id",
            (user_id,),
        )
        rows = await cur.fetchall()
        return [_normalize_inventory_item(dict(row)) for row in rows]


async def add_item(user_id: int, item_type: str, item_name: str, item_value: int = 0) -> None:
    clean_name = _repair_mojibake_text(item_name)
    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            await db.execute(
                "INSERT INTO inventory (user_id, item_type, item_name, item_value) VALUES (?, ?, ?, ?)",
                (user_id, item_type, clean_name, item_value),
            )
            await db.commit()
        finally:
            await db.close()


async def get_item_by_id(user_id: int, item_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, item_type, item_name, item_value FROM inventory WHERE user_id = ? AND id = ?",
            (user_id, item_id),
        )
        row = await cur.fetchone()
        return _normalize_inventory_item(dict(row)) if row else None


async def remove_item(user_id: int, item_id: int) -> None:
    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            await db.execute("DELETE FROM inventory WHERE user_id = ? AND id = ?", (user_id, item_id))
            await db.commit()
        finally:
            await db.close()


async def take_item_by_id(user_id: int, item_id: int) -> Optional[Dict]:
    item = await get_item_by_id(user_id, item_id)
    if not item:
        return None
    await remove_item(user_id, item_id)
    return item


async def apply_item_effect(user_id: int, item: Dict) -> int:
    bonus = int(item.get("item_value", 0))
    if bonus <= 0:
        return 0
    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            cur = await db.execute(
                "SELECT prize_bonus_percent FROM user_effects WHERE user_id = ?",
                (user_id,),
            )
            row = await cur.fetchone()
            current = int(row[0]) if row else 0
            updated = current + bonus
            await db.execute(
                "INSERT INTO user_effects (user_id, prize_bonus_percent) VALUES (?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET prize_bonus_percent = excluded.prize_bonus_percent, updated_at = CURRENT_TIMESTAMP",
                (user_id, updated),
            )
            await db.commit()
        finally:
            await db.close()
    return updated


async def get_prize_bonus_percent(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT prize_bonus_percent FROM user_effects WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row else 0
