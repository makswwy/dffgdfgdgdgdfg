from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import aiosqlite
from utils.sqlite_utils import DB_WRITE_LOCK, connect_sqlite

DB_PATH = "database.db"
BUSINESS_COLLECT_COOLDOWN = timedelta(hours=24)
MAX_BRANCHES_PER_BUSINESS = 1000

BUSINESSES_CATALOG: Dict[str, Dict] = {
    "grand_espresso": {
        "name": 'Комиссионный дом "Второй Шанс"',
        "price": 1_000_000,
        "base_income": 275_000,
        "income_min": 250_000,
        "income_max": 300_000,
    },
    "golden_croissant": {
        "name": 'Бистро "На Ходу"',
        "price": 3_000_000,
        "base_income": 400_000,
        "income_min": 300_000,
        "income_max": 500_000,
    },
    "fashion_house": {
        "name": 'Бутик "Северный Лоск"',
        "price": 6_000_000,
        "base_income": 550_000,
        "income_min": 500_000,
        "income_max": 600_000,
    },
    "gourmania": {
        "name": 'Ресторан "Лунный Берег"',
        "price": 13_000_000,
        "base_income": 1_500_000,
        "income_min": 1_000_000,
        "income_max": 2_000_000,
    },
    "global_market": {
        "name": 'Маркет "Круглые Сутки"',
        "price": 20_000_000,
        "base_income": 2_500_000,
        "income_min": 2_000_000,
        "income_max": 3_000_000,
    },
    "fuel_giant": {
        "name": 'Сеть АЗС "Импульс Ойл"',
        "price": 75_000_000,
        "base_income": 9_500_000,
        "income_min": 8_000_000,
        "income_max": 11_000_000,
    },
    "neboskreb": {
        "name": 'Девелоперская группа "Монолит Вектор"',
        "price": 200_000_000,
        "base_income": 22_500_000,
        "income_min": 20_000_000,
        "income_max": 25_000_000,
    },
    "imax_empire": {
        "name": 'Клуб развлечений "Золотая Фишка"',
        "price": 300_000_000,
        "base_income": 37_500_000,
        "income_min": 35_000_000,
        "income_max": 40_000_000,
    },
    "worldwide_holdings": {
        "name": 'Инвестхолдинг "Союз Капитал"',
        "price": 500_000_000,
        "base_income": 77_500_000,
        "income_min": 70_000_000,
        "income_max": 85_000_000,
    },
    "powercore": {
        "name": 'Биобанк "Helix Nova"',
        "price": 650_000_000,
        "base_income": 95_000_000,
        "income_min": 90_000_000,
        "income_max": 100_000_000,
    },
    "cybersoft": {
        "name": 'Верфь "Ocean Matrix"',
        "price": 750_000_000,
        "base_income": 120_000_000,
        "income_min": 110_000_000,
        "income_max": 130_000_000,
    },
    "neotech": {
        "name": 'Аэрокосмический концерн "Orion Dynamics"',
        "price": 1_000_000_000,
        "base_income": 165_000_000,
        "income_min": 150_000_000,
        "income_max": 180_000_000,
    },
    "aurora_motors": {
        "name": 'Премиум-автоконцерн "Aurora Motors"',
        "price": 60_000_000_000,
        "base_income": 13_500_000_000,
        "income_min": 12_500_000_000,
        "income_max": 14_500_000_000,
    },
    "skyline_resort": {
        "name": 'Сеть люкс-курортов "Skyline Resort"',
        "price": 120_000_000_000,
        "base_income": 27_000_000_000,
        "income_min": 25_000_000_000,
        "income_max": 29_000_000_000,
    },
    "quantum_labs": {
        "name": 'Технопарк "Quantum Labs"',
        "price": 240_000_000_000,
        "base_income": 54_000_000_000,
        "income_min": 50_000_000_000,
        "income_max": 58_000_000_000,
    },
    "imperial_shipyards": {
        "name": 'Судостроительный холдинг "Imperial Shipyards"',
        "price": 500_000_000_000,
        "base_income": 112_500_000_000,
        "income_min": 104_000_000_000,
        "income_max": 121_000_000_000,
    },
    "crystal_bank": {
        "name": 'Инвестбанк "Crystal Bank"',
        "price": 1_000_000_000_000,
        "base_income": 225_000_000_000,
        "income_min": 208_000_000_000,
        "income_max": 242_000_000_000,
    },
    "titanium_air": {
        "name": 'Авиагруппа "Titanium Air"',
        "price": 2_000_000_000_000,
        "base_income": 450_000_000_000,
        "income_min": 417_000_000_000,
        "income_max": 483_000_000_000,
    },
    "genesis_mining": {
        "name": 'Добывающая корпорация "Genesis Mining"',
        "price": 4_000_000_000_000,
        "base_income": 900_000_000_000,
        "income_min": 834_000_000_000,
        "income_max": 966_000_000_000,
    },
    "nova_media": {
        "name": 'Медиахолдинг "Nova Media"',
        "price": 8_000_000_000_000,
        "base_income": 1_800_000_000_000,
        "income_min": 1_668_000_000_000,
        "income_max": 1_932_000_000_000,
    },
    "orbital_logistics": {
        "name": 'Логистический альянс "Orbital Logistics"',
        "price": 15_000_000_000_000,
        "base_income": 3_375_000_000_000,
        "income_min": 3_126_000_000_000,
        "income_max": 3_624_000_000_000,
    },
    "zenith_estate": {
        "name": 'Девелоперская империя "Zenith Estate"',
        "price": 28_000_000_000_000,
        "base_income": 6_300_000_000_000,
        "income_min": 5_838_000_000_000,
        "income_max": 6_762_000_000_000,
    },
    "fusion_dynamics": {
        "name": 'Энергоконцерн "Fusion Dynamics"',
        "price": 45_000_000_000_000,
        "base_income": 10_125_000_000_000,
        "income_min": 9_384_000_000_000,
        "income_max": 10_866_000_000_000,
    },
    "atlas_defense": {
        "name": 'Оборонный гигант "Atlas Defense"',
        "price": 75_000_000_000_000,
        "base_income": 16_875_000_000_000,
        "income_min": 15_636_000_000_000,
        "income_max": 18_114_000_000_000,
    },
    "celestium_group": {
        "name": 'Глобальный конгломерат "Celestium Group"',
        "price": 120_000_000_000_000,
        "base_income": 27_000_000_000_000,
        "income_min": 25_020_000_000_000,
        "income_max": 28_980_000_000_000,
    },
}

PURCHASEABLE_BUSINESS_KEYS = [
    "grand_espresso",
    "golden_croissant",
    "fashion_house",
    "gourmania",
    "global_market",
    "fuel_giant",
    "neboskreb",
    "imax_empire",
    "worldwide_holdings",
    "powercore",
    "cybersoft",
    "neotech",
    "aurora_motors",
    "skyline_resort",
    "quantum_labs",
    "imperial_shipyards",
    "crystal_bank",
    "titanium_air",
    "genesis_mining",
    "nova_media",
    "orbital_logistics",
    "zenith_estate",
    "fusion_dynamics",
    "atlas_defense",
    "celestium_group",
]

# Совместимость со старыми филиалами в базе данных.
BUSINESSES_CATALOG["premium_detailing"] = dict(BUSINESSES_CATALOG["golden_croissant"])
BUSINESSES_CATALOG["agro_empire"] = dict(BUSINESSES_CATALOG["global_market"])
BUSINESSES_CATALOG["iron_world"] = dict(BUSINESSES_CATALOG["global_market"])
BUSINESSES_CATALOG["empire_realty"] = dict(BUSINESSES_CATALOG["neboskreb"])
BUSINESSES_CATALOG["megadrive"] = dict(BUSINESSES_CATALOG["fuel_giant"])
BUSINESSES_CATALOG["royal_beauty"] = dict(BUSINESSES_CATALOG["fashion_house"])
BUSINESSES_CATALOG["golden_trust"] = dict(BUSINESSES_CATALOG["grand_espresso"])
BUSINESSES_CATALOG["diamond_crown"] = dict(BUSINESSES_CATALOG["gourmania"])

UPGRADE_BONUSES = {0: 0.0, 1: 0.10, 2: 0.25, 3: 0.50}


def get_upgrade_cost_for_business(business_meta: Dict, new_level: int) -> int:
    base_price = int(business_meta.get("price", 0) or 0)
    level_multipliers = {
        1: 0.05,
        2: 0.10,
        3: 0.20,
    }
    multiplier = level_multipliers.get(int(new_level), 0.20)
    return max(100_000, int(base_price * multiplier))


async def add_business(user_id: int, business_key: str) -> int:
    async with DB_WRITE_LOCK:
        db = await connect_sqlite()
        try:
            cur = await db.execute(
                "SELECT COALESCE(MAX(branch_no), 0) FROM businesses WHERE user_id = ? AND business_key = ?",
                (user_id, business_key),
            )
            next_branch = int((await cur.fetchone())[0]) + 1
            if next_branch > MAX_BRANCHES_PER_BUSINESS:
                raise ValueError(f"Нельзя иметь больше {MAX_BRANCHES_PER_BUSINESS} филиалов одного типа бизнеса.")
            await db.execute(
                "INSERT INTO businesses (user_id, business_key, branch_no) VALUES (?, ?, ?)",
                (user_id, business_key, next_branch),
            )
            await db.commit()
            return next_branch
        finally:
            await db.close()


async def get_user_businesses(user_id: int) -> List[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, business_key, branch_no, upgrade_level, products, branch_balance, talisman_active, regular_talisman_active, talisman_bonus_percent, last_collected_at "
            "FROM businesses WHERE user_id = ? ORDER BY business_key, branch_no",
            (user_id,),
        )
        rows = [dict(r) for r in await cur.fetchall()]
    for row in rows:
        row["meta"] = BUSINESSES_CATALOG.get(
            row["business_key"],
            {"name": row["business_key"], "base_income": 0, "price": 0},
        )
    return rows


async def get_all_business_branch_counts() -> Dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id, COUNT(*) FROM businesses GROUP BY user_id"
        )
        rows = await cur.fetchall()
    return {str(int(user_id)): int(count) for user_id, count in rows}


async def get_business_by_id(user_id: int, business_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, business_key, branch_no, upgrade_level, products, branch_balance, talisman_active, regular_talisman_active, talisman_bonus_percent, last_collected_at "
            "FROM businesses WHERE user_id = ? AND id = ?",
            (user_id, business_id),
        )
        row = await cur.fetchone()
    if not row:
        return None
    item = dict(row)
    item["meta"] = BUSINESSES_CATALOG.get(
        item["business_key"],
        {"name": item["business_key"], "base_income": 0, "price": 0},
    )
    return item


def get_business_collect_ready_at(business: Dict) -> Optional[datetime]:
    raw_value = business.get("last_collected_at")
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(str(raw_value)) + BUSINESS_COLLECT_COOLDOWN
    except (TypeError, ValueError):
        return None


def get_business_collect_seconds_left(business: Dict) -> int:
    ready_at = get_business_collect_ready_at(business)
    if ready_at is None:
        return 0
    return max(0, int((ready_at - datetime.now()).total_seconds()))


def get_business_talisman_bonus_percent(business: Dict) -> int:
    explicit_bonus = int(business.get("talisman_bonus_percent", 0) or 0)
    if explicit_bonus > 0:
        return explicit_bonus
    return 500 if int(business.get("talisman_active", 0)) else 0


def has_regular_business_talisman(business: Dict) -> bool:
    if int(business.get("regular_talisman_active", 0) or 0) == 1:
        return True
    return int(business.get("talisman_active", 0) or 0) == 1 and int(business.get("talisman_bonus_percent", 0) or 0) == 500


async def delete_business_branch(user_id: int, business_id: int) -> Tuple[bool, Optional[Dict]]:
    business = await get_business_by_id(user_id, business_id)
    if not business:
        return False, None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM businesses WHERE id = ? AND user_id = ?",
            (business_id, user_id),
        )
        await db.commit()
    return True, business


async def delete_business_group(user_id: int, business_key: str) -> Tuple[int, Optional[Dict]]:
    businesses = await get_user_businesses(user_id)
    target_branches = [biz for biz in businesses if biz["business_key"] == business_key]
    if not target_branches:
        return 0, None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM businesses WHERE user_id = ? AND business_key = ?",
            (user_id, business_key),
        )
        await db.commit()
    return len(target_branches), target_branches[0]


async def upgrade_business(user_id: int, business_id: int) -> Tuple[bool, str, int]:
    business = await get_business_by_id(user_id, business_id)
    if not business:
        return False, "Филиал не найден.", 0
    level = int(business["upgrade_level"])
    if level >= 3:
        return False, "Филиал уже на максимальном уровне.", 0
    new_level = level + 1
    cost = get_upgrade_cost_for_business(business["meta"], new_level)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE businesses SET upgrade_level = ? WHERE id = ? AND user_id = ?",
            (new_level, business_id, user_id),
        )
        await db.commit()
    return True, f"Улучшение до уровня {new_level} применено.", cost


async def refill_products(user_id: int, business_id: int, amount: int, max_products: int = 100) -> Tuple[bool, str, int]:
    business = await get_business_by_id(user_id, business_id)
    if not business:
        return False, "Филиал не найден.", 0
    current = int(business["products"])
    if current >= max_products:
        return False, "Продукты уже заполнены.", 0
    add_amount = max(1, min(amount, max_products - current))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE businesses SET products = products + ? WHERE id = ? AND user_id = ?",
            (add_amount, business_id, user_id),
        )
        await db.commit()
    return True, f"Пополнено на {add_amount} ед.", add_amount


async def collect_income(user_id: int, business_id: int) -> Tuple[bool, str, int]:
    business = await get_business_by_id(user_id, business_id)
    if not business:
        return False, "Филиал не найден.", 0
    seconds_left = get_business_collect_seconds_left(business)
    if seconds_left > 0:
        hours = seconds_left // 3600
        minutes = (seconds_left % 3600) // 60
        return False, f"Сбор будет доступен через {hours}ч. {minutes}м.", 0
    products_to_spend = 5
    current_products = int(business["products"])
    if current_products < products_to_spend:
        return False, f"Недостаточно продуктов. Для сбора нужно минимум {products_to_spend}.", 0
    base_income = int(business["meta"]["base_income"])
    upgrade_bonus = UPGRADE_BONUSES.get(int(business["upgrade_level"]), 0.0)
    talisman_bonus = get_business_talisman_bonus_percent(business) / 100.0
    final_income = int(base_income * (1 + upgrade_bonus + talisman_bonus))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE businesses SET products = products - ?, branch_balance = branch_balance + ?, last_collected_at = ? "
            "WHERE id = ? AND user_id = ?",
            (products_to_spend, final_income, datetime.now().isoformat(), business_id, user_id),
        )
        await db.commit()
    return True, f"Доход собран. Списано продуктов: {products_to_spend}.", final_income


async def activate_business_talisman(user_id: int, business_id: int, bonus_percent: int = 500) -> Tuple[bool, str]:
    business = await get_business_by_id(user_id, business_id)
    if not business:
        return False, "Филиал не найден."
    if int(business.get("talisman_active", 0)) == 1:
        return False, f'Талисман уже активирован на филиал #{business["branch_no"]}.'
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE businesses SET talisman_active = 1, talisman_bonus_percent = ? WHERE id = ? AND user_id = ?",
            (int(bonus_percent), business_id, user_id),
        )
        await db.commit()
    return True, f'Талисман активирован на филиал #{business["branch_no"]}.'


async def has_active_business_talisman(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM businesses WHERE user_id = ? AND talisman_active = 1 LIMIT 1",
            (user_id,),
        )
        row = await cur.fetchone()
    return bool(row)


async def activate_business_talisman(user_id: int, business_id: int, bonus_percent: int = 500) -> Tuple[bool, str]:
    business = await get_business_by_id(user_id, business_id)
    if not business:
        return False, "Филиал не найден."
    is_t34_stack = int(bonus_percent) >= 1500
    if not is_t34_stack and has_regular_business_talisman(business):
        return False, f'Обычный талисман уже активирован на филиал #{business["branch_no"]}.'
    current_bonus = int(business.get("talisman_bonus_percent", 0) or 0)
    new_bonus = current_bonus + int(bonus_percent)
    regular_active = 1 if (has_regular_business_talisman(business) or not is_t34_stack) else 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE businesses SET talisman_active = 1, regular_talisman_active = ?, talisman_bonus_percent = ? WHERE id = ? AND user_id = ?",
            (regular_active, new_bonus, business_id, user_id),
        )
        await db.commit()
    if is_t34_stack:
        return True, f'Бонус Т-34 добавлен на филиал #{business["branch_no"]}.'
    return True, f'Талисман активирован на филиал #{business["branch_no"]}.'
