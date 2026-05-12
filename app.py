import json
import os
import sqlite3
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

from flask import Flask, abort, jsonify, redirect, render_template, request, session, url_for


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"
CONFIG_PATH = BASE_DIR / "config.json"
DEFAULT_LIMIT = 100
MAX_LIMIT = 500
SESSION_SECRET = os.environ.get("WEB_SESSION_SECRET", "banana-manager-web-secret")
VK_API_VERSION = "5.199"

config_data = json.loads(CONFIG_PATH.read_text(encoding="utf-8")) if CONFIG_PATH.exists() else {}
VK_APP_ID = str(config_data.get("application_id", "") or "").strip()
VK_BOT_TOKEN = str(config_data.get("bot-token", "") or "").strip()

TECH_ROLE_NAMES = {
    1: "Младший технический специалист",
    2: "Технический специалист",
    3: "Старший технический специалист",
    4: "Куратор технических специалистов",
    5: "Заместитель главного технического специалиста",
    6: "Главный технический специалист",
}

GLOBAL_ROLE_NAMES = {
    0: "Пользователь",
    1: "Модератор",
    2: "Старший модератор",
    3: "Администратор",
    4: "Старший администратор",
    5: "Зам. спец администратора",
    6: "Спец администратор",
    7: "Владелец беседы",
    8: "Заместитель директора",
    9: "Осн. заместитель директора",
    10: "Директор бота",
    11: "Разработчик бота",
}

LOG_TABLES = {
    "economy": {
        "title": "Логи экономики",
        "description": "Экономические действия с пользователями, суммами и датой операций.",
        "value_column": "amount",
        "value_label": "Сумма",
    },
    "logchats": {
        "title": "Логи бота",
        "description": "Модерация, роли, настройки и служебные действия бота.",
        "value_column": "role",
        "value_label": "Роль поля",
    },
    "exchange_logs": {
        "title": "Логи обменов",
        "description": "Все действия по обменам между пользователями: создание, подтверждение, отмена и завершение.",
        "value_column": "status",
        "value_label": "Статус",
    },
}

REQUEST_TYPES = [
    ("reset_money", "Обнуление баланса"),
    ("reset_bananas", "Обнуление бананов"),
    ("reset_business", "Обнуление бизнесов"),
    ("reset_vip", "Обнуление VIP"),
    ("reset_pass", "Обнуление BANANA PASS"),
    ("refund_money", "Возврат монет"),
    ("refund_item", "Возврат предмета"),
    ("refund_business", "Возврат бизнеса"),
]

app = Flask(__name__)
app.secret_key = SESSION_SECRET

_vk_name_cache: dict[int, dict] = {}


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return bool(row)


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row[1]) == column_name for row in rows)


def ensure_web_schema() -> None:
    with get_db_connection() as conn:
        for table_name in ("economy", "logchats", "exchange_logs"):
            if _table_exists(conn, table_name) and not _column_exists(conn, table_name, "created_at"):
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN created_at TEXT")
                conn.execute(
                    f"UPDATE {table_name} SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL OR created_at = ''"
                )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS web_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                requester_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                amount INTEGER,
                details TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                reviewer_id INTEGER,
                resolution_note TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_web_requests_status ON web_requests(status, created_at)")
        conn.commit()


def _safe_int(value: Optional[str], default: int, minimum: int = 1, maximum: int = MAX_LIMIT) -> int:
    try:
        parsed = int(str(value or default))
    except Exception:
        parsed = default
    return max(minimum, min(parsed, maximum))


def _safe_user_id(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = str(value).strip()
    return int(value) if value.isdigit() else None


def _get_global_level(conn: sqlite3.Connection, user_id: int) -> int:
    if not _table_exists(conn, "permissions_11"):
        return 0
    row = conn.execute(
        "SELECT level FROM permissions_11 WHERE user_id = ? ORDER BY level DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    return int(row[0]) if row else 0


def _get_tech_level(conn: sqlite3.Connection, user_id: int) -> int:
    if not _table_exists(conn, "tech_permissions"):
        return 0
    row = conn.execute(
        "SELECT level FROM tech_permissions WHERE user_id = ? ORDER BY level DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    return int(row[0]) if row else 0


def _vk_api_call(method: str, params: dict) -> dict:
    if not VK_BOT_TOKEN:
        return {}
    query = urllib.parse.urlencode({**params, "access_token": VK_BOT_TOKEN, "v": VK_API_VERSION})
    url = f"https://api.vk.com/method/{method}?{query}"
    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload


def _fetch_vk_name_by_id(user_id: int) -> dict:
    if user_id in _vk_name_cache:
        return _vk_name_cache[user_id]

    data = {
        "full_name": f"Пользователь {user_id}",
        "first_name": "Пользователь",
        "last_name": str(user_id),
    }
    try:
        payload = _vk_api_call("users.get", {"user_ids": user_id})
        response = payload.get("response") or []
        if response:
            user = response[0]
            first_name = str(user.get("first_name", "")).strip()
            last_name = str(user.get("last_name", "")).strip()
            data = {
                "full_name": f"{first_name} {last_name}".strip() or f"Пользователь {user_id}",
                "first_name": first_name or "Пользователь",
                "last_name": last_name or str(user_id),
            }
    except Exception:
        pass

    _vk_name_cache[user_id] = data
    return data


def _build_profile(conn: sqlite3.Connection, user_id: Optional[int]) -> dict:
    if not user_id:
        return {
            "id": None,
            "name": "Не указан",
            "role": "—",
            "role_type": "none",
            "vk_url": None,
            "subtitle": "ID: —",
        }

    vk_name = _fetch_vk_name_by_id(user_id)
    global_level = _get_global_level(conn, user_id)
    tech_level = _get_tech_level(conn, user_id)

    if global_level > 0:
        role = GLOBAL_ROLE_NAMES.get(global_level, "Пользователь")
        role_type = "global"
    elif tech_level > 0:
        role = TECH_ROLE_NAMES.get(tech_level, "Технический специалист")
        role_type = "tech"
    else:
        role = "Пользователь"
        role_type = "global"

    return {
        "id": user_id,
        "name": vk_name["full_name"],
        "role": role,
        "role_type": role_type,
        "vk_url": f"https://vk.com/id{user_id}",
        "subtitle": f"ID: {user_id}",
    }


def _load_logs(table_name: str, limit: int, user_id: Optional[int] = None) -> list[dict]:
    if table_name not in LOG_TABLES:
        abort(404)

    value_column = LOG_TABLES[table_name]["value_column"]
    where_clause = ""
    params: list[object] = []
    if user_id is not None:
        where_clause = "WHERE user_id = ? OR target_id = ?"
        params.extend([user_id, user_id])

    query = (
        f"SELECT rowid, user_id, target_id, {value_column} AS value, log, created_at "
        f"FROM {table_name} {where_clause} ORDER BY rowid DESC LIMIT ?"
    )
    params.append(limit)

    with get_db_connection() as conn:
        raw_rows = [dict(row) for row in conn.execute(query, params).fetchall()]
        rows = []
        for row in raw_rows:
            uid = row.get("user_id")
            tid = row.get("target_id")
            row["actor"] = _build_profile(conn, int(uid)) if uid is not None else _build_profile(conn, None)
            row["target"] = _build_profile(conn, int(tid)) if tid is not None else _build_profile(conn, None)
            row["created_label"] = str(row.get("created_at") or "Дата не записана")
            rows.append(row)
    return rows


def _load_requests(limit: int, status_filter: str) -> list[dict]:
    with get_db_connection() as conn:
        params: list[object] = []
        where = ""
        if status_filter and status_filter != "all":
            where = "WHERE status = ?"
            params.append(status_filter)
        params.append(limit)
        rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM web_requests {where} ORDER BY id DESC LIMIT ?",
                params,
            ).fetchall()
        ]
        for row in rows:
            row["requester"] = _build_profile(conn, int(row["requester_id"]))
            row["target"] = _build_profile(conn, int(row["target_id"]))
            row["reviewer"] = _build_profile(conn, int(row["reviewer_id"])) if row.get("reviewer_id") else _build_profile(conn, None)
    return rows


def _get_session_user() -> Optional[dict]:
    user_id = session.get("vk_user_id")
    if not user_id:
        return None

    user_id = int(user_id)
    with get_db_connection() as conn:
        global_level = _get_global_level(conn, user_id)
        tech_level = _get_tech_level(conn, user_id)
        profile = _build_profile(conn, user_id)
    return {
        "id": user_id,
        "name": profile["name"],
        "vk_url": profile["vk_url"],
        "global_level": global_level,
        "tech_level": tech_level,
        "role_label": profile["role"],
    }


def _admin_context() -> dict:
    session_user = _get_session_user()
    allowed_by_session = bool(
        session_user and (session_user["tech_level"] > 0 or session_user["global_level"] >= 10)
    )
    return {
        "allowed": allowed_by_session,
        "session_user": session_user,
        "vk_login_ready": bool(VK_APP_ID and VK_APP_ID != "YOUR APP_ID"),
    }


def _require_log_access(admin: dict):
    if not admin["session_user"]:
        if admin["vk_login_ready"]:
            return redirect(url_for("auth_vk_start"))
        return (
            render_template(
                "index.html",
                stats={"page": "botlogs", "limit": DEFAULT_LIMIT, "db_exists": DB_PATH.exists()},
                admin=admin,
                page_title="Доступ к логам",
                page_description="Для доступа к защищённым логам требуется вход через VK.",
                access_denied=True,
                denied_text="Для доступа к защищённым разделам войдите через VK.",
                rows=[],
                request_rows=[],
                request_types=REQUEST_TYPES,
                value_label="",
            ),
            403,
        )
    if not admin["allowed"]:
        return (
            render_template(
                "index.html",
                stats={"page": "botlogs", "limit": DEFAULT_LIMIT, "db_exists": DB_PATH.exists()},
                admin=admin,
                page_title="Доступ к логам",
                page_description="Раздел доступен только техролям, директору и разработчикам.",
                access_denied=True,
                denied_text="У этого аккаунта нет доступа к разделу логов.",
                rows=[],
                request_rows=[],
                request_types=REQUEST_TYPES,
                value_label="",
            ),
            403,
        )
    return None


@app.route("/auth/vk/start")
def auth_vk_start():
    if not VK_APP_ID or VK_APP_ID == "YOUR APP_ID":
        return "В config.json не задан реальный application_id VK.", 500

    callback_url = request.url_root.rstrip("/") + url_for("auth_vk_callback")
    params = {
        "client_id": VK_APP_ID,
        "redirect_uri": callback_url,
        "response_type": "token",
        "scope": "",
        "display": "page",
        "v": VK_API_VERSION,
    }
    return redirect(f"https://oauth.vk.com/authorize?{urllib.parse.urlencode(params)}")


@app.route("/auth/vk/callback")
def auth_vk_callback():
    return render_template("vk_callback.html")


@app.post("/auth/vk/session")
def auth_vk_session():
    payload = request.get_json(silent=True) or {}
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        return jsonify({"ok": False, "error": "Не передан access_token"}), 400

    try:
        query = urllib.parse.urlencode({"access_token": access_token, "v": VK_API_VERSION})
        url = f"https://api.vk.com/method/users.get?{query}"
        with urllib.request.urlopen(url, timeout=10) as response:
            vk_payload = json.loads(response.read().decode("utf-8"))
        response_data = vk_payload.get("response") or []
        if not response_data:
            return jsonify({"ok": False, "error": "VK не вернул данные пользователя"}), 403
        user = response_data[0]
        session["vk_user_id"] = int(user["id"])
        return jsonify({"ok": True, "user_id": int(user["id"])})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/auth/logout")
def auth_logout():
    session.pop("vk_user_id", None)
    return redirect(url_for("economy_logs_page"))


def _render_logs_page(page_key: str, protected: bool = False):
    ensure_web_schema()
    admin = _admin_context()
    if protected:
        guard = _require_log_access(admin)
        if guard:
            return guard

    limit = _safe_int(request.args.get("limit"), DEFAULT_LIMIT)
    user_filter = _safe_user_id(request.args.get("user_id"))
    meta = LOG_TABLES[page_key]
    rows = _load_logs(page_key, limit, user_filter)
    stats = {
        "user_filter": user_filter,
        "limit": limit,
        "db_exists": DB_PATH.exists(),
        "page": "economy" if page_key == "economy" else ("botlogs" if page_key == "logchats" else "exchangelogs"),
    }
    return render_template(
        "index.html",
        rows=rows,
        request_rows=[],
        request_types=REQUEST_TYPES,
        stats=stats,
        admin=admin,
        page_title=meta["title"],
        page_description=meta["description"],
        value_label=meta["value_label"],
    )


@app.route("/")
@app.route("/economy")
def economy_logs_page():
    return _render_logs_page("economy", protected=False)


@app.route("/botlogs")
def bot_logs_page():
    return _render_logs_page("logchats", protected=True)


@app.route("/exchangelogs")
def exchange_logs_page():
    return _render_logs_page("exchange_logs", protected=True)


@app.route("/requests")
def requests_page():
    ensure_web_schema()
    admin = _admin_context()
    guard = _require_log_access(admin)
    if guard:
        return guard

    limit = _safe_int(request.args.get("limit"), DEFAULT_LIMIT)
    status_filter = str(request.args.get("status") or "open").strip().lower()
    request_rows = _load_requests(limit, status_filter)
    stats = {
        "limit": limit,
        "db_exists": DB_PATH.exists(),
        "page": "requests",
        "status_filter": status_filter,
    }
    return render_template(
        "index.html",
        rows=[],
        request_rows=request_rows,
        request_types=REQUEST_TYPES,
        stats=stats,
        admin=admin,
        page_title="Формы действий",
        page_description="Создание и просмотр заявок на обнуление, возврат и другие служебные действия.",
        value_label="",
    )


@app.post("/requests/create")
def create_request():
    ensure_web_schema()
    admin = _admin_context()
    guard = _require_log_access(admin)
    if guard:
        return guard

    requester_id = int(admin["session_user"]["id"])
    target_id = _safe_user_id(request.form.get("target_id"))
    action_type = str(request.form.get("action_type") or "").strip()
    details = str(request.form.get("details") or "").strip()
    amount_raw = str(request.form.get("amount") or "").strip()
    amount = int(amount_raw) if amount_raw.isdigit() else None

    if not target_id or action_type not in {item[0] for item in REQUEST_TYPES}:
        return redirect(url_for("requests_page", status="open", error="invalid"))

    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO web_requests (requester_id, target_id, action_type, amount, details, status, updated_at)
            VALUES (?, ?, ?, ?, ?, 'open', CURRENT_TIMESTAMP)
            """,
            (requester_id, target_id, action_type, amount, details),
        )
        conn.commit()
    return redirect(url_for("requests_page", status="open", created="1"))


@app.route("/health")
def health():
    return {
        "ok": True,
        "database_exists": DB_PATH.exists(),
        "mode": "web",
        "vk_login_ready": bool(VK_APP_ID and VK_APP_ID != "YOUR APP_ID"),
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    app.run(host="0.0.0.0", port=port)
