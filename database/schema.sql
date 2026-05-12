CREATE TABLE IF NOT EXISTS cases_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    case_type TEXT NOT NULL,
    reward_type TEXT NOT NULL,
    reward_value TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    item_type TEXT NOT NULL,
    item_name TEXT NOT NULL,
    item_value INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS businesses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    business_key TEXT NOT NULL,
    branch_no INTEGER NOT NULL DEFAULT 1,
    upgrade_level INTEGER NOT NULL DEFAULT 0,
    products INTEGER NOT NULL DEFAULT 100,
    branch_balance INTEGER NOT NULL DEFAULT 0,
    talisman_active INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bananas (
    user_id INTEGER PRIMARY KEY,
    amount INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_cases_log_user_id ON cases_log(user_id);
CREATE INDEX IF NOT EXISTS idx_inventory_user_id ON inventory(user_id);
CREATE INDEX IF NOT EXISTS idx_businesses_user_id ON businesses(user_id);
