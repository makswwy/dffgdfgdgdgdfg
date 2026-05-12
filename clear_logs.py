# clear_logs.py - очистка всех логов
import sqlite3

print("🗑️ Начинаю очистку логов...")
print("-" * 50)

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

# Список таблиц с логами
log_tables = [
    'economy',           # логи экономики
    'logchats',          # логи действий модераторов
    'mutelogs_%',        # логи мутов (для всех чатов)
    'warnhistory_%',     # история предупреждений (для всех чатов)
    'bugsusers',         # логи багов
]

# Очищаем основные таблицы
for table in ['economy', 'logchats', 'bugsusers']:
    try:
        cursor.execute(f"DELETE FROM {table}")
        print(f"✅ {table}: удалено {cursor.rowcount} записей")
    except:
        print(f"⚠️ Таблица {table} не найдена")

# Очищаем таблицы для каждого чата (mutelogs_, warnhistory_)
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'mutelogs_%' OR name LIKE 'warnhistory_%')")
dynamic_tables = [row[0] for row in cursor.fetchall()]

for table in dynamic_tables:
    try:
        cursor.execute(f"DELETE FROM {table}")
        if cursor.rowcount > 0:
            print(f"✅ {table}: удалено {cursor.rowcount} записей")
    except:
        pass

conn.commit()
conn.close()

print("-" * 50)
print("🎉 Очистка логов завершена!")