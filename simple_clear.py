# simple_clear.py - максимально простой скрипт
import sqlite3
import os

# Текущая папка
current_dir = os.path.dirname(os.path.abspath(__file__))
print(f"Текущая папка: {current_dir}")
print("-" * 50)

# Ищем все .db файлы
db_files = []
for root, dirs, files in os.walk(current_dir):
    for file in files:
        if file.endswith('.db'):
            full_path = os.path.join(root, file)
            db_files.append(full_path)
            print(f"Найден: {full_path}")

if not db_files:
    print("❌ Нет файлов .db!")
    exit()

print("-" * 50)
db_path = db_files[0]
print(f"Используем: {db_path}")
print("-" * 50)

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Выводим все таблицы
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Таблицы: {tables}")
    print("-" * 50)
    
    # Очищаем нужные таблицы
    for table in ['blacklist', 'gbanlist', 'globalban']:
        if table in tables:
            cursor.execute(f"DELETE FROM {table};")
            print(f"✅ {table}: удалено {cursor.rowcount} записей")
        else:
            print(f"⚠️ {table}: таблица не найдена")
    
    conn.commit()
    conn.close()
    print("-" * 50)
    print("✅ Готово!")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")

input("\nНажмите Enter для выхода...")