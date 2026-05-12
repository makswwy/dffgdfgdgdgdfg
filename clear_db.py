# clear_db.py - отдельный скрипт для очистки базы данных
import sqlite3
import os

DB_PATH = "database/bot_database.db"

def clear_specific_tables():
    """Очистка указанных таблиц"""
    
    if not os.path.exists(DB_PATH):
        print(f"❌ База данных не найдена по пути: {DB_PATH}")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("🗑️ Начинаю очистку таблиц...")
    print("-" * 40)
    
    tables_to_clear = ['blacklist', 'gbanlist', 'globalban']
    results = {}
    
    for table in tables_to_clear:
        try:
            cursor.execute(f"DELETE FROM {table};")
            deleted_count = cursor.rowcount
            results[table] = deleted_count
            print(f"✅ Таблица '{table}': удалено {deleted_count} записей")
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print(f"⚠️ Таблица '{table}': не существует")
                results[table] = "не существует"
            else:
                print(f"❌ Ошибка в таблице '{table}': {e}")
                results[table] = f"ошибка: {e}"
    
    print("-" * 40)
    conn.commit()
    conn.close()
    print("✅ Очистка завершена!")
    
    return results

if __name__ == "__main__":
    print("=" * 40)
    print("🔄 СКРИПТ ОЧИСТКИ БАЗЫ ДАННЫХ")
    print("=" * 40)
    
    # Подтверждение перед очисткой
    confirm = input("Вы действительно хотите очистить таблицы blacklist, gbanlist, globalban? (да/нет): ")
    
    if confirm.lower() == "да":
        results = clear_specific_tables()
        print("\n📊 Результаты:")
        for table, count in results.items():
            print(f"   {table}: {count}")
    else:
        print("❌ Очистка отменена")
    
    input("\nНажмите Enter для выхода...")