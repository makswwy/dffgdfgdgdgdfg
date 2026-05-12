# remove_global_role.py - снятие глобальной роли пользователя
import sqlite3

# ID пользователя, у которого нужно снять глобальную роль
USER_ID = 813161311  # ЗАМЕНИТЕ НА ID ПОЛЬЗОВАТЕЛЯ

print(f"🔍 Поиск глобальной роли пользователя {USER_ID}...")
print("-" * 50)

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

# Проверяем текущую глобальную роль
cursor.execute("SELECT level FROM global_managers WHERE user_id = ?", (USER_ID,))
result = cursor.fetchone()

if result:
    print(f"📊 Текущий глобальный уровень: {result[0]}")
    
    # Удаляем запись о глобальной роли
    cursor.execute("DELETE FROM global_managers WHERE user_id = ?", (USER_ID,))
    conn.commit()
    print(f"✅ Глобальная роль успешно снята с пользователя {USER_ID}")
else:
    print(f"⚠️ Пользователь {USER_ID} не имеет глобальной роли")
    
    # Показываем всех, у кого есть глобальные роли
    cursor.execute("SELECT user_id, level FROM global_managers ORDER BY level DESC")
    all_global = cursor.fetchall()
    if all_global:
        print("\n📋 Список пользователей с глобальными ролями:")
        for uid, level in all_global:
            print(f"   - ID: {uid}, уровень: {level}")
    else:
        print("📋 Нет пользователей с глобальными ролями")

conn.close()
print("-" * 50)
print("🎉 Готово! Перезапустите бота.")