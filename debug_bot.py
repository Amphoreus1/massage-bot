#!/usr/bin/env python3
"""Запуск бота с отладкой"""

import asyncio
import aiosqlite
import config

async def test_database():
    """Тест соединения с базой данных"""
    try:
        async with aiosqlite.connect(config.DB_PATH) as db:
            # Проверяем существование таблиц
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = await cursor.fetchall()
            await cursor.close()
            
            print("✅ База данных подключена")
            print(f"📋 Найдено таблиц: {len(tables)}")
            for table in tables:
                print(f"  - {table[0]}")
            
            # Проверяем администраторов
            cursor = await db.execute("SELECT COUNT(*) FROM admins")
            admin_count = (await cursor.fetchone())[0]
            await cursor.close()
            print(f"👑 Администраторов в базе: {admin_count}")
            
            return True
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return False

async def test_config():
    """Тест конфигурации"""
    print("\n🔧 Проверка конфигурации:")
    print(f"  BOT_TOKEN: {'установлен' if config.BOT_TOKEN and config.BOT_TOKEN != 'ваш_токен_бота_здесь' else 'НЕ УСТАНОВЛЕН'}")
    print(f"  ADMIN_USERNAME: {config.ADMIN_USERNAME}")
    print(f"  DB_PATH: {config.DB_PATH}")
    print(f"  LOG_LEVEL: {config.LOG_LEVEL}")
    
    # Проверяем валидацию
    errors = config.validate_config()
    if errors:
        print("⚠️  Предупреждения конфигурации:")
        for error in errors:
            print(f"  {error}")
    else:
        print("✅ Конфигурация проверена")

if __name__ == "__main__":
    print("🤖 ТЕСТОВЫЙ ЗАПУСК МАССАЖНОГО БОТА")
    print("=" * 50)
    
    # Тест конфигурации
    asyncio.run(test_config())
    
    # Тест базы данных
    asyncio.run(test_database())
    
    print("\n" + "=" * 50)
    print("📋 Для полного запуска бота выполните:")
    print("   python bot.py")