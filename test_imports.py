#!/usr/bin/env python3
"""Тестовый скрипт для проверки импортов"""

print("🔍 Тестирование импортов...")

try:
    import config
    print("✅ config.py - OK")
except Exception as e:
    print(f"❌ config.py - Ошибка: {e}")

try:
    from utils import monitoring
    print("✅ utils.monitoring - OK")
except Exception as e:
    print(f"❌ utils.monitoring - Ошибка: {e}")

try:
    from utils import security
    print("✅ utils.security - OK")
except Exception as e:
    print(f"❌ utils.security - Ошибка: {e}")

try:
    from database import backup
    print("✅ database.backup - OK")
except Exception as e:
    print(f"❌ database.backup - Ошибка: {e}")

try:
    import scheduler
    print("✅ scheduler.py - OK")
except Exception as e:
    print(f"❌ scheduler.py - Ошибка: {e}")

print("\n📊 Итог проверки импортов:")
print("-" * 30)