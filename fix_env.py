import secrets
import os

# Генерируем новый ключ
new_key = secrets.token_hex(32)
print(f"🔑 Генерирую новый ключ: {new_key}")

# Содержимое .env файла
env_content = f"""# Telegram Bot Configuration
BOT_TOKEN=8545485296:AAHS8kU49-aldFggRINfOGMZHv2FLyWEI0o
ADMIN_USERNAME=@amphoreus1

# Database Configuration
DB_PATH=massage.db
BACKUP_DIR=backups/

# Security Configuration
ENCRYPTION_KEY={new_key}
MAX_LOGIN_ATTEMPTS=5
SESSION_TIMEOUT=7200

# Notification Configuration
SEND_ADMIN_NOTIFICATIONS=true
NOTIFY_ON_ERROR=true

# Reminder Configuration
REMINDER_DAY_BEFORE=true
REMINDER_HOUR_BEFORE=true
REMINDER_10MIN_BEFORE=true

# Backup Configuration
AUTO_BACKUP=true
BACKUP_INTERVAL_HOURS=6
MAX_BACKUPS=30

# Monitoring Configuration
ENABLE_METRICS=true
LOG_LEVEL=INFO

# Metrics Scheduler Configuration
SEND_DAILY_REPORT=true
DAILY_REPORT_TIME=20:00
METRICS_UPDATE_INTERVAL=300
SAVE_METRICS_INTERVAL=21600
"""

# Записываем файл
with open('.env', 'w', encoding='utf-8') as f:
    f.write(env_content)

print("✅ Файл .env перезаписан!")
print(f"📁 Путь: {os.path.abspath('.env')}")
print("\n📋 Проверьте содержимое файла:")
print("-" * 40)
print(env_content[:200] + "...")
print("-" * 40)