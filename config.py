"""
Конфигурация бота массажного салона
Все настройки загружаются из .env файла
"""

import os
from dotenv import load_dotenv
from pathlib import Path

# Загружаем переменные окружения
load_dotenv()

# ============================
# НАСТРОЙКИ БОТА
# ============================

# Telegram Bot
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "@amphoreus1")

# База данных
DB_PATH = os.getenv("DB_PATH", "massage.db")
BACKUP_DIR = os.getenv("BACKUP_DIR", "backups/")

# Безопасность
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "default_key_change_me").encode()
MAX_LOGIN_ATTEMPTS = int(os.getenv("MAX_LOGIN_ATTEMPTS", "5"))
SESSION_TIMEOUT = int(os.getenv("SESSION_TIMEOUT", "7200"))

# Уведомления
SEND_ADMIN_NOTIFICATIONS = os.getenv("SEND_ADMIN_NOTIFICATIONS", "true").lower() == "true"
NOTIFY_ON_ERROR = os.getenv("NOTIFY_ON_ERROR", "true").lower() == "true"

# Напоминания
REMINDER_DAY_BEFORE = os.getenv("REMINDER_DAY_BEFORE", "true").lower() == "true"
REMINDER_HOUR_BEFORE = os.getenv("REMINDER_HOUR_BEFORE", "true").lower() == "true"
REMINDER_10MIN_BEFORE = os.getenv("REMINDER_10MIN_BEFORE", "true").lower() == "true"

# Резервное копирование
AUTO_BACKUP = os.getenv("AUTO_BACKUP", "true").lower() == "true"
BACKUP_INTERVAL_HOURS = int(os.getenv("BACKUP_INTERVAL_HOURS", "6"))
MAX_BACKUPS = int(os.getenv("MAX_BACKUPS", "30"))

# Мониторинг
ENABLE_METRICS = os.getenv("ENABLE_METRICS", "true").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ============================
# ПУТИ И ФАЙЛЫ
# ============================

# Создаем директории при необходимости
Path(BACKUP_DIR).mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# Пути к логам
LOG_FILE = "logs/bot.log"
ERROR_LOG_FILE = "logs/error.log"

# ============================
# ВАЛИДАЦИЯ КОНФИГУРАЦИИ
# ============================

def validate_config():
    """Проверяет корректность конфигурации"""
    errors = []
    
    if not BOT_TOKEN or BOT_TOKEN == "ваш_токен_бота_здесь":
        errors.append("❌ BOT_TOKEN не установлен. Укажите токен в .env файле")
    
    if ENCRYPTION_KEY == b"default_key_change_me":
        errors.append("⚠️  ENCRYPTION_KEY установлен по умолчанию. Измените его в .env файле!")
    
    if len(ENCRYPTION_KEY) < 32:
        errors.append("⚠️  ENCRYPTION_KEY должен быть не менее 32 символов")
    
    if not ADMIN_USERNAME or ADMIN_USERNAME == "@username_администратора":
        errors.append("⚠️  ADMIN_USERNAME не установлен корректно")
    
    if not ADMIN_USERNAME.startswith("@"):
        errors.append("⚠️  ADMIN_USERNAME должен начинаться с @")
    
    return errors

# Проверяем конфигурацию при импорте
config_errors = validate_config()
if config_errors:
    for error in config_errors:
        print(error)
    if "❌" in config_errors[0]:
        print("\n🛑 Критические ошибки конфигурации! Заполните .env файл.")

# Настройки планировщика метрик
SEND_DAILY_REPORT = os.getenv("SEND_DAILY_REPORT", "true").lower() == "true"
DAILY_REPORT_TIME = os.getenv("DAILY_REPORT_TIME", "20:00")  # Время отправки отчета
METRICS_UPDATE_INTERVAL = int(os.getenv("METRICS_UPDATE_INTERVAL", "300"))  # Секунды
SAVE_METRICS_INTERVAL = int(os.getenv("SAVE_METRICS_INTERVAL", "21600"))  # 6 часов в секундах

# Проверка корректности времени
try:
    report_hour, report_minute = map(int, DAILY_REPORT_TIME.split(':'))
    if not (0 <= report_hour <= 23 and 0 <= report_minute <= 59):
        raise ValueError
except:
    print("⚠️  Некорректное время DAILY_REPORT_TIME. Использую 20:00")
    DAILY_REPORT_TIME = "20:00"