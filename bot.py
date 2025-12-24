import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import logging
import sqlite3
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import asyncio
import aiosqlite
from typing import Dict, List, Optional, Tuple
import json
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import pandas as pd
import io

import config

from utils import monitoring
from utils import security
from database import backup
import scheduler  # Добавлен импорт scheduler

metrics_collector = monitoring.metrics_collector
security_obj = security.security
safe_sender = security.safe_sender

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_MAX_SIZE = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5
ADMIN_IDS = [1627345808]  # ID администратора из вашего bot.env
DB_BACKUP_HOUR = 3  # Час для бэкапа

load_dotenv()

# ============================
# КОНФИГУРАЦИЯ ЛОГИРОВАНИЯ
# ============================

# Настройка расширенного логирования
import logging.handlers

logger = logging.getLogger(__name__)

formatter = logging.Formatter(LOG_FORMAT)  # Используем нашу константу

# Файловый обработчик с ротацией
file_handler = logging.handlers.RotatingFileHandler(
    config.LOG_FILE,
    maxBytes=LOG_MAX_SIZE,  # Используем нашу константу
    backupCount=LOG_BACKUP_COUNT,  # Используем нашу константу
    encoding='utf-8'
)
file_handler.setFormatter(formatter)

# Консольный обработчик
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Настройка корневого логгера
root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, config.LOG_LEVEL))
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

config_errors = config.validate_config()  # Исправлено: validate_config()
if config_errors:
    for error in config_errors:
        print(error)
    if any("❌" in error for error in config_errors):
        print("\n🛑 Критические ошибки конфигурации! Заполните .env файл.")
        exit(1)
else:
    logger.info("✅ Конфигурация успешно загружена и проверена")

# ============================
# БАЗА ДАННЫХ (асинхронная)
# ============================

DB_PATH = config.DB_PATH
TOKEN = config.BOT_TOKEN
ADMIN_USERNAME = config.ADMIN_USERNAME
ADMIN_ID = None  # Будет установлен автоматически

async def init_db():
    """Инициализация базы данных"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица клиентов
        await db.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE,
            username TEXT,
            name TEXT,
            phone TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Таблица услуг
        await db.execute('''
        CREATE TABLE IF NOT EXISTS services (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            duration INTEGER,
            price INTEGER
        )
        ''')
        
        # Таблица массажистов
        await db.execute('''
        CREATE TABLE IF NOT EXISTS masters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            active BOOLEAN DEFAULT 1
        )
        ''')
        
        # Таблица записей - ВАЖНО: статусы сохраняются!
        await db.execute('''
        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER,
            service_id INTEGER,
            master_id INTEGER,
            appointment_time TIMESTAMP,
            status TEXT DEFAULT 'active',  -- active, completed, cancelled
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reminder_sent_day BOOLEAN DEFAULT 0,
            reminder_sent_hour BOOLEAN DEFAULT 0,
            reminder_sent_admin BOOLEAN DEFAULT 0,
            FOREIGN KEY (client_id) REFERENCES clients (id),
            FOREIGN KEY (service_id) REFERENCES services (id),
            FOREIGN KEY (master_id) REFERENCES masters (id),
            UNIQUE(appointment_time, master_id)
        )
        ''')
        
        # Таблица администраторов
        await db.execute('''
        CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE,
            username TEXT,
            name TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Таблица отзывов
        await db.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appointment_id INTEGER,
            client_id INTEGER,
            master_id INTEGER,
            service_id INTEGER,
            rating INTEGER CHECK(rating >= 1 AND rating <= 5),
            comment TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (appointment_id) REFERENCES appointments (id),
            FOREIGN KEY (client_id) REFERENCES clients (id),
            FOREIGN KEY (master_id) REFERENCES masters (id),
            FOREIGN KEY (service_id) REFERENCES services (id)
        )
        ''')
        
        # Индексы для оптимизации
        await db.execute('CREATE INDEX IF NOT EXISTS idx_appointments_time_master ON appointments(appointment_time, master_id)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_appointments_status ON appointments(status)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_clients_tg ON clients(telegram_id)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_clients_username ON clients(username)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_appointments_client_status ON appointments(client_id, status)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_reviews_appointment ON reviews(appointment_id)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_reviews_master ON reviews(master_id)')
        
        # Добавляем тестовые данные, если таблицы пустые
        cursor = await db.execute("SELECT COUNT(*) FROM services")
        count = (await cursor.fetchone())[0]
        await cursor.close()
        
        if count == 0:
            await db.executemany('''
            INSERT INTO services (name, duration, price) VALUES (?, ?, ?)
            ''', [
                ('Классический массаж', 60, 1000),
                ('Спортивный массаж', 60, 1000),
                ('Перкуссионный массаж', 60, 1500),
                ('Вакуумный массаж', 45, 1500)
            ])
        
        cursor = await db.execute("SELECT COUNT(*) FROM masters")
        count = (await cursor.fetchone())[0]
        await cursor.close()
        
        if count == 0:
            await db.executemany('''
            INSERT INTO masters (name) VALUES (?)
            ''', [('Илья',), ('Богдан',)])
        
        # Добавляем администратора по умолчанию (будет обновлен при первом запуске)
        cursor = await db.execute("SELECT COUNT(*) FROM admins")
        count = (await cursor.fetchone())[0]
        await cursor.close()
        
        if count == 0:
            await db.execute('''
            INSERT INTO admins (telegram_id, username, name) VALUES (?, ?, ?)
            ''', (1627345808, ADMIN_USERNAME, "Администратор"))
        
        await db.commit()

# ============================
# УТИЛИТЫ
# ============================

def get_available_times() -> List[str]:
    """Генерирует список доступного времени с интервалом 1.5 часа, последнее время 17:30"""
    times = []
    start_hour = 10
    end_hour = 18
    
    current_hour = start_hour
    current_minute = 0
    
    while current_hour < end_hour or (current_hour == end_hour and current_minute == 0):
        time_str = f"{current_hour:02d}:{current_minute:02d}"
        times.append(time_str)
        
        # Добавляем 1.5 часа
        current_hour += 1
        current_minute += 30
        if current_minute >= 60:
            current_hour += 1
            current_minute -= 60
    
    filtered_times = []
    for time_str in times:
        time_dt = datetime.strptime(time_str, '%H:%M')
        if time_dt <= datetime.strptime('17:30', '%H:%M'):
            filtered_times.append(time_str)
    
    return filtered_times

def get_available_dates() -> List[Tuple[str, str]]:
    """Генерирует список дат на 14 дней вперед"""
    dates = []
    today = datetime.now().date()
    
    for i in range(14):
        date = today + timedelta(days=i)
        date_str = date.strftime('%d.%m.%Y')
        weekday = date.strftime('%A')
        weekdays_ru = {
            'Monday': 'Пн',
            'Tuesday': 'Вт',
            'Wednesday': 'Ср',
            'Thursday': 'Чт',
            'Friday': 'Пт',
            'Saturday': 'Сб',
            'Sunday': 'Вс'
        }
        weekday_ru = weekdays_ru.get(weekday, weekday)
        dates.append((date_str, weekday_ru))
    
    return dates

async def is_slot_available(date_str: str, time_str: str, master_id: int) -> bool:
    """Проверяет доступность слота для конкретного массажиста"""
    appointment_datetime = datetime.strptime(f"{date_str} {time_str}", '%d.%m.%Y %H:%M')
    appointment_db_str = appointment_datetime.strftime('%Y-%m-%d %H:%M:%S')
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT COUNT(*) FROM appointments 
        WHERE appointment_time = ? 
        AND master_id = ?
        AND status = 'active'
        ''', (appointment_db_str, master_id))
        count = (await cursor.fetchone())[0]
        await cursor.close()
        
        return count == 0

async def get_available_slots(date_str: str, master_id: int) -> List[str]:
    """Возвращает список доступных слотов на дату для конкретного массажиста"""
    all_times = get_available_times()
    available_times = []
    
    # Получаем дату и текущее время
    date_obj = datetime.strptime(date_str, '%d.%m.%Y').date()
    now = datetime.now()
    today = now.date()
    
    for time_str in all_times:
        if date_obj == today:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            current_time = now.time()
            # Если время уже прошло, пропускаем
            if time_obj <= current_time:
                continue
        
        if await is_slot_available(date_str, time_str, master_id):
            available_times.append(time_str)
    
    return available_times

def get_back_button(target="main_menu"):
    return [InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_{target}")]

async def clear_user_context(context: ContextTypes.DEFAULT_TYPE):
    """Очистка контекста пользователя"""
    context.user_data.clear()

# ============================
# АДМИНИСТРИРОВАНИЕ
# ============================

async def is_admin(user_id: int, username: str = None) -> bool:
    """Проверяет, является ли пользователь администратором"""
    global ADMIN_ID
    
    async with aiosqlite.connect(DB_PATH) as db:
        if username:
            cursor = await db.execute(
                "SELECT telegram_id FROM admins WHERE username = ?",
                (username.lower(),)
            )
            result = await cursor.fetchone()
            await cursor.close()
            
            if result:
                ADMIN_ID = result[0]
                return True
        
        cursor = await db.execute(
            "SELECT COUNT(*) FROM admins WHERE telegram_id = ?",
            (user_id,)
        )
        count = (await cursor.fetchone())[0]
        await cursor.close()
        
        return count > 0

async def add_admin(user_id: int, username: str, name: str):
    """Добавляет администратора"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO admins (telegram_id, username, name) VALUES (?, ?, ?)",
            (user_id, username.lower(), name)
        )
        await db.commit()

# ============================
# ЭКСПОРТ В EXCEL
# ============================

async def export_to_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт данных в Excel с русскими названиями столбцов"""
    user = update.effective_user
    
    if not await is_admin(user.id, user.username):
        await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        return
    
    try:
        # Показываем сообщение о начале экспорта
        await update.callback_query.edit_message_text("📊 Начинаю экспорт данных...")
        
        # Создаем Excel файл в памяти
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Используем синхронное соединение для pandas
            with sqlite3.connect(DB_PATH) as conn:
                
                # 1. ЗАПИСИ НА МАССАЖ
                await context.bot.edit_message_text(
                    chat_id=user.id,
                    message_id=update.callback_query.message.message_id,
                    text="📊 Экспорт: Записи на массаж..."
                )
                query = '''
                SELECT 
                    a.id as "№ записи",
                    c.name as "Имя клиента",
                    c.telegram_id as "Telegram ID",
                    c.phone as "Телефон",
                    s.name as "Услуга",
                    m.name as "Массажист",
                    a.appointment_time as "Дата и время записи",
                    a.status as "Статус",
                    a.created_at as "Дата создания",
                    a.updated_at as "Дата обновления",
                    CASE 
                        WHEN a.status = 'active' AND a.appointment_time > datetime('now') THEN 'активная'
                        WHEN a.status = 'active' AND a.appointment_time <= datetime('now') THEN 'прошедшая'
                        ELSE a.status
                    END as "Статус записи",
                    s.price as "Стоимость"
                FROM appointments a
                JOIN clients c ON a.client_id = c.id
                JOIN services s ON a.service_id = s.id
                JOIN masters m ON a.master_id = m.id
                ORDER BY a.appointment_time DESC
                '''
                df_appointments = pd.read_sql_query(query, conn)
                df_appointments.to_excel(writer, sheet_name='Записи', index=False)
                
                # 2. КЛИЕНТЫ
                await context.bot.edit_message_text(
                    chat_id=user.id,
                    message_id=update.callback_query.message.message_id,
                    text="📊 Экспорт: Клиенты..."
                )
                query = '''
                SELECT 
                    id as "ID клиента",
                    name as "Имя",
                    username as "Username",
                    telegram_id as "Telegram ID",
                    phone as "Телефон",
                    created_at as "Дата регистрации",
                    (SELECT COUNT(*) FROM appointments WHERE client_id = clients.id) as "Всего записей",
                    (SELECT COUNT(*) FROM appointments WHERE client_id = clients.id AND status = 'completed') as "Завершенных записей",
                    (SELECT COUNT(*) FROM appointments WHERE client_id = clients.id AND status = 'cancelled') as "Отмененных записей",
                    (SELECT SUM(s.price) FROM appointments a 
                     JOIN services s ON a.service_id = s.id 
                     WHERE a.client_id = clients.id AND a.status = 'completed') as "Всего потрачено",
                    (SELECT MAX(appointment_time) FROM appointments WHERE client_id = clients.id) as "Последняя запись",
                    (SELECT AVG(rating) FROM reviews WHERE client_id = clients.id) as "Средний рейтинг"
                FROM clients
                ORDER BY created_at DESC
                '''
                df_clients = pd.read_sql_query(query, conn)
                df_clients.to_excel(writer, sheet_name='Клиенты', index=False)
                
                # 3. УСЛУГИ И ЦЕНЫ
                await context.bot.edit_message_text(
                    chat_id=user.id,
                    message_id=update.callback_query.message.message_id,
                    text="📊 Экспорт: Услуги и цены..."
                )
                query = '''
                SELECT 
                    s.id as "ID услуги",
                    s.name as "Название услуги",
                    s.duration as "Длительность (мин)",
                    s.price as "Цена (руб)",
                    COUNT(a.id) as "Всего заказов",
                    SUM(CASE WHEN a.status = 'completed' THEN 1 ELSE 0 END) as "Завершенных заказов",
                    SUM(CASE WHEN a.status = 'cancelled' THEN 1 ELSE 0 END) as "Отмененных заказов",
                    SUM(CASE WHEN a.status = 'completed' THEN s.price ELSE 0 END) as "Общий доход",
                    AVG(r.rating) as "Средний рейтинг",
                    ROUND(100.0 * SUM(CASE WHEN a.status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 2) as "Процент выполнения (%)"
                FROM services s
                LEFT JOIN appointments a ON s.id = a.service_id
                LEFT JOIN reviews r ON r.service_id = s.id
                GROUP BY s.id
                ORDER BY s.id
                '''
                df_services = pd.read_sql_query(query, conn)
                df_services.to_excel(writer, sheet_name='Услуги', index=False)
                
                # 4. МАССАЖИСТЫ
                await context.bot.edit_message_text(
                    chat_id=user.id,
                    message_id=update.callback_query.message.message_id,
                    text="📊 Экспорт: Массажисты..."
                )
                query = '''
                SELECT 
                    m.id as "ID массажиста",
                    m.name as "Имя массажиста",
                    CASE 
                        WHEN m.active = 1 THEN 'активен'
                        ELSE 'не активен'
                    END as "Статус",
                    COUNT(a.id) as "Всего записей",
                    COUNT(CASE WHEN a.status = 'completed' THEN 1 END) as "Завершенных",
                    COUNT(CASE WHEN a.status = 'active' AND a.appointment_time > datetime('now') THEN 1 END) as "Предстоящих",
                    COUNT(CASE WHEN a.status = 'cancelled' THEN 1 END) as "Отмененных",
                    AVG(r.rating) as "Средний рейтинг",
                    COUNT(DISTINCT c.id) as "Уникальных клиентов",
                    SUM(CASE WHEN a.status = 'completed' THEN s.price ELSE 0 END) as "Принесенный доход",
                    ROUND(100.0 * COUNT(CASE WHEN a.status = 'completed' THEN 1 END) / COUNT(*), 2) as "Процент выполнения (%)",
                    (SELECT GROUP_CONCAT(DISTINCT s.name) 
                     FROM appointments a2 
                     JOIN services s ON a2.service_id = s.id 
                     WHERE a2.master_id = m.id) as "Предоставляемые услуги"
                FROM masters m
                LEFT JOIN appointments a ON m.id = a.master_id
                LEFT JOIN reviews r ON m.id = r.master_id
                LEFT JOIN clients c ON a.client_id = c.id
                LEFT JOIN services s ON a.service_id = s.id
                GROUP BY m.id
                ORDER BY m.id
                '''
                df_masters = pd.read_sql_query(query, conn)
                df_masters.to_excel(writer, sheet_name='Массажисты', index=False)
                
                # 5. ОТЗЫВЫ
                await context.bot.edit_message_text(
                    chat_id=user.id,
                    message_id=update.callback_query.message.message_id,
                    text="📊 Экспорт: Отзывы..."
                )
                query = '''
                SELECT 
                    r.id as "№ отзыва",
                    c.name as "Имя клиента",
                    m.name as "Массажист",
                    s.name as "Услуга",
                    r.rating as "Оценка (1-5)",
                    CASE 
                        WHEN r.rating = 5 THEN '⭐⭐⭐⭐⭐ Отлично'
                        WHEN r.rating = 4 THEN '⭐⭐⭐⭐ Хорошо'
                        WHEN r.rating = 3 THEN '⭐⭐⭐ Удовлетворительно'
                        WHEN r.rating = 2 THEN '⭐⭐ Плохо'
                        WHEN r.rating = 1 THEN '⭐ Очень плохо'
                    END as "Текст оценки",
                    r.comment as "Комментарий",
                    r.created_at as "Дата отзыва",
                    CASE 
                        WHEN LENGTH(r.comment) > 0 THEN 'с комментарием'
                        ELSE 'без комментария'
                    END as "Тип отзыва"
                FROM reviews r
                JOIN clients c ON r.client_id = c.id
                JOIN masters m ON r.master_id = m.id
                JOIN services s ON r.service_id = s.id
                ORDER BY r.created_at DESC
                '''
                df_reviews = pd.read_sql_query(query, conn)
                df_reviews.to_excel(writer, sheet_name='Отзывы', index=False)
                
                # 6. ФИНАНСОВЫЙ ОТЧЕТ ПО МЕСЯЦАМ
                await context.bot.edit_message_text(
                    chat_id=user.id,
                    message_id=update.callback_query.message.message_id,
                    text="📊 Экспорт: Финансовый отчет..."
                )
                query = '''
                SELECT 
                    strftime('%Y-%m', a.appointment_time) as "Месяц",
                    COUNT(*) as "Всего заказов",
                    SUM(CASE WHEN a.status = 'completed' THEN s.price ELSE 0 END) as "Выручка (руб)",
                    SUM(CASE WHEN a.status = 'cancelled' THEN s.price ELSE 0 END) as "Потерянная выручка (руб)",
                    COUNT(CASE WHEN a.status = 'completed' THEN 1 END) as "Завершенных заказов",
                    COUNT(CASE WHEN a.status = 'cancelled' THEN 1 END) as "Отмененных заказов",
                    ROUND(100.0 * COUNT(CASE WHEN a.status = 'completed' THEN 1 END) / COUNT(*), 2) as "Процент выполнения (%)",
                    AVG(r.rating) as "Средний рейтинг",
                    ROUND(AVG(s.price), 2) as "Средний чек (руб)",
                    COUNT(DISTINCT c.id) as "Уникальных клиентов",
                    (SELECT GROUP_CONCAT(DISTINCT m.name) 
                     FROM appointments a2 
                     JOIN masters m ON a2.master_id = m.id 
                     WHERE strftime('%Y-%m', a2.appointment_time) = strftime('%Y-%m', a.appointment_time)) as "Работавшие массажисты"
                FROM appointments a
                JOIN services s ON a.service_id = s.id
                JOIN clients c ON a.client_id = c.id
                LEFT JOIN reviews r ON r.appointment_id = a.id
                GROUP BY strftime('%Y-%m', a.appointment_time)
                ORDER BY "Месяц" DESC
                '''
                df_financial = pd.read_sql_query(query, conn)
                df_financial.to_excel(writer, sheet_name='Финансовый отчет', index=False)
                
                # 7. ЕЖЕДНЕВНАЯ СТАТИСТИКА (30 ДНЕЙ)
                await context.bot.edit_message_text(
                    chat_id=user.id,
                    message_id=update.callback_query.message.message_id,
                    text="📊 Экспорт: Ежедневная статистика..."
                )
                query = '''
                SELECT 
                    DATE(a.appointment_time) as "Дата",
                    strftime('%w', a.appointment_time) as "День недели (0-6)",
                    CASE strftime('%w', a.appointment_time)
                        WHEN '0' THEN 'Воскресенье'
                        WHEN '1' THEN 'Понедельник'
                        WHEN '2' THEN 'Вторник'
                        WHEN '3' THEN 'Среда'
                        WHEN '4' THEN 'Четверг'
                        WHEN '5' THEN 'Пятница'
                        WHEN '6' THEN 'Суббота'
                    END as "День недели",
                    COUNT(*) as "Всего записей",
                    COUNT(CASE WHEN a.status = 'completed' THEN 1 END) as "Завершенных",
                    COUNT(CASE WHEN a.status = 'cancelled' THEN 1 END) as "Отмененных",
                    SUM(CASE WHEN a.status = 'completed' THEN s.price ELSE 0 END) as "Дневная выручка (руб)",
                    AVG(r.rating) as "Средний рейтинг",
                    COUNT(DISTINCT m.id) as "Кол-во массажистов",
                    COUNT(DISTINCT c.id) as "Кол-во клиентов",
                    GROUP_CONCAT(DISTINCT m.name) as "Массажисты дня",
                    GROUP_CONCAT(DISTINCT s.name) as "Услуги дня"
                FROM appointments a
                LEFT JOIN services s ON a.service_id = s.id
                LEFT JOIN reviews r ON r.appointment_id = a.id
                LEFT JOIN masters m ON a.master_id = m.id
                LEFT JOIN clients c ON a.client_id = c.id
                WHERE DATE(a.appointment_time) > date('now', '-30 days')
                GROUP BY DATE(a.appointment_time)
                ORDER BY "Дата" DESC
                '''
                df_daily_stats = pd.read_sql_query(query, conn)
                df_daily_stats.to_excel(writer, sheet_name='Ежедневная статистика', index=False)
                
                # 8. АНАЛИТИКА ПО ЧАСАМ
                await context.bot.edit_message_text(
                    chat_id=user.id,
                    message_id=update.callback_query.message.message_id,
                    text="📊 Экспорт: Аналитика по часам..."
                )
                query = '''
                SELECT 
                    strftime('%H:00', a.appointment_time) as "Час",
                    COUNT(*) as "Кол-во записей",
                    COUNT(CASE WHEN a.status = 'completed' THEN 1 END) as "Завершенные",
                    COUNT(CASE WHEN a.status = 'cancelled' THEN 1 END) as "Отмененные",
                    ROUND(100.0 * COUNT(CASE WHEN a.status = 'completed' THEN 1 END) / COUNT(*), 2) as "Процент выполнения (%)",
                    SUM(CASE WHEN a.status = 'completed' THEN s.price ELSE 0 END) as "Выручка (руб)",
                    AVG(r.rating) as "Средний рейтинг",
                    GROUP_CONCAT(DISTINCT m.name) as "Массажисты",
                    GROUP_CONCAT(DISTINCT s.name) as "Услуги"
                FROM appointments a
                JOIN services s ON a.service_id = s.id
                LEFT JOIN reviews r ON r.appointment_id = a.id
                LEFT JOIN masters m ON a.master_id = m.id
                WHERE a.status = 'completed'
                GROUP BY strftime('%H:00', a.appointment_time)
                ORDER BY "Час"
                '''
                df_hourly_stats = pd.read_sql_query(query, conn)
                df_hourly_stats.to_excel(writer, sheet_name='Аналитика по часам', index=False)
        
        output.seek(0)
        
        # Отправляем файл
        await context.bot.send_document(
            chat_id=user.id,
            document=output,
            filename=f'массажный_салон_экспорт_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
            caption='📊 Полный экспорт данных массажного салона (8 листов)'
        )
        
        # Возвращаемся в админ-панель
        keyboard = [get_back_button("admin_panel")]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            "✅ Данные успешно экспортированы в Excel файл (8 листов).\n"
            "📁 Файл отправлен вам в личные сообщения.\n\n"
            "📋 *Содержание файла:*\n"
            "1️⃣ Записи - все записи на массаж\n"
            "2️⃣ Клиенты - информация о клиентах\n"
            "3️⃣ Услуги - услуги и статистика\n"
            "4️⃣ Массажисты - работа и статистика\n"
            "5️⃣ Отзывы - оценки и комментарии\n"
            "6️⃣ Финансовый отчет - доходы по месяцам\n"
            "7️⃣ Ежедневная статистика - статистика за 30 дней\n"
            "8️⃣ Аналитика по часам - популярное время",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Ошибка экспорта в Excel: {e}")
        keyboard = [get_back_button("admin_panel")]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            f"❌ Ошибка при экспорте данных: {str(e)}",
            reply_markup=reply_markup
        )

# ============================
# СИСТЕМА ОТЗЫВОВ
# ============================

async def ask_for_review(update: Update, context: ContextTypes.DEFAULT_TYPE, appointment_id: int):
    """Запрос отзыва после завершения сеанса"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT c.telegram_id, s.name, m.name
        FROM appointments a
        JOIN clients c ON a.client_id = c.id
        JOIN services s ON a.service_id = s.id
        JOIN masters m ON a.master_id = m.id
        WHERE a.id = ?
        ''', (appointment_id,))
        
        appointment = await cursor.fetchone()
        await cursor.close()
    
    if appointment:
        client_id, service_name, master_name = appointment
        
        keyboard = [
            [
                InlineKeyboardButton("⭐ 1", callback_data=f"review_{appointment_id}_1"),
                InlineKeyboardButton("⭐⭐ 2", callback_data=f"review_{appointment_id}_2"),
                InlineKeyboardButton("⭐⭐⭐ 3", callback_data=f"review_{appointment_id}_3"),
            ],
            [
                InlineKeyboardButton("⭐⭐⭐⭐ 4", callback_data=f"review_{appointment_id}_4"),
                InlineKeyboardButton("⭐⭐⭐⭐⭐ 5", callback_data=f"review_{appointment_id}_5"),
            ],
            [InlineKeyboardButton("Пропустить", callback_data=f"review_{appointment_id}_skip")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await context.bot.send_message(
                chat_id=client_id,
                text=f"🙏 *Пожалуйста, оставьте отзыв о сеансе:*\n\n"
                     f"Услуга: {service_name}\n"
                     f"Массажист: {master_name}\n\n"
                     f"*Оцените от 1 до 5 звезд:*",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Ошибка отправки запроса на отзыв: {e}")

async def save_review(update: Update, context: ContextTypes.DEFAULT_TYPE, appointment_id: int, rating: int):
    """Сохранение отзыва"""
    query = update.callback_query
    await query.answer()
    
    if rating == -1:  # Пропуск
        await query.edit_message_text("✅ Спасибо! Если захотите оставить отзыв позже, используйте команду /review")
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Получаем информацию о записи
        cursor = await db.execute('''
        SELECT a.client_id, a.master_id, a.service_id
        FROM appointments a
        WHERE a.id = ?
        ''', (appointment_id,))
        
        appointment_info = await cursor.fetchone()
        await cursor.close()
        
        if appointment_info:
            client_id, master_id, service_id = appointment_info
            
            # Проверяем, есть ли уже отзыв на эту запись
            cursor = await db.execute('''
            SELECT id FROM reviews WHERE appointment_id = ?
            ''', (appointment_id,))
            
            existing_review = await cursor.fetchone()
            await cursor.close()
            
            if existing_review:
                await query.edit_message_text("❌ Вы уже оставили отзыв на этот сеанс.")
                return
            
            # Запрашиваем комментарий
            context.user_data['review_data'] = {
                'appointment_id': appointment_id,
                'client_id': client_id,
                'master_id': master_id,
                'service_id': service_id,
                'rating': rating
            }
            
            await query.edit_message_text(
                f"⭐ Вы поставили оценку: {rating}/5\n\n"
                f"💬 *Напишите комментарий (необязательно):*\n"
                f"Максимум 500 символов.\n\n"
                f"Или нажмите 'Пропустить' чтобы оставить только оценку.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="review_comment_skip")]]),
                parse_mode="Markdown"
            )

async def save_review_comment(update: Update, context: ContextTypes.DEFAULT_TYPE, comment: str = None):
    """Сохранение комментария к отзыву"""
    query = update.callback_query
    await query.answer()
    
    review_data = context.user_data.get('review_data')
    if not review_data:
        await query.edit_message_text("❌ Ошибка: данные отзыва не найдены.")
        return
    
    # Сохраняем отзыв в базу
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
        INSERT INTO reviews (appointment_id, client_id, master_id, service_id, rating, comment)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            review_data['appointment_id'],
            review_data['client_id'],
            review_data['master_id'],
            review_data['service_id'],
            review_data['rating'],
            comment[:500] if comment else None
        ))
        await db.commit()
    
    # Очищаем данные
    context.user_data.pop('review_data', None)
    
    # Отправляем уведомление администратору
    await send_review_notification(context.application, review_data['appointment_id'], review_data['rating'], comment)
    
    await query.edit_message_text(
        "✅ *Спасибо за ваш отзыв!* 🌟\n\n"
        "Ваше мнение очень важно для нас и помогает становиться лучше.",
        parse_mode="Markdown"
    )

async def send_review_notification(application, appointment_id: int, rating: int, comment: str = None):
    """Уведомление администратора о новом отзыве"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT c.name, m.name, s.name
        FROM appointments a
        JOIN clients c ON a.client_id = c.id
        JOIN masters m ON a.master_id = m.id
        JOIN services s ON a.service_id = s.id
        WHERE a.id = ?
        ''', (appointment_id,))
        
        appointment = await cursor.fetchone()
        await cursor.close()
    
    if appointment:
        client_name, master_name, service_name = appointment
        
        stars = "⭐" * rating
        
        message = (
            "🌟 *НОВЫЙ ОТЗЫВ!*\n\n"
            f"👤 Клиент: {client_name}\n"
            f"👨‍⚕️ Массажист: {master_name}\n"
            f"🏷 Услуга: {service_name}\n"
            f"⭐ Оценка: {rating}/5 {stars}\n"
        )
        
        if comment:
            message += f"💬 Комментарий: {comment}\n"
        
        message += f"\n🎫 Номер записи: #{appointment_id}"
        
        await send_admin_notification(application, message)

# ============================
# УВЕДОМЛЕНИЯ
# ============================

async def send_notification(application, chat_id: int, message: str, parse_mode="Markdown"):
    """Отправка уведомления с обработкой ошибок и метриками"""
    start_time = datetime.now()
    
    try:
        success = await safe_sender.send_message(
            application.bot,
            chat_id,
            message,
            parse_mode=parse_mode
        )
        
        # Записываем метрики
        response_time = (datetime.now() - start_time).total_seconds()
        metrics_collector.record_response_time(response_time)
        metrics_collector.increment_counter('messages_sent')
        
        if not success:
            metrics_collector.increment_counter('failed_messages')
            
        return success
        
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления: {e}")
        metrics_collector.increment_counter('errors')
        return False

async def send_admin_notification(application, message: str):
    """Отправка уведомления всем администраторам"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT telegram_id FROM admins")
        admins = await cursor.fetchall()
        await cursor.close()
        
        for admin in admins:
            admin_id = admin[0]
            await send_notification(application, admin_id, message)

async def send_new_appointment_notification(application, appointment_id: int):
    """Уведомление администратору о новой записи"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT c.name, c.telegram_id, c.username, c.phone, s.name, m.name, a.appointment_time
        FROM appointments a
        JOIN clients c ON a.client_id = c.id
        JOIN services s ON a.service_id = s.id
        JOIN masters m ON a.master_id = m.id
        WHERE a.id = ?
        ''', (appointment_id,))
        
        appointment = await cursor.fetchone()
        await cursor.close()
    
    if appointment:
        client_name, telegram_id, username, phone, service_name, master_name, appointment_time = appointment
        time_str = datetime.strptime(appointment_time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
        
        user_mention = f"@{username}" if username else f"ID: {telegram_id}"
        
        message = (
            "📋 *НОВАЯ ЗАПИСЬ!*\n\n"
            f"🎫 Номер: #{appointment_id}\n"
            f"👤 Клиент: {client_name}\n"
            f"📱 Телеграм: {user_mention}\n"
            f"📞 Телефон: {phone or 'не указан'}\n"
            f"🏷 Услуга: {service_name}\n"
            f"👨‍⚕️ Массажист: {master_name}\n"
            f"📅 Дата и время: {time_str}"
        )
        
        await send_admin_notification(application, message)

async def send_cancellation_notification(application, appointment_id: int, cancelled_by: str = "клиент"):
    """Уведомление об отмене записи"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT c.name, c.telegram_id, c.username, s.name, m.name, a.appointment_time
        FROM appointments a
        JOIN clients c ON a.client_id = c.id
        JOIN services s ON a.service_id = s.id
        JOIN masters m ON a.master_id = m.id
        WHERE a.id = ?
        ''', (appointment_id,))
        
        appointment = await cursor.fetchone()
        await cursor.close()
    
    if appointment:
        client_name, telegram_id, username, service_name, master_name, appointment_time = appointment
        time_str = datetime.strptime(appointment_time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
        
        user_mention = f"@{username}" if username else f"ID: {telegram_id}"
        
        message = (
            "❌ *ОТМЕНА ЗАПИСИ!*\n\n"
            f"🎫 Номер записи: #{appointment_id}\n"
            f"👤 Клиент: {client_name}\n"
            f"📱 Телеграм: {user_mention}\n"
            f"🏷 Услуга: {service_name}\n"
            f"👨‍⚕️ Массажист: {master_name}\n"
            f"📅 Дата и время: {time_str}\n"
            f"📝 Отменено: {cancelled_by}"
        )
        
        await send_admin_notification(application, message)

async def schedule_reminders(application):
    """Планировщик напоминаний"""
    while True:
        try:
            now = datetime.now()
            
            async with aiosqlite.connect(DB_PATH) as db:
                # Автоматическое завершение прошедших записей
                past_appointments = await db.execute('''
                SELECT a.id, c.telegram_id
                FROM appointments a
                JOIN clients c ON a.client_id = c.id
                WHERE a.appointment_time < ?
                AND a.status = 'active'
                ''', (now.strftime('%Y-%m-%d %H:%M:%S'),))
                
                past_apps = await past_appointments.fetchall()
                await past_appointments.close()
                
                for app_id, telegram_id in past_apps:
                    await db.execute(
                        "UPDATE appointments SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (app_id,)
                    )
                    await db.commit()
                    
                    # Запрашиваем отзыв через 10 минут после завершения
                    await asyncio.sleep(600)  # 10 минут
                    await ask_for_review(None, ContextTypes.DEFAULT_TYPE, app_id)
                
                # Напоминание за день
                day_later = now + timedelta(days=1)
                day_start = day_later.replace(hour=0, minute=0, second=0)
                day_end = day_later.replace(hour=23, minute=59, second=59)
                
                cursor = await db.execute('''
                SELECT a.id, c.telegram_id, s.name, m.name, a.appointment_time
                FROM appointments a
                JOIN clients c ON a.client_id = c.id
                JOIN services s ON a.service_id = s.id
                JOIN masters m ON a.master_id = m.id
                WHERE a.appointment_time BETWEEN ? AND ?
                AND a.status = 'active'
                AND a.reminder_sent_day = 0
                ''', (day_start.strftime('%Y-%m-%d %H:%M:%S'), 
                      day_end.strftime('%Y-%m-%d %H:%M:%S')))
                
                day_appointments = await cursor.fetchall()
                await cursor.close()
                
                for app in day_appointments:
                    app_id, telegram_id, service_name, master_name, appointment_time = app
                    time_str = datetime.strptime(appointment_time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
                    
                    message = (
                        "🔔 *Напоминание за день!*\n\n"
                        f"Завтра у вас запись на {service_name}\n"
                        f"Массажист: {master_name}\n"
                        f"Время: {time_str}\n\n"
                        "Пожалуйста, не опаздывайте!"
                    )
                    
                    await send_notification(application, telegram_id, message)
                    
                    await db.execute(
                        "UPDATE appointments SET reminder_sent_day = 1 WHERE id = ?",
                        (app_id,)
                    )
                    await db.commit()
                
                # Напоминание за час
                hour_later = now + timedelta(hours=1)
                hour_start = hour_later.replace(minute=0, second=0)
                hour_end = hour_later.replace(minute=59, second=59)
                
                cursor = await db.execute('''
                SELECT a.id, c.telegram_id, s.name, m.name, a.appointment_time
                FROM appointments a
                JOIN clients c ON a.client_id = c.id
                JOIN services s ON a.service_id = s.id
                JOIN masters m ON a.master_id = m.id
                WHERE a.appointment_time BETWEEN ? AND ?
                AND a.status = 'active'
                AND a.reminder_sent_hour = 0
                ''', (hour_start.strftime('%Y-%m-%d %H:%M:%S'), 
                      hour_end.strftime('%Y-%m-%d %H:%M:%S')))
                
                hour_appointments = await cursor.fetchall()
                await cursor.close()
                
                for app in hour_appointments:
                    app_id, telegram_id, service_name, master_name, appointment_time = app
                    time_str = datetime.strptime(appointment_time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
                    
                    message = (
                        "🔔 *Напоминание за час!*\n\n"
                        f"Через час у вас запись на {service_name}\n"
                        f"Массажист: {master_name}\n"
                        f"Время: {time_str}\n\n"
                        "Пожалуйста, не опаздывайте!"
                    )
                    
                    await send_notification(application, telegram_id, message)
                    
                    await db.execute(
                        "UPDATE appointments SET reminder_sent_hour = 1 WHERE id = ?",
                        (app_id,)
                    )
                    await db.commit()
                
                # Напоминание администратору за 10 минут
                ten_min_later = now + timedelta(minutes=10)
                ten_min_start = ten_min_later.replace(second=0)
                ten_min_end = ten_min_later.replace(second=59)
                
                cursor = await db.execute('''
                SELECT a.id, c.name, s.name, m.name, a.appointment_time
                FROM appointments a
                JOIN clients c ON a.client_id = c.id
                JOIN services s ON a.service_id = s.id
                JOIN masters m ON a.master_id = m.id
                WHERE a.appointment_time BETWEEN ? AND ?
                AND a.status = 'active'
                AND a.reminder_sent_admin = 0
                ''', (ten_min_start.strftime('%Y-%m-%d %H:%M:%S'), 
                      ten_min_end.strftime('%Y-%m-%d %H:%M:%S')))
                
                admin_appointments = await cursor.fetchall()
                await cursor.close()
                
                for app in admin_appointments:
                    app_id, client_name, service_name, master_name, appointment_time = app
                    time_str = datetime.strptime(appointment_time, '%Y-%m-%d %H:%M:%S').strftime('%H:%M')
                    
                    message = (
                        "⏰ *Клиент скоро придет!*\n\n"
                        f"Через 10 минут: {client_name}\n"
                        f"Услуга: {service_name}\n"
                        f"Массажист: {master_name}\n"
                        f"Время: {time_str}"
                    )
                    
                    await send_admin_notification(application, message)
                    
                    await db.execute(
                        "UPDATE appointments SET reminder_sent_admin = 1 WHERE id = ?",
                        (app_id,)
                    )
                    await db.commit()
            
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Ошибка в планировщике напоминаний: {e}")
            await asyncio.sleep(60)

# ============================
# ОСНОВНЫЕ КОМАНДЫ
# ============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню"""
    user = update.effective_user

    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("start")
        metrics_collector.log_active_user(user.id)
    except:
        pass

    if user.username and ADMIN_USERNAME.lower() == f"@{user.username}".lower():
        await add_admin(user.id, user.username, user.full_name)
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем, существует ли клиент
        cursor = await db.execute(
            "SELECT id FROM clients WHERE telegram_id = ? OR username = ?",
            (user.id, user.username)
        )
        existing_client = await cursor.fetchone()
        await cursor.close()
        
        if not existing_client:
            # Создаем нового клиента
            await db.execute(
                "INSERT INTO clients (telegram_id, username, name) VALUES (?, ?, ?)",
                (user.id, user.username, user.full_name)
            )
            await db.commit()
    
    keyboard = [
        [InlineKeyboardButton("📅 Записаться на массаж", callback_data="book_appointment")],
        [InlineKeyboardButton("📋 Мои активные записи", callback_data="my_appointments")],
        [InlineKeyboardButton("📜 История записей", callback_data="my_all_appointments")],
        [InlineKeyboardButton("👨‍💼 Контакты салона", callback_data="contacts")],
        [InlineKeyboardButton("💵 Услуги и цены", callback_data="services")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            f"👋 Добро пожаловать, {user.first_name}!\n\n"
            f"Вы в главном меню массажного салона 'Релакс'!\n\n"
            f"Выберите действие:",
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            f"👋 Привет, {user.first_name}!\n\n"
            f"Добро пожаловать в массажный салон 'Релакс'!\n\n"
            f"Выберите действие:",
            reply_markup=reply_markup
        )

async def book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса записи"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("book")
        metrics_collector.log_active_user(user.id)
    except:
        pass

    keyboard = [
        [InlineKeyboardButton("1️⃣ Классический (60 мин - 1000₽)", callback_data="service_1")],
        [InlineKeyboardButton("2️⃣ Спортивный (60 мин - 1000₽)", callback_data="service_2")],
        [InlineKeyboardButton("3️⃣ Перкуссионный (60 мин - 1500₽)", callback_data="service_3")],
        [InlineKeyboardButton("4️⃣ Вакуумный (45 мин - 1500₽)", callback_data="service_4")],
        get_back_button("main_menu")
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "🏆 *Выберите тип массажа:*\n\n"
            "1. Классический - 60 мин, 1000₽\n"
            "2. Спортивный - 60 мин, 1000₽\n"
            "3. Перкуссионный - 60 мин, 1500₽\n"
            "4. Вакуумный - 45 мин, 1500₽",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "🏆 *Выберите тип массажа:*\n\n"
            "1. Классический - 60 мин, 1000₽\n"
            "2. Спортивный - 60 мин, 1000₽\n"
            "3. Перкуссионный - 60 мин, 1500₽\n"
            "4. Вакуумный - 45 мин, 1500₽",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def my_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Мои активные записи - ТОЛЬКО АКТИВНЫЕ И БУДУЩИЕ"""
    user = update.effective_user

    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("my_appointments")
        metrics_collector.log_active_user(user.id)
    except:
        pass    

    async with aiosqlite.connect(DB_PATH) as db:
        # Ищем клиента по telegram_id
        cursor = await db.execute('''
        SELECT id FROM clients 
        WHERE telegram_id = ?
        LIMIT 1
        ''', (user.id,))
        
        client = await cursor.fetchone()
        await cursor.close()
        
        if not client:
            # Если клиента нет, создаем его
            await db.execute(
                "INSERT INTO clients (telegram_id, username, name) VALUES (?, ?, ?)",
                (user.id, user.username, user.full_name)
            )
            await db.commit()
            
            cursor = await db.execute('SELECT id FROM clients WHERE telegram_id = ?', (user.id,))
            client = await cursor.fetchone()
            await cursor.close()
        
        client_id = client[0]
        
        cursor = await db.execute('''
        SELECT a.id, s.name, m.name, a.appointment_time, a.status
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        JOIN masters m ON a.master_id = m.id
        WHERE a.client_id = ? 
        AND a.status = 'active'
        AND a.appointment_time > datetime('now')
        ORDER BY a.appointment_time ASC
        ''', (client_id,))
        
        appointments = await cursor.fetchall()
        await cursor.close()
    
    if not appointments:
        keyboard = [get_back_button("main_menu")]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                "📭 У вас нет активных записей на будущее.",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "📭 У вас нет активных записей на будущее.",
                reply_markup=reply_markup
            )
        return
    
    appointments_text = "📋 *Ваши активные записи (на будущее):*\n\n"
    keyboard_rows = []
    
    for app in appointments:
        app_id, service, master, time, status = app
        time_str = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
        
        appointments_text += f"🟢 #{app_id}: {service}\n"
        appointments_text += f"  Массажист: {master}\n"
        appointments_text += f"  Время: {time_str}\n\n"
        
        # Добавляем кнопку отмены для каждой активной записи
        keyboard_rows.append([InlineKeyboardButton(
            f"❌ Отменить запись #{app_id}",
            callback_data=f"cancel_my_{app_id}"
        )])
    
    keyboard_rows.append(get_back_button("main_menu"))
    reply_markup = InlineKeyboardMarkup(keyboard_rows)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            appointments_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            appointments_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def cancel_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, appointment_id: int):
    """Отмена записи клиентом"""
    user = update.effective_user
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем, что запись принадлежит пользователю
        cursor = await db.execute('''
        SELECT a.id, c.id as client_id, c.telegram_id, s.name, m.name, a.appointment_time
        FROM appointments a
        JOIN clients c ON a.client_id = c.id
        JOIN services s ON a.service_id = s.id
        JOIN masters m ON a.master_id = m.id
        WHERE a.id = ? AND a.status = 'active'
        ''', (appointment_id,))
        
        appointment = await cursor.fetchone()
        await cursor.close()
        
        if not appointment:
            await update.callback_query.edit_message_text(
                "❌ Запись не найдена или уже отменена.",
                reply_markup=InlineKeyboardMarkup([get_back_button("my_appointments")])
            )
            return
        
        app_id, client_db_id, client_id, service_name, master_name, appointment_time = appointment
        
        cursor = await db.execute('''
        SELECT id FROM clients 
        WHERE id = ? AND telegram_id = ?
        ''', (client_db_id, user.id))
        
        is_owner = await cursor.fetchone()
        await cursor.close()
        
        if not is_owner:
            await update.callback_query.edit_message_text(
                "❌ Вы не можете отменить эту запись.",
                reply_markup=InlineKeyboardMarkup([get_back_button("my_appointments")])
            )
            return
        
        await db.execute(
            "UPDATE appointments SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (appointment_id,)
        )
        await db.commit()
    
    await send_cancellation_notification(context.application, appointment_id, "клиент")
    
    time_str = datetime.strptime(appointment_time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
    
    message = (
        "❌ *Запись отменена*\n\n"
        f"Запись #{appointment_id} успешно отменена.\n"
        f"Услуга: {service_name}\n"
        f"Массажист: {master_name}\n"
        f"Время: {time_str}\n\n"
        f"Администратор уведомлен об отмене.\n"
        f"Запись сохранится в истории."
    )
    
    keyboard = [
        [InlineKeyboardButton("📋 Мои активные записи", callback_data="my_appointments")],
        [InlineKeyboardButton("📜 История записей", callback_data="my_all_appointments")],
        get_back_button("main_menu")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def my_all_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ВСЕ мои записи (включая историю) - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    user = update.effective_user

    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("my_all_appointments")
        metrics_collector.log_active_user(user.id)
    except:
        pass
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Ищем клиента по telegram_id
        cursor = await db.execute('''
        SELECT id FROM clients 
        WHERE telegram_id = ?
        LIMIT 1
        ''', (user.id,))
        
        client = await cursor.fetchone()
        await cursor.close()
        
        if not client:
            keyboard = [get_back_button("main_menu")]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if update.callback_query:
                await update.callback_query.edit_message_text(
                    "📭 У вас нет записей.",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text(
                    "📭 У вас нет записей.",
                    reply_markup=reply_markup
                )
            return
        
        client_id = client[0]
        
        cursor = await db.execute('''
        SELECT a.id, s.name, m.name, a.appointment_time, a.status, a.updated_at
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        JOIN masters m ON a.master_id = m.id
        WHERE a.client_id = ?
        ORDER BY a.appointment_time DESC
        LIMIT 30
        ''', (client_id,))
        
        appointments = await cursor.fetchall()
        await cursor.close()
    
    if not appointments:
        keyboard = [
            get_back_button("main_menu")
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                "📭 У вас нет записей.",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "📭 У вас нет записей.",
                reply_markup=reply_markup
            )
        return
    
    appointments_text = "📋 *Вся история ваших записей (последние 30):*\n\n"
    
    for app in appointments:
        app_id, service, master, time, status, updated_at = app
        time_str = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
        
        if status == 'active':
            # Проверяем, прошла ли уже запись
            appointment_dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            if appointment_dt < now:
                status_emoji = "🕐"
                status_text = "прошла"
            else:
                status_emoji = "🟢"
                status_text = "активна"
        elif status == 'completed':
            status_emoji = "✅"
            status_text = "завершена"
        elif status == 'cancelled':
            status_emoji = "❌"
            status_text = "отменена"
        else:
            status_emoji = "⚪"
            status_text = status
        
        appointments_text += f"{status_emoji} #{app_id}: {service}\n"
        appointments_text += f"  Массажист: {master}\n"
        appointments_text += f"  Время: {time_str}\n"
        appointments_text += f"  Статус: {status_text}\n"
        
        # Добавляем дату изменения для завершенных/отмененных записей
        if status in ['completed', 'cancelled'] and updated_at:
            updated_str = datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y')
            appointments_text += f"  Обновлено: {updated_str}"
            
        appointments_text += "\n\n"
    
    keyboard = [
        [InlineKeyboardButton("🗑 Очистить историю", callback_data="clear_history")],
        get_back_button("main_menu")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            appointments_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            appointments_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def clear_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистка истории записей пользователя (только завершенных и отмененных)"""
    user = update.effective_user
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT id FROM clients 
        WHERE telegram_id = ?
        LIMIT 1
        ''', (user.id,))
        
        client = await cursor.fetchone()
        await cursor.close()
        
        if not client:
            await update.callback_query.edit_message_text(
                "❌ Клиент не найден.",
                reply_markup=InlineKeyboardMarkup([get_back_button("main_menu")])
            )
            return
        
        client_id = client[0]
        
        cursor = await db.execute('''
        DELETE FROM appointments 
        WHERE client_id = ? 
        AND status IN ('completed', 'cancelled')
        ''', (client_id,))
        
        deleted_count = cursor.rowcount
        await cursor.close()
        await db.commit()
    
    message = f"🗑 История очищена! Удалено {deleted_count} записей."
    
    keyboard = [get_back_button("main_menu")]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def services(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Услуги и цены"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("services")
        metrics_collector.log_active_user(user.id)
    except:
        pass

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT name, duration, price FROM services')
        services_list = await cursor.fetchall()
        await cursor.close()
    
    services_text = "💵 *Услуги и цены:*\n\n"
    for i, (name, duration, price) in enumerate(services_list, 1):
        services_text += f"{i}. {name}\n"
        services_text += f"   ⏰ {duration} мин | 💵 {price}₽\n\n"
    
    keyboard = [
        [InlineKeyboardButton("📅 Записаться", callback_data="book_from_services")],
        get_back_button("main_menu")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            services_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            services_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def show_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Контакты салона"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("show_contacts")
        metrics_collector.log_active_user(user.id)
    except:
        pass
    contacts_text = (
        "👨‍💼 *Контакты массажного салона:*\n\n"
        "📍 Адрес: кв.Мирный д.12\n"
        "📞 Телефон: +7 959 500 91 55\n"
        "🕐 Часы работы: 10:00 - 19:00 (запись до 17:30)\n"
        "📧 Email: ilysha2007@yandex.ru\n"
    )
    
    keyboard = [get_back_button("main_menu")]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            contacts_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            contacts_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

# ============================
# ПРОЦЕСС ЗАПИСИ
# ============================

async def choose_master(update: Update, context: ContextTypes.DEFAULT_TYPE, service_id: int):
    """Выбор массажиста"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT name, duration, price FROM services WHERE id = ?', (service_id,))
        service = await cursor.fetchone()
        await cursor.close()
        
        cursor = await db.execute('SELECT id, name FROM masters WHERE active = 1')
        masters = await cursor.fetchall()
        await cursor.close()
    
    if service and masters:
        service_name, duration, price = service
        
        keyboard = []
        for master_id, master_name in masters:
            keyboard.append([InlineKeyboardButton(
                f"👨‍⚕️ {master_name}", 
                callback_data=f"master_{service_id}_{master_id}"
            )])
        
        keyboard.append(get_back_button("book"))
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            f"✅ Вы выбрали: *{service_name}*\n"
            f"⏰ Длительность: {duration} мин\n"
            f"💵 Цена: {price}₽\n\n"
            "👨‍⚕️ *Выберите массажиста:*",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def choose_date(update: Update, context: ContextTypes.DEFAULT_TYPE, service_id: int, master_id: int):
    """Выбор даты записи"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT name FROM services WHERE id = ?', (service_id,))
        service_name = (await cursor.fetchone())[0]
        await cursor.close()
        
        cursor = await db.execute('SELECT name FROM masters WHERE id = ?', (master_id,))
        master_name = (await cursor.fetchone())[0]
        await cursor.close()
    
    dates = get_available_dates()
    
    keyboard = []
    row = []
    for i, (date_str, weekday) in enumerate(dates):
        # Проверяем доступные слоты для конкретного массажиста
        available_slots = await get_available_slots(date_str, master_id)
        if available_slots:
            button_text = f"{date_str} ({weekday})"
            row.append(InlineKeyboardButton(button_text, callback_data=f"date_{service_id}_{master_id}_{date_str}"))
            
            if len(row) == 2 or i == len(dates) - 1:
                keyboard.append(row)
                row = []
    
    if not keyboard:
        await update.callback_query.edit_message_text(
            "❌ На ближайшие две недели нет свободных слотов у этого массажиста.\n"
            "Пожалуйста, выберите другого массажиста или свяжитесь с администратором: +7 959 500 91 55",
            reply_markup=InlineKeyboardMarkup([get_back_button("choose_master")])
        )
        return
    
    keyboard.append(get_back_button("choose_master"))
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        f"📅 *Выберите дату:*\n\n"
        f"Услуга: {service_name}\n"
        f"Массажист: {master_name}\n\n"
        f"*Только даты со свободными слотами:*",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE, service_id: int, master_id: int, selected_date: str):
    """Выбор времени записи"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT name FROM services WHERE id = ?', (service_id,))
        service_name = (await cursor.fetchone())[0]
        await cursor.close()
        
        cursor = await db.execute('SELECT name FROM masters WHERE id = ?', (master_id,))
        master_name = (await cursor.fetchone())[0]
        await cursor.close()
    
    available_times = await get_available_slots(selected_date, master_id)
    
    if not available_times:
        await update.callback_query.edit_message_text(
            "❌ На выбранную дату нет свободных слотов у этого массажиста.\n"
            "Пожалуйста, выберите другую дату или другого массажиста.",
            reply_markup=InlineKeyboardMarkup([get_back_button("choose_date")])
        )
        return
    
    keyboard = []
    row = []
    for i, time_str in enumerate(available_times):
        row.append(InlineKeyboardButton(time_str, callback_data=f"time_{service_id}_{master_id}_{selected_date}_{time_str}"))
        
        if len(row) == 2 or i == len(available_times) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append(get_back_button("choose_date"))
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    date_obj = datetime.strptime(selected_date, '%d.%m.%Y')
    date_display = date_obj.strftime('%d %B %Y')
    
    await update.callback_query.edit_message_text(
        f"⏰ *Выберите время:*\n\n"
        f"Услуга: {service_name}\n"
        f"Массажист: {master_name}\n"
        f"Дата: {date_display}\n\n"
        f"*Доступное время (до 17:30):*",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def confirm_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, service_id: int, master_id: int, selected_date: str, selected_time: str):
    """Подтверждение записи"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT name, duration, price FROM services WHERE id = ?', (service_id,))
        service = await cursor.fetchone()
        await cursor.close()
        
        cursor = await db.execute('SELECT name FROM masters WHERE id = ?', (master_id,))
        master = await cursor.fetchone()
        await cursor.close()
    
    if service and master:
        service_name, duration, price = service
        master_name = master[0]
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_yes"),
                InlineKeyboardButton("❌ Отменить", callback_data="confirm_no")
            ],
            get_back_button("choose_time")
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            f"📋 *Подтверждение записи:*\n\n"
            f"🏷 Услуга: {service_name}\n"
            f"⏰ Длительность: {duration} мин\n"
            f"💵 Цена: {price}₽\n"
            f"👨‍⚕️ Массажист: {master_name}\n"
            f"📅 Дата: {selected_date}\n"
            f"⏰ Время: {selected_time}\n\n"
            f"*Подтвердить запись?*",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def create_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, service_id: int, master_id: int, selected_date: str, selected_time: str):
    """Создание записи"""
    user = update.effective_user
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Получаем или создаем клиента
        cursor = await db.execute('''
        SELECT id FROM clients 
        WHERE telegram_id = ?
        LIMIT 1
        ''', (user.id,))
        
        client = await cursor.fetchone()
        await cursor.close()
        
        if not client:
            # Если клиента нет, создаем его
            await db.execute(
                "INSERT INTO clients (telegram_id, username, name) VALUES (?, ?, ?)",
                (user.id, user.username, user.full_name)
            )
            await db.commit()
            
            cursor = await db.execute('SELECT id FROM clients WHERE telegram_id = ?', (user.id,))
            client = await cursor.fetchone()
            await cursor.close()
        
        client_id = client[0]
        
        if not await is_slot_available(selected_date, selected_time, master_id):
            await update.callback_query.edit_message_text(
                "❌ Этот слот только что заняли. Пожалуйста, выберите другое время.",
                reply_markup=InlineKeyboardMarkup([get_back_button("choose_date")])
            )
            return
        
        cursor = await db.execute('SELECT name, price FROM services WHERE id = ?', (service_id,))
        service = await cursor.fetchone()
        await cursor.close()
        
        cursor = await db.execute('SELECT name FROM masters WHERE id = ?', (master_id,))
        master = await cursor.fetchone()
        await cursor.close()
        
        appointment_datetime = datetime.strptime(f"{selected_date} {selected_time}", '%d.%m.%Y %H:%M')
        appointment_db_str = appointment_datetime.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor = await db.execute('''
        INSERT INTO appointments (client_id, service_id, master_id, appointment_time, status)
        VALUES (?, ?, ?, ?, 'active')
        ''', (client_id, service_id, master_id, appointment_db_str))
        
        appointment_id = cursor.lastrowid
        await cursor.close()
        await db.commit()
    
    if service and master:
        service_name, price = service
        master_name = master[0]
        
        message = (
            f"✅ *Запись успешно создана!*\n\n"
            f"🎫 Номер записи: #{appointment_id}\n"
            f"🏷 Услуга: {service_name}\n"
            f"💵 Цена: {price}₽\n"
            f"👨‍⚕️ Массажист: {master_name}\n"
            f"📅 Дата: {selected_date}\n"
            f"⏰ Время: {selected_time}\n\n"
            f"*Ждем вас!*\n\n"
            f"Напоминание придет за день и за час до записи."
        )
        
        keyboard = [
            [InlineKeyboardButton("📋 Мои активные записи", callback_data="my_appointments")],
            [InlineKeyboardButton("📜 История записей", callback_data="my_all_appointments")],
            get_back_button("main_menu")
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
        
        await send_new_appointment_notification(context.application, appointment_id)
        
        await clear_user_context(context)

# ============================
# АДМИН-ПАНЕЛЬ
# ============================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ-панель"""
    user = update.effective_user

    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("admin_panel")
        metrics_collector.log_active_user(user.id)
    except:
        pass
    
    if not await is_admin(user.id, user.username):
        if update.callback_query:
            await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        else:
            await update.message.reply_text("❌ Доступ запрещен.")
        return
    
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("📈 Системные метрики", callback_data="admin_metrics")],
        [InlineKeyboardButton("📋 Все активные записи", callback_data="admin_active_appointments")],
        [InlineKeyboardButton("📜 Все записи (история)", callback_data="admin_all_appointments")],
        [InlineKeyboardButton("💵 Доходы", callback_data="admin_income")],
        [InlineKeyboardButton("📈 Загруженность массажистов", callback_data="admin_masters_load")],
        [InlineKeyboardButton("📊 Отзывы и рейтинги", callback_data="admin_reviews")],
        [InlineKeyboardButton("📤 Экспорт в Excel", callback_data="admin_export")],
        [InlineKeyboardButton("👥 Клиенты", callback_data="admin_clients")],
        get_back_button("main_menu")
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "👑 *Админ-панель*\n\n"
            "Выберите действие:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "👑 *Админ-панель*\n\n"
            "Выберите действие:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("admin_stats")
        metrics_collector.log_active_user(user.id)
    except:
        pass

    if not await is_admin(user.id, user.username):
        await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM clients")
        total_clients = (await cursor.fetchone())[0]
        await cursor.close()
        
        cursor = await db.execute("SELECT COUNT(*) FROM appointments")
        total_appointments = (await cursor.fetchone())[0]
        await cursor.close()
        
        cursor = await db.execute("SELECT COUNT(*) FROM appointments WHERE status = 'active' AND appointment_time > datetime('now')")
        active_appointments = (await cursor.fetchone())[0]
        await cursor.close()
        
        cursor = await db.execute("SELECT COUNT(*) FROM appointments WHERE DATE(appointment_time) = DATE('now') AND status = 'active'")
        today_appointments = (await cursor.fetchone())[0]
        await cursor.close()
        
        cursor = await db.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Популярные услуги
        cursor = await db.execute('''
        SELECT s.name, COUNT(*) as count
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE a.status = 'completed'
        GROUP BY s.id
        ORDER BY count DESC
        LIMIT 3
        ''')
        
        popular_services = await cursor.fetchall()
        await cursor.close()
        
        # Средний рейтинг
        cursor = await db.execute('SELECT AVG(rating) FROM reviews')
        avg_rating = (await cursor.fetchone())[0] or 0
        await cursor.close()
    
    stats_text = "📊 *Общая статистика:*\n\n"
    stats_text += f"👥 Всего клиентов: {total_clients}\n"
    stats_text += f"📋 Всего записей: {total_appointments}\n"
    stats_text += f"✅ Активных записей: {active_appointments}\n"
    stats_text += f"📅 Записей на сегодня: {today_appointments}\n"
    stats_text += f"⭐ Всего отзывов: {total_reviews}\n"
    stats_text += f"🌟 Средний рейтинг: {avg_rating:.1f}/5\n\n"
    
    stats_text += "🏆 *Популярные услуги:*\n"
    for i, (name, count) in enumerate(popular_services, 1):
        stats_text += f"{i}. {name}: {count} записей\n"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_stats")],
        get_back_button("admin_panel")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        stats_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def show_metrics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показ метрик для администратора"""
    user = update.effective_user
    
    if not await is_admin(user.id, user.username):
        await update.message.reply_text("❌ Доступ запрещен.")
        return
    
    # Обновляем метрики
    await metrics_collector.update_metrics()
    
    # Получаем сводку
    summary = metrics_collector.get_metrics_summary()
    
    metrics_text = (
        "📊 *СИСТЕМНЫЕ МЕТРИКИ*\n\n"
        f"🕐 Аптайм: {summary['uptime_hours']} часов\n"
        f"👥 Всего пользователей: {summary['total_users']}\n"
        f"🔥 Активных сегодня: {summary['active_today']}\n"
        f"🆕 Новых сегодня: {summary['new_today']}\n\n"
        f"📋 Записей сегодня: {summary['appointments_today']}\n"
        f"💰 Доход сегодня: {summary['revenue_today']}₽\n"
        f"✅ Активных записей: {summary['active_appointments']}\n\n"
        f"📨 Отправлено сообщений: {summary['messages_sent']}\n"
        f"⚡ Среднее время ответа: {summary['avg_response_time']}с\n"
        f"❌ Ошибок: {summary['errors']}\n"
        f"⏳ Блокировок rate limit: {metrics_collector.metrics['rate_limit_hits']}"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_metrics")],
        get_back_button("admin_panel")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            metrics_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            metrics_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def show_full_metrics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показ полного отчета метрик для администратора"""
    user = update.effective_user
    
    if not await is_admin(user.id, user.username):
        await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        return
    
    await metrics_collector.update_metrics()
    
    metrics = metrics_collector.metrics
    
    report = (
        "📊 *ПОЛНЫЙ ОТЧЕТ МЕТРИК*\n\n"
        f"🕐 Время генерации: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        f"⏱ Аптайм: {round((datetime.now() - metrics['uptime']).total_seconds() / 3600, 2)} часов\n\n"
        
        f"👥 *ПОЛЬЗОВАТЕЛИ:*\n"
        f"  Всего пользователей: {metrics.get('total_users', 0)}\n"
        f"  Новых сегодня: {metrics.get('new_users_today', 0)}\n"
        f"  Активных сегодня: {metrics.get('active_users_today', 0)}\n\n"
        
        f"📋 *ЗАПИСИ:*\n"
        f"  Всего записей: {metrics.get('total_appointments', 0)}\n"
        f"  Активных записей: {metrics.get('active_appointments', 0)}\n"
        f"  Создано сегодня: {metrics.get('appointments_created_today', 0)}\n"
        f"  Завершено сегодня: {metrics.get('appointments_completed_today', 0)}\n"
        f"  Отменено сегодня: {metrics.get('appointments_cancelled_today', 0)}\n\n"
        
        f"💰 *ФИНАНСЫ:*\n"
        f"  Доход сегодня: {metrics.get('revenue_today', 0)}₽\n"
        f"  Доход за неделю: {metrics.get('revenue_week', 0)}₽\n"
        f"  Доход за месяц: {metrics.get('revenue_month', 0)}₽\n"
        f"  Средний чек: {metrics.get('average_ticket', 0)}₽\n\n"
        
        f"⚙️ *ПРОИЗВОДИТЕЛЬНОСТЬ:*\n"
        f"  Отправлено сообщений: {metrics.get('messages_sent', 0)}\n"
        f"  Неудачных отправок: {metrics.get('failed_messages', 0)}\n"
        f"  Ошибок: {metrics.get('errors', 0)}\n"
        f"  Блокировок rate limit: {metrics.get('rate_limit_hits', 0)}\n"
        f"  Среднее время ответа: {sum(metrics.get('response_times', [0])) / max(len(metrics.get('response_times', [1])), 1):.3f}с\n\n"
    )
    
    if metrics.get('by_service'):
        report += "🏆 *ТОП УСЛУГ:*\n"
        services_sorted = sorted(
            metrics['by_service'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        for service, count in services_sorted:
            report += f"  {service}: {count} записей\n"
        report += "\n"
    
    if metrics.get('by_master'):
        report += "👨‍⚕️ *ТОП МАССАЖИСТОВ:*\n"
        masters_sorted = sorted(
            metrics['by_master'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        for master, count in masters_sorted:
            report += f"  {master}: {count} записей\n"
        report += "\n"
    
    if metrics.get('commands_processed'):
        report += "🎮 *ТОП КОМАНД:*\n"
        commands_sorted = sorted(
            metrics['commands_processed'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        for command, count in commands_sorted:
            report += f"  /{command}: {count} раз\n"
    
    keyboard = [
        [InlineKeyboardButton("📊 Краткий отчет", callback_data="admin_metrics")],
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_full_metrics")],
        get_back_button("admin_panel")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query = update.callback_query
    await query.edit_message_text(
        report,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def admin_active_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Все активные записи (ИСПРАВЛЕННЫЙ ВАРИАНТ)"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("admin_active_appointments")
        metrics_collector.log_active_user(user.id)
    except:
        pass
    
    if not await is_admin(user.id, user.username):
        await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT a.id, c.name, c.telegram_id, s.name, m.name, a.appointment_time
        FROM appointments a
        JOIN clients c ON a.client_id = c.id
        JOIN services s ON a.service_id = s.id
        JOIN masters m ON a.master_id = m.id
        WHERE a.status = 'active'
        AND a.appointment_time > datetime('now')
        ORDER BY a.appointment_time ASC
        ''')
        
        appointments = await cursor.fetchall()
        await cursor.close()
    
    if not appointments:
        await update.callback_query.edit_message_text(
            "📭 Нет активных записей на будущее.",
            reply_markup=InlineKeyboardMarkup([get_back_button("admin_panel")])
        )
        return
    
    appointments_text = "📋 *Все активные записи (на будущее):*\n\n"
    keyboard_rows = []
    
    for app in appointments:
        app_id, client_name, client_id, service, master, time = app
        time_str = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
        
        appointments_text += f"🟢 #{app_id}: {client_name} (ID: {client_id})\n"
        appointments_text += f"   Услуга: {service}\n"
        appointments_text += f"   Массажист: {master}\n"
        appointments_text += f"   Время: {time_str}\n\n"
        
        keyboard_rows.append([
            InlineKeyboardButton(f"✅ Завершить #{app_id}", callback_data=f"admin_complete_{app_id}"),
            InlineKeyboardButton(f"❌ Отменить #{app_id}", callback_data=f"admin_cancel_{app_id}")
        ])
    
    keyboard_rows.append([InlineKeyboardButton("🔄 Обновить", callback_data="admin_active_appointments")])
    keyboard_rows.append(get_back_button("admin_panel"))
    
    reply_markup = InlineKeyboardMarkup(keyboard_rows)
    
    await update.callback_query.edit_message_text(
        appointments_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def admin_all_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Все записи (история) - ИСПРАВЛЕННЫЙ ВАРИАНТ"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("admin_all_appointments")
        metrics_collector.log_active_user(user.id)
    except:
        pass
    
    if not await is_admin(user.id, user.username):
        await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT a.id, c.name, c.telegram_id, s.name, m.name, a.appointment_time, a.status, a.updated_at
        FROM appointments a
        JOIN clients c ON a.client_id = c.id
        JOIN services s ON a.service_id = s.id
        JOIN masters m ON a.master_id = m.id
        ORDER BY a.appointment_time DESC
        LIMIT 50
        ''')
        
        appointments = await cursor.fetchall()
        await cursor.close()
    
    if not appointments:
        await update.callback_query.edit_message_text(
            "📭 Нет записей.",
            reply_markup=InlineKeyboardMarkup([get_back_button("admin_panel")])
        )
        return
    
    appointments_text = "📋 *Последние 50 записей (вся история):*\n\n"
    for app in appointments:
        app_id, client_name, client_id, service, master, time, status, updated_at = app
        time_str = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
        
        # Определяем эмодзи статуса
        if status == 'active':
            # Проверяем, прошла ли уже запись
            appointment_dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            if appointment_dt < now:
                status_emoji = "🕐"
            else:
                status_emoji = "🟢"
        elif status == 'completed':
            status_emoji = "✅"
        elif status == 'cancelled':
            status_emoji = "❌"
        else:
            status_emoji = "⚪"
            
        appointments_text += f"{status_emoji} #{app_id}: {client_name} (ID: {client_id})\n"
        appointments_text += f"   {service} | {master}\n"
        appointments_text += f"   {time_str} | {status}"
        
        if status in ['completed', 'cancelled'] and updated_at:
            updated_str = datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y')
            appointments_text += f" | обновлено: {updated_str}"
            
        appointments_text += "\n\n"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_all_appointments")],
        get_back_button("admin_panel")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        appointments_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def admin_income(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Доходы от завершенных заказов"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("admin_income")
        metrics_collector.log_active_user(user.id)
    except:
        pass
    
    if not await is_admin(user.id, user.username):
        await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # Доход за сегодня (только завершенные)
        cursor = await db.execute('''
        SELECT SUM(s.price) as income, COUNT(*) as count
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE DATE(a.appointment_time) = DATE('now')
        AND a.status = 'completed'
        ''')
        today_result = await cursor.fetchone()
        await cursor.close()
        
        today_income = today_result[0] or 0
        today_count = today_result[1] or 0
        
        # Доход за неделю
        week_ago = datetime.now() - timedelta(days=7)
        cursor = await db.execute('''
        SELECT SUM(s.price) as income, COUNT(*) as count
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE a.appointment_time >= ?
        AND a.status = 'completed'
        ''', (week_ago.strftime('%Y-%m-%d %H:%M:%S'),))
        week_result = await cursor.fetchone()
        await cursor.close()
        
        week_income = week_result[0] or 0
        week_count = week_result[1] or 0
        
        # Доход за месяц
        month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0)
        cursor = await db.execute('''
        SELECT SUM(s.price) as income, COUNT(*) as count
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE a.appointment_time >= ?
        AND a.status = 'completed'
        ''', (month_start.strftime('%Y-%m-%d %H:%M:%S'),))
        month_result = await cursor.fetchone()
        await cursor.close()
        
        month_income = month_result[0] or 0
        month_count = month_result[1] or 0
        
        # Доход за все время
        cursor = await db.execute('''
        SELECT SUM(s.price) as income, COUNT(*) as count
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE a.status = 'completed'
        ''')
        total_result = await cursor.fetchone()
        await cursor.close()
        
        total_income = total_result[0] or 0
        total_count = total_result[1] or 0
        
        # Доход по услугам
        cursor = await db.execute('''
        SELECT s.name, SUM(s.price) as income, COUNT(*) as count
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE a.status = 'completed'
        GROUP BY s.id
        ORDER BY income DESC
        ''')
        
        service_stats = await cursor.fetchall()
        await cursor.close()
    
    income_text = "💵 *Доходы от завершенных заказов:*\n\n"
    income_text += f"📅 *Сегодня:* {today_income}₽ ({today_count} заказов)\n"
    income_text += f"📆 *За неделю:* {week_income}₽ ({week_count} заказов)\n"
    income_text += f"🗓 *За месяц:* {month_income}₽ ({month_count} заказов)\n"
    income_text += f"📊 *Всего:* {total_income}₽ ({total_count} заказов)\n\n"
    
    income_text += "📈 *По услугам:*\n"
    for name, income, count in service_stats:
        income_text += f"• {name}: {income}₽ ({count} заказов)\n"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_income")],
        get_back_button("admin_panel")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        income_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def admin_masters_load(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика загруженности массажистов"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("admin_masters_load")
        metrics_collector.log_active_user(user.id)
    except:
        pass
    
    if not await is_admin(user.id, user.username):
        await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # Статистика по каждому массажисту
        cursor = await db.execute('''
        SELECT 
            m.name,
            COUNT(CASE WHEN a.status = 'completed' THEN 1 END) as completed,
            COUNT(CASE WHEN a.status = 'active' AND a.appointment_time > datetime('now') THEN 1 END) as upcoming,
            COUNT(CASE WHEN a.status = 'active' AND a.appointment_time <= datetime('now') THEN 1 END) as past_active,
            COUNT(CASE WHEN a.status = 'cancelled' THEN 1 END) as cancelled,
            COUNT(*) as total,
            AVG(r.rating) as avg_rating
        FROM masters m
        LEFT JOIN appointments a ON m.id = a.master_id
        LEFT JOIN reviews r ON m.id = r.master_id
        WHERE m.active = 1
        GROUP BY m.id
        ORDER BY m.name
        ''')
        
        masters_stats = await cursor.fetchall()
        await cursor.close()
        
        # Записи на сегодня
        cursor = await db.execute('''
        SELECT m.name, COUNT(*) as today_count
        FROM appointments a
        JOIN masters m ON a.master_id = m.id
        WHERE DATE(a.appointment_time) = DATE('now')
        AND a.status = 'active'
        GROUP BY m.id
        ''')
        
        today_stats = {row[0]: row[1] for row in await cursor.fetchall()}
        await cursor.close()
        
        # Записи на завтра
        cursor = await db.execute('''
        SELECT m.name, COUNT(*) as tomorrow_count
        FROM appointments a
        JOIN masters m ON a.master_id = m.id
        WHERE DATE(a.appointment_time) = DATE('now', '+1 day')
        AND a.status = 'active'
        GROUP BY m.id
        ''')
        
        tomorrow_stats = {row[0]: row[1] for row in await cursor.fetchall()}
        await cursor.close()
    
    if not masters_stats:
        await update.callback_query.edit_message_text(
            "📭 Нет данных о массажистах.",
            reply_markup=InlineKeyboardMarkup([get_back_button("admin_panel")])
        )
        return
    
    load_text = "📈 *Загруженность массажистов:*\n\n"
    
    for master in masters_stats:
        name, completed, upcoming, past_active, cancelled, total, avg_rating = master
        
        load_text += f"👨‍⚕️ *{name}:*\n"
        load_text += f"   ✅ Завершено: {completed}\n"
        load_text += f"   🟢 Активных (будущих): {upcoming}\n"
        load_text += f"   📅 Сегодня: {today_stats.get(name, 0)}\n"
        load_text += f"   📆 Завтра: {tomorrow_stats.get(name, 0)}\n"
        load_text += f"   ❌ Отменено: {cancelled}\n"
        load_text += f"   📊 Всего записей: {total}\n"
        
        if avg_rating:
            load_text += f"   ⭐ Средний рейтинг: {avg_rating:.1f}/5\n"
        else:
            load_text += f"   ⭐ Рейтинг: нет данных\n"
        
        load_text += "\n"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_masters_load")],
        get_back_button("admin_panel")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        load_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def admin_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отзывы и рейтинги"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("admin_reviews")
        metrics_collector.log_active_user(user.id)
    except:
        pass

    async with aiosqlite.connect(DB_PATH) as db:
        # Общая статистика отзывов
        cursor = await db.execute('''
        SELECT 
            COUNT(*) as total,
            AVG(rating) as avg_rating,
            COUNT(CASE WHEN rating = 5 THEN 1 END) as rating_5,
            COUNT(CASE WHEN rating = 4 THEN 1 END) as rating_4,
            COUNT(CASE WHEN rating = 3 THEN 1 END) as rating_3,
            COUNT(CASE WHEN rating = 2 THEN 1 END) as rating_2,
            COUNT(CASE WHEN rating = 1 THEN 1 END) as rating_1
        FROM reviews
        ''')
        
        review_stats = await cursor.fetchone()
        await cursor.close()
        
        total, avg_rating, r5, r4, r3, r2, r1 = review_stats
        
        # Рейтинги по массажистам
        cursor = await db.execute('''
        SELECT m.name, 
               AVG(r.rating) as avg_rating,
               COUNT(r.id) as review_count
        FROM masters m
        LEFT JOIN reviews r ON m.id = r.master_id
        GROUP BY m.id
        ORDER BY avg_rating DESC NULLS LAST
        ''')
        
        master_ratings = await cursor.fetchall()
        await cursor.close()
        
        # Последние отзывы
        cursor = await db.execute('''
        SELECT r.rating, r.comment, c.name as client, m.name as master, r.created_at
        FROM reviews r
        JOIN clients c ON r.client_id = c.id
        JOIN masters m ON r.master_id = m.id
        ORDER BY r.created_at DESC
        LIMIT 10
        ''')
        
        recent_reviews = await cursor.fetchall()
        await cursor.close()
    
    reviews_text = "⭐ *Отзывы и рейтинги:*\n\n"
    
    if total > 0:
        reviews_text += f"📊 *Общая статистика:*\n"
        reviews_text += f"   Всего отзывов: {total}\n"
        reviews_text += f"   Средний рейтинг: {avg_rating or 0:.1f}/5\n\n"
        
        reviews_text += f"📈 *Распределение оценок:*\n"
        reviews_text += f"   ⭐⭐⭐⭐⭐: {r5 or 0}\n"
        reviews_text += f"   ⭐⭐⭐⭐: {r4 or 0}\n"
        reviews_text += f"   ⭐⭐⭐: {r3 or 0}\n"
        reviews_text += f"   ⭐⭐: {r2 or 0}\n"
        reviews_text += f"   ⭐: {r1 or 0}\n\n"
        
        reviews_text += f"👨‍⚕️ *Рейтинги массажистов:*\n"
        for master_name, avg_rating, count in master_ratings:
            if avg_rating:
                stars = "⭐" * int(round(avg_rating))
                reviews_text += f"   {master_name}: {avg_rating:.1f}/5 {stars} ({count} отзывов)\n"
            else:
                reviews_text += f"   {master_name}: нет отзывов\n"
        
        reviews_text += f"\n📝 *Последние отзывы:*\n"
        for rating, comment, client, master, created_at in recent_reviews:
            stars = "⭐" * rating
            date_str = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y')
            reviews_text += f"\n   {client} → {master}: {rating}/5 {stars}\n"
            if comment:
                reviews_text += f"   \"{comment[:100]}{'...' if len(comment) > 100 else ''}\"\n"
            reviews_text += f"   {date_str}"
    else:
        reviews_text += "📭 Пока нет отзывов."
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_reviews")],
        get_back_button("admin_panel")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        reviews_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def admin_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Клиенты"""
    user = update.effective_user
    
    try:
        from utils.monitoring import metrics_collector
        metrics_collector.log_command("admin_clients")
        metrics_collector.log_active_user(user.id)
    except:
        pass
    
    if not await is_admin(user.id, user.username):
        await update.callback_query.edit_message_text("❌ Доступ запрещен.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
        SELECT c.name, c.telegram_id, c.username, 
               COUNT(a.id) as total_appointments,
               SUM(CASE WHEN a.status = 'active' AND a.appointment_time > datetime('now') THEN 1 ELSE 0 END) as active_appointments,
               SUM(CASE WHEN a.status = 'completed' THEN s.price ELSE 0 END) as total_spent
        FROM clients c
        LEFT JOIN appointments a ON c.id = a.client_id
        LEFT JOIN services s ON a.service_id = s.id
        GROUP BY c.id
        ORDER BY total_appointments DESC
        LIMIT 20
        ''')
        
        clients = await cursor.fetchall()
        await cursor.close()
    
    if not clients:
        await update.callback_query.edit_message_text(
            "📭 Нет клиентов.",
            reply_markup=InlineKeyboardMarkup([get_back_button("admin_panel")])
        )
        return
    
    clients_text = "👥 *Клиенты (топ-20):*\n\n"
    for i, (name, telegram_id, username, appointments, active_apps, spent) in enumerate(clients, 1):
        user_mention = f"@{username}" if username else f"ID: {telegram_id}"
        clients_text += f"{i}. {name}\n"
        clients_text += f"   📱 {user_mention}\n"
        clients_text += f"   📊 Всего записей: {appointments or 0}\n"
        clients_text += f"   🟢 Активных: {active_apps or 0}\n"
        clients_text += f"   💵 Потратил: {spent or 0}₽\n\n"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_clients")],
        get_back_button("admin_panel")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        clients_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def admin_complete_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, appointment_id: int):
    """Завершение записи администратором - ИСТОРИЯ СОХРАНЯЕТСЯ!"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "UPDATE appointments SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (appointment_id,)
        )
        await db.commit()
    
    # Запрашиваем отзыв через 10 минут
    await asyncio.sleep(600)  # 10 минут
    await ask_for_review(update, context, appointment_id)
    
    await update.callback_query.edit_message_text(
        f"✅ Запись #{appointment_id} завершена и сохранена в истории.\n"
        f"Клиенту отправлен запрос на отзыв.",
        reply_markup=InlineKeyboardMarkup([get_back_button("admin_panel")])
    )

async def admin_cancel_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, appointment_id: int):
    """Отмена записи администратором - ИСТОРИЯ СОХРАНЯЕТСЯ!"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "UPDATE appointments SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (appointment_id,)
        )
        await db.commit()
    
    # Отправляем уведомление
    await send_cancellation_notification(context.application, appointment_id, "администратор")
    
    await update.callback_query.edit_message_text(
        f"❌ Запись #{appointment_id} отменена администратором и сохранена в истории.",
        reply_markup=InlineKeyboardMarkup([get_back_button("admin_panel")])
    )

# ============================
# ОБРАБОТЧИК КНОПОК
# ============================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    # Обработка кнопки "Назад"
    if data.startswith("back_to_"):
        target = data.replace("back_to_", "")
        if target == "main_menu":
            await start(update, context)
        elif target == "book":
            await book(update, context)
        elif target == "services":
            await services(update, context)
        elif target == "admin_panel":
            await admin_panel(update, context)
        elif target == "my_appointments":
            await my_appointments(update, context)
        elif target == "my_all_appointments":
            await my_all_appointments(update, context)
        elif target == "choose_master":
            if context.user_data.get('selected_service'):
                service_id = context.user_data['selected_service']
                await choose_master(update, context, service_id)
        elif target == "choose_date":
            if context.user_data.get('selected_service') and context.user_data.get('selected_master'):
                service_id = context.user_data['selected_service']
                master_id = context.user_data['selected_master']
                await choose_date(update, context, service_id, master_id)
        elif target == "choose_time":
            if (context.user_data.get('selected_service') and 
                context.user_data.get('selected_master') and 
                context.user_data.get('selected_date')):
                service_id = context.user_data['selected_service']
                master_id = context.user_data['selected_master']
                selected_date = context.user_data['selected_date']
                await choose_time(update, context, service_id, master_id, selected_date)
        return
    
    # Очистка истории
    elif data == "clear_history":
        await clear_history(update, context)
    
    # Отмена записи клиентом
    elif data.startswith("cancel_my_"):
        appointment_id = int(data.replace("cancel_my_", ""))
        await cancel_appointment(update, context, appointment_id)
    
    # Админ-действия
    elif data.startswith("admin_complete_"):
        appointment_id = int(data.replace("admin_complete_", ""))
        await admin_complete_appointment(update, context, appointment_id)
    
    elif data.startswith("admin_cancel_"):
        appointment_id = int(data.replace("admin_cancel_", ""))
        await admin_cancel_appointment(update, context, appointment_id)
    
    # Система отзывов
    elif data.startswith("review_"):
        parts = data.split("_")
        
        if parts[1].isdigit():  # review_{appointment_id}_{rating}
            appointment_id = int(parts[1])
            if parts[2] == "skip":
                rating = -1
            else:
                rating = int(parts[2])
            await save_review(update, context, appointment_id, rating)
        
        elif parts[1] == "comment":  # review_comment_skip
            await save_review_comment(update, context)
    
    # Админ-панель
    elif data == "admin_panel":
        await admin_panel(update, context)
    elif data == "admin_stats":
        await admin_stats(update, context)
    elif data == "admin_metrics":
        await show_metrics(update, context)
    elif data == "admin_active_appointments":
        await admin_active_appointments(update, context)
    elif data == "admin_all_appointments":
        await admin_all_appointments(update, context)
    elif data == "admin_income":
        await admin_income(update, context)
    elif data == "admin_masters_load":
        await admin_masters_load(update, context)
    elif data == "admin_reviews":
        await admin_reviews(update, context)
    elif data == "admin_export":
        await export_to_excel(update, context)
    elif data == "admin_clients":
        await admin_clients(update, context)
    
    # Основные кнопки
    elif data == "book_appointment" or data == "book_from_services":
        await book(update, context)
    
    elif data == "my_appointments":
        await my_appointments(update, context)
    
    elif data == "my_all_appointments":
        await my_all_appointments(update, context)
    
    elif data == "services":
        await services(update, context)
    
    elif data == "contacts":
        await show_contacts(update, context)
    
    elif data.startswith("service_"):
        service_id = int(data.split("_")[1])
        context.user_data['selected_service'] = service_id
        await choose_master(update, context, service_id)
    
    elif data.startswith("master_"):
        parts = data.split("_")
        service_id = int(parts[1])
        master_id = int(parts[2])
        context.user_data['selected_service'] = service_id
        context.user_data['selected_master'] = master_id
        await choose_date(update, context, service_id, master_id)
    
    elif data.startswith("date_"):
        parts = data.split("_")
        service_id = int(parts[1])
        master_id = int(parts[2])
        selected_date = parts[3]
        context.user_data['selected_service'] = service_id
        context.user_data['selected_master'] = master_id
        context.user_data['selected_date'] = selected_date
        await choose_time(update, context, service_id, master_id, selected_date)
    
    elif data.startswith("time_"):
        parts = data.split("_")
        service_id = int(parts[1])
        master_id = int(parts[2])
        selected_date = parts[3]
        selected_time = parts[4]
        context.user_data['selected_service'] = service_id
        context.user_data['selected_master'] = master_id
        context.user_data['selected_date'] = selected_date
        context.user_data['selected_time'] = selected_time
        await confirm_appointment(update, context, service_id, master_id, selected_date, selected_time)
    
    elif data.startswith("confirm_"):
        parts = data.split("_")
        if parts[1] == "yes":
            service_id = context.user_data.get('selected_service')
            master_id = context.user_data.get('selected_master')
            selected_date = context.user_data.get('selected_date')
            selected_time = context.user_data.get('selected_time')
            
            if all([service_id, master_id, selected_date, selected_time]):
                await create_appointment(update, context, service_id, master_id, selected_date, selected_time)
            else:
                await query.edit_message_text(
                    "❌ Произошла ошибка. Пожалуйста, начните запись заново.",
                    reply_markup=InlineKeyboardMarkup([get_back_button("main_menu")])
                )
        else:
            await query.edit_message_text(
                "❌ Запись отменена.",
                reply_markup=InlineKeyboardMarkup([get_back_button("main_menu")])
            )

# ============================
# ОБРАБОТЧИК СООБЩЕНИЙ
# ============================

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    user = update.effective_user
    text = update.message.text
    
    # Обработка комментариев к отзывам
    if 'review_data' in context.user_data:
        # Сохраняем комментарий
        await save_review_comment(update, context, text)
        return
    
    if await is_admin(user.id, user.username):
        if text.isdigit():
            appointment_id = int(text)
            
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute(
                    "SELECT status FROM appointments WHERE id = ?",
                    (appointment_id,)
                )
                appointment = await cursor.fetchone()
                await cursor.close()
            
            if appointment:
                status = appointment[0]
                if status == 'active':
                    keyboard = [
                        [
                            InlineKeyboardButton("✅ Завершить", callback_data=f"admin_complete_{appointment_id}"),
                            InlineKeyboardButton("❌ Отменить", callback_data=f"admin_cancel_{appointment_id}")
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    await update.message.reply_text(
                        f"Запись #{appointment_id} активна. Выберите действие:",
                        reply_markup=reply_markup
                    )
                else:
                    await update.message.reply_text(
                        f"Запись #{appointment_id} имеет статус: {status}"
                    )
            else:
                await update.message.reply_text(
                    f"Запись #{appointment_id} не найдена."
                )
        elif text.startswith('/'):
            pass
        else:
            await update.message.reply_text(
                "Для работы с админ-панелью используйте команды или кнопки."
            )

async def metrics_scheduler(application):
    """Планировщик для периодического обновления метрик"""
    while True:
        try:
            await asyncio.sleep(300)  # Каждые 5 минут
            await metrics_collector.update_metrics()
            metrics_collector.save_metrics_to_file()
        except Exception as e:
            logger.error(f"Ошибка в metrics_scheduler: {e}")

# ============================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================

def main():
    """Основная функция"""
    
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import threading
    
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/health':
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'OK')
            else:
                self.send_response(404)
                self.end_headers()
    
    def run_health_server():
        server = HTTPServer(('0.0.0.0', 10000), HealthHandler)
        server.serve_forever()
    
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    logger.info("✅ HTTP-сервер для health check запущен на порту 10000")
    
    logger.info("=" * 50)
    logger.info("🤖 ЗАПУСК МАССАЖНОГО БОТА")
    logger.info("=" * 50)
    logger.info(f"📍 Адрес салона: кв.Мирный д.12")
    logger.info(f"📞 Телефон: +7 959 500 91 55")
    logger.info(f"🕐 Часы работы: 10:00 - 19:00")
    logger.info(f"📅 Последняя запись: 17:30")
    logger.info(f"👑 Администраторы: {ADMIN_IDS}")  # Исправлено: ADMIN_IDS
    logger.info(f"💾 Бэкапы каждый день в {DB_BACKUP_HOUR}:00")  # Исправлено: DB_BACKUP_HOUR
    logger.info("=" * 50)
    
    asyncio.run(init_db())
    logger.info("✅ База данных инициализирована")
    
    application = Application.builder().token(TOKEN).build()
    
    async def wrapped_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await rate_limit_middleware(update, context, 
                                   lambda u, c: application.process_update(u))
    
    application.add_handler(MessageHandler(
        filters.ALL & ~filters.COMMAND,
        wrapped_handler
    ), group=-1)
    
    # Обычные обработчики (оставляем как есть)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("book", book))
    application.add_handler(CommandHandler("my", my_appointments))
    application.add_handler(CommandHandler("history", my_all_appointments))
    application.add_handler(CommandHandler("services", services))
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("review", lambda update, context: admin_reviews(update, context)))
    application.add_handler(CommandHandler("metrics", show_metrics))
    application.add_handler(CommandHandler("fullmetrics", show_full_metrics))
    
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    loop.create_task(schedule_reminders(application))
    loop.create_task(backup.schedule_backups(application))  # Исправлено: backup вместо db_backup
    
    scheduler_task = loop.create_task(scheduler.start_metrics_scheduler(application))
    
    logger.info("✅ Все системы запущены")
    logger.info("🤖 Бот готов к работе!")
    
    try:
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )
    except KeyboardInterrupt:
        logger.info("⏹ Бот остановлен пользователем")
        
        scheduler_task.cancel()
        
        metrics_collector.save_metrics_to_file()
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при работе бота: {e}")
        logger.exception("Полная трассировка ошибки:")
        
        loop.run_until_complete(send_admin_notification(
            application,
            f"🚨 КРИТИЧЕСКАЯ ОШИБКА БОТА:\n\n{str(e)[:1000]}"
        ))

if __name__ == "__main__":

    main()
