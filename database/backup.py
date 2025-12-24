"""
Модуль для автоматического резервного копирования базы данных
"""

import os
import shutil
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import logging
import asyncio
import aiosqlite

import config

logger = logging.getLogger(__name__)


def create_backup(db_path=config.DB_PATH):
    """
    Создает резервную копию базы данных
    
    Returns:
        str: Путь к созданному бэкапу или None в случае ошибки
    """
    if not os.path.exists(db_path):
        logger.error(f"База данных {db_path} не найдена")
        return None
    
    try:
        # Создаем директорию для бэкапов, если её нет
        os.makedirs(config.BACKUP_DIR, exist_ok=True)
        
        # Создаем имя файла бэкапа с датой и временем
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(config.BACKUP_DIR, f"massage_backup_{timestamp}.db")
        
        # Копируем файл БД
        shutil.copy2(db_path, backup_file)
        
        # Проверяем целостность БД
        try:
            conn = sqlite3.connect(backup_file)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result[0] != "ok":
                logger.error(f"Проверка целостности бэкапа не пройдена: {result[0]}")
                os.remove(backup_file)
                return None
        except Exception as e:
            logger.error(f"Ошибка проверки целостности: {e}")
            os.remove(backup_file)
            return None
        
        logger.info(f"✅ Резервная копия создана: {backup_file}")
        return backup_file
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания резервной копии: {e}")
        return None


def cleanup_old_backups(max_backups=config.MAX_BACKUPS):
    """
    Удаляет старые бэкапы, оставляя только последние max_backups
    """
    try:
        # Создаем директорию, если её нет
        os.makedirs(config.BACKUP_DIR, exist_ok=True)
        
        backup_files = []
        
        # Собираем все файлы бэкапов
        for file in os.listdir(config.BACKUP_DIR):
            if file.endswith(".db") and file.startswith("massage_backup_"):
                file_path = os.path.join(config.BACKUP_DIR, file)
                if os.path.isfile(file_path):
                    backup_files.append((file_path, os.path.getmtime(file_path)))
        
        # Сортируем по дате создания (самые старые первыми)
        backup_files.sort(key=lambda x: x[1])
        
        # Удаляем старые бэкапы
        if len(backup_files) > max_backups:
            files_to_delete = len(backup_files) - max_backups
            for i in range(files_to_delete):
                file_path, _ = backup_files[i]
                try:
                    os.remove(file_path)
                    logger.info(f"🗑 Удален старый бэкап: {os.path.basename(file_path)}")
                except Exception as e:
                    logger.error(f"Ошибка удаления файла {file_path}: {e}")
    
    except Exception as e:
        logger.error(f"❌ Ошибка очистки старых бэкапов: {e}")


async def schedule_backups(application):
    """Планировщик автоматического резервного копирования"""
    logger.info("🔄 Запуск планировщика резервного копирования")
    
    while True:
        try:
            if config.AUTO_BACKUP:
                # Создаем бэкап
                backup_file = create_backup()
                if backup_file:
                    logger.info(f"✅ Создан автоматический бэкап: {backup_file}")
                
                # Очищаем старые бэкапы
                cleanup_old_backups()
            
            # Ждем указанный интервал
            await asyncio.sleep(config.BACKUP_INTERVAL_HOURS * 3600)
            
        except asyncio.CancelledError:
            logger.info("🔄 Планировщик бэкапов остановлен")
            break
        except Exception as e:
            logger.error(f"❌ Ошибка в планировщике бэкапов: {e}")
            await asyncio.sleep(3600)  # Ждем час при ошибке


async def create_manual_backup(application, chat_id: int):
    """Создание ручного бэкапа по запросу администратора"""
    try:
        await application.bot.send_message(
            chat_id=chat_id,
            text="🔄 Начинаю создание резервной копии базы данных..."
        )
        
        backup_file = create_backup()
        
        if backup_file:
            cleanup_old_backups()
            
            # Отправляем подтверждение
            await application.bot.send_message(
                chat_id=chat_id,
                text=f"✅ Резервная копия успешно создана!\n"
                     f"Файл: {os.path.basename(backup_file)}\n"
                     f"Размер: {os.path.getsize(backup_file) / 1024:.1f} КБ"
            )
            return True
        else:
            await application.bot.send_message(
                chat_id=chat_id,
                text="❌ Не удалось создать резервную копию. Проверьте логи."
            )
            return False
            
    except Exception as e:
        logger.error(f"Ошибка создания ручного бэкапа: {e}")
        await application.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Ошибка при создании бэкапа: {str(e)[:100]}"
        )
        return False