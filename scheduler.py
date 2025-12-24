"""
Планировщик для регулярных задач
"""

import asyncio
import logging
from datetime import datetime, time
from typing import Optional

import config
import aiosqlite
from utils import monitoring

logger = logging.getLogger(__name__)


class MetricsScheduler:
    """Планировщик метрик"""
    
    def __init__(self, application):
        self.application = application
        self.running = False
        self.metrics_collector = monitoring.metrics_collector
    
    async def start(self):
        """Запуск планировщика"""
        self.running = True
        logger.info("📊 Планировщик метрик запущен")
        
        while self.running:
            try:
                now = datetime.now()
                
                # 1. Обновляем метрики каждые 5 минут
                if now.minute % 5 == 0 and now.second < 10:  # Добавили проверку секунд
                    await self.metrics_collector.update_metrics()
                    logger.debug("📊 Метрики обновлены")
                    await asyncio.sleep(10)  # Ждем 10 секунд чтобы не повторять
                
                # 2. Отправляем ежедневный отчет в 20:00
                if now.hour == 20 and now.minute == 0 and now.second < 10:
                    await self.send_daily_report()
                    # Ждем 61 секунду, чтобы не отправлять повторно в ту же минуту
                    await asyncio.sleep(61)
                
                # 3. Сбрасываем ежедневные метрики в 00:00
                if now.hour == 0 and now.minute == 0 and now.second < 10:
                    self.metrics_collector.reset_daily_metrics()
                    logger.info("🔄 Ежедневные метрики сброшены")
                    await asyncio.sleep(10)
                
                # 4. Сохраняем метрики в файл каждые 6 часов
                if now.hour % 6 == 0 and now.minute == 0 and now.second < 10:
                    self.metrics_collector.save_metrics_to_file()
                    await asyncio.sleep(10)
                
                await asyncio.sleep(1)  # Проверяем каждую секунду
                
            except asyncio.CancelledError:
                logger.info("📊 Планировщик метрик остановлен")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в планировщике метрик: {e}")
                await asyncio.sleep(60)
    
    async def send_daily_report(self):
        """Отправка ежедневного отчета администраторам"""
        try:
            report = await self.metrics_collector.generate_daily_report()
            
            if config.SEND_ADMIN_NOTIFICATIONS:
                # Получаем список администраторов из базы
                async with aiosqlite.connect(config.DB_PATH) as db:
                    cursor = await db.execute("SELECT telegram_id FROM admins")
                    admins = await cursor.fetchall()
                    await cursor.close()
                
                for admin in admins:
                    admin_id = admin[0]
                    try:
                        await self.application.bot.send_message(
                            chat_id=admin_id,
                            text=report,
                            parse_mode="Markdown"
                        )
                        logger.info(f"📊 Отчет отправлен администратору {admin_id}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка отправки отчета админу {admin_id}: {e}")
            
            # Сохраняем метрики
            self.metrics_collector.save_metrics_to_file()
            
            logger.info("📊 Ежедневный отчет сформирован и отправлен")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки ежедневного отчета: {e}")
    
    def stop(self):
        """Остановка планировщика"""
        self.running = False
        logger.info("📊 Планировщик метрик остановлен")


# Создаем функцию для запуска планировщика
async def start_metrics_scheduler(application):
    """Запуск планировщика метрик"""
    scheduler = MetricsScheduler(application)
    await scheduler.start()
    return scheduler