"""
Модуль мониторинга и сбора метрик
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any
import logging
import aiosqlite

import config

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Сборщик метрик для мониторинга"""
    
    def __init__(self):
        self.metrics: Dict[str, Any] = {
            # Основные метрики
            'total_users': 0,
            'active_users_today': 0,
            'new_users_today': 0,
            
            # Статистика записей
            'appointments_created_today': 0,
            'appointments_completed_today': 0,
            'appointments_cancelled_today': 0,
            'total_appointments': 0,
            'active_appointments': 0,
            
            # Финансовые метрики
            'revenue_today': 0,
            'revenue_week': 0,
            'revenue_month': 0,
            'average_ticket': 0,
            
            # Метрики производительности
            'messages_sent': 0,
            'errors': 0,
            'response_times': [],
            'uptime': datetime.now(),
            
            # Детализированные метрики
            'by_service': defaultdict(int),
            'by_master': defaultdict(int),
            'by_hour': defaultdict(int),
            
            # Rate limiting
            'rate_limit_hits': 0,
            'failed_messages': 0,
            
            # Команды (ДОБАВЛЕНО - исправление проблемы 4)
            'commands_processed': defaultdict(int),
            'active_users': set()
        }
        
        self._lock = asyncio.Lock()
        self.db_path = config.DB_PATH
    
    async def update_metrics(self):
        """Обновление всех метрик из базы данных"""
        async with self._lock:
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    await self._update_user_metrics(db)
                    await self._update_appointment_metrics(db)
                    await self._update_financial_metrics(db)
                    await self._update_service_metrics(db)
                    
                    logger.debug("Метрики успешно обновлены")
            except Exception as e:
                logger.error(f"Ошибка обновления метрик: {e}")
                self.metrics['errors'] += 1
    
    async def _update_user_metrics(self, db):
        """Обновление метрик пользователей"""
        # Всего пользователей
        cursor = await db.execute("SELECT COUNT(*) FROM clients")
        self.metrics['total_users'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Новые пользователи сегодня
        cursor = await db.execute(
            "SELECT COUNT(*) FROM clients WHERE DATE(created_at) = DATE('now')"
        )
        self.metrics['new_users_today'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Активные пользователи сегодня (которые делали записи)
        cursor = await db.execute('''
        SELECT COUNT(DISTINCT c.id) 
        FROM clients c
        JOIN appointments a ON c.id = a.client_id
        WHERE DATE(a.created_at) = DATE('now')
        ''')
        self.metrics['active_users_today'] = (await cursor.fetchone())[0]
        await cursor.close()
    
    async def _update_appointment_metrics(self, db):
        """Обновление метрик записей"""
        # Всего записей
        cursor = await db.execute("SELECT COUNT(*) FROM appointments")
        self.metrics['total_appointments'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Активные записи
        cursor = await db.execute('''
        SELECT COUNT(*) FROM appointments 
        WHERE status = 'active' AND appointment_time > datetime('now')
        ''')
        self.metrics['active_appointments'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Сегодняшние записи по статусам
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Созданные сегодня
        cursor = await db.execute('''
        SELECT COUNT(*) FROM appointments 
        WHERE DATE(created_at) = ?
        ''', (today,))
        self.metrics['appointments_created_today'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Завершенные сегодня
        cursor = await db.execute('''
        SELECT COUNT(*) FROM appointments 
        WHERE DATE(updated_at) = ? AND status = 'completed'
        ''', (today,))
        self.metrics['appointments_completed_today'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Отмененные сегодня
        cursor = await db.execute('''
        SELECT COUNT(*) FROM appointments 
        WHERE DATE(updated_at) = ? AND status = 'cancelled'
        ''', (today,))
        self.metrics['appointments_cancelled_today'] = (await cursor.fetchone())[0]
        await cursor.close()
    
    async def _update_financial_metrics(self, db):
        """Обновление финансовых метрик"""
        # Доход за сегодня
        cursor = await db.execute('''
        SELECT SUM(s.price) 
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE DATE(a.appointment_time) = DATE('now')
        AND a.status = 'completed'
        ''')
        today_revenue = (await cursor.fetchone())[0] or 0
        self.metrics['revenue_today'] = today_revenue
        await cursor.close()
        
        # Доход за неделю
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        cursor = await db.execute('''
        SELECT SUM(s.price) 
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE DATE(a.appointment_time) >= ?
        AND a.status = 'completed'
        ''', (week_ago,))
        week_revenue = (await cursor.fetchone())[0] or 0
        self.metrics['revenue_week'] = week_revenue
        await cursor.close()
        
        # Доход за месяц
        month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
        cursor = await db.execute('''
        SELECT SUM(s.price) 
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE DATE(a.appointment_time) >= ?
        AND a.status = 'completed'
        ''', (month_start,))
        month_revenue = (await cursor.fetchone())[0] or 0
        self.metrics['revenue_month'] = month_revenue
        await cursor.close()
        
        # Средний чек
        cursor = await db.execute('''
        SELECT AVG(s.price)
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE a.status = 'completed'
        ''')
        avg_ticket = (await cursor.fetchone())[0] or 0
        self.metrics['average_ticket'] = round(avg_ticket, 2)
        await cursor.close()
    
    async def _update_service_metrics(self, db):
        """Обновление метрик по услугам и мастерам"""
        # По услугам
        cursor = await db.execute('''
        SELECT s.name, COUNT(*) as count
        FROM appointments a
        JOIN services s ON a.service_id = s.id
        WHERE a.status = 'completed'
        GROUP BY s.id
        ''')
        
        for name, count in await cursor.fetchall():
            self.metrics['by_service'][name] = count
        await cursor.close()
        
        # По мастерам
        cursor = await db.execute('''
        SELECT m.name, COUNT(*) as count
        FROM appointments a
        JOIN masters m ON a.master_id = m.id
        WHERE a.status = 'completed'
        GROUP BY m.id
        ''')
        
        for name, count in await cursor.fetchall():
            self.metrics['by_master'][name] = count
        await cursor.close()
        
        # По часам
        cursor = await db.execute('''
        SELECT strftime('%H:00', appointment_time) as hour, COUNT(*)
        FROM appointments
        WHERE status = 'completed'
        GROUP BY strftime('%H:00', appointment_time)
        ORDER BY hour
        ''')
        
        for hour, count in await cursor.fetchall():
            self.metrics['by_hour'][hour] = count
        await cursor.close()
    
    def increment_counter(self, metric_name: str, value: int = 1):
        """Увеличение счетчика метрики"""
        if metric_name in self.metrics:
            self.metrics[metric_name] += value
        else:
            self.metrics[metric_name] = value
    
    def record_response_time(self, response_time: float):
        """Запись времени ответа"""
        self.metrics['response_times'].append(response_time)
        # Храним только последние 1000 значений
        if len(self.metrics['response_times']) > 1000:
            self.metrics['response_times'] = self.metrics['response_times'][-1000:]
    
    def log_command(self, command_name: str):
        """Логирование использования команды"""
        self.metrics['commands_processed'][command_name] += 1
    
    def log_active_user(self, user_id: int):
        """Логирование активного пользователя"""
        self.metrics['active_users'].add(user_id)
        # Также обновляем счетчик активных пользователей сегодня
        if 'active_users_today_set' not in self.metrics:
            self.metrics['active_users_today_set'] = set()
        self.metrics['active_users_today_set'].add(user_id)
        self.metrics['active_users_today'] = len(self.metrics['active_users_today_set'])
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Получение сводки метрик"""
        return {
            'timestamp': datetime.now().isoformat(),
            'uptime_hours': round((datetime.now() - self.metrics['uptime']).total_seconds() / 3600, 2),
            'total_users': self.metrics['total_users'],
            'active_today': self.metrics['active_users_today'],
            'new_today': self.metrics['new_users_today'],
            'appointments_today': self.metrics['appointments_created_today'],
            'revenue_today': self.metrics['revenue_today'],
            'active_appointments': self.metrics['active_appointments'],
            'errors': self.metrics['errors'],
            'messages_sent': self.metrics['messages_sent'],
            'avg_response_time': self._calculate_average_response_time(),
        }
    
    def _calculate_average_response_time(self) -> float:
        """Расчет среднего времени ответа"""
        if not self.metrics['response_times']:
            return 0.0
        return round(sum(self.metrics['response_times']) / len(self.metrics['response_times']), 3)
    
    async def generate_daily_report(self) -> str:
        """
        Генерация ежедневного отчета
        
        Returns:
            str: Текст отчета в формате Markdown
        """
        await self.update_metrics()
        
        # Формируем отчет
        report = (
            "📊 *ЕЖЕДНЕВНЫЙ ОТЧЕТ*\n\n"
            f"📅 Дата: {datetime.now().strftime('%d.%m.%Y')}\n"
            f"👥 Всего пользователей: {self.metrics.get('total_users', 0)}\n"
            f"🆕 Новых сегодня: {self.metrics.get('new_users_today', 0)}\n"
            f"🔥 Активных сегодня: {self.metrics.get('active_users_today', 0)}\n\n"
            f"📋 Записей сегодня: {self.metrics.get('appointments_created_today', 0)}\n"
            f"💰 Доход сегодня: {self.metrics.get('revenue_today', 0)}₽\n"
            f"✅ Активных записей: {self.metrics.get('active_appointments', 0)}\n\n"
            f"📨 Отправлено сообщений: {self.metrics.get('messages_sent', 0)}\n"
            f"⚡ Среднее время ответа: {self._calculate_average_response_time()}с\n"
            f"❌ Ошибок: {self.metrics.get('errors', 0)}\n"
            f"⏳ Блокировок rate limit: {self.metrics.get('rate_limit_hits', 0)}\n\n"
            f"📈 Аптайм: {round((datetime.now() - self.metrics['uptime']).total_seconds() / 3600, 2)} часов\n"
        )
        
        # Добавляем топ команд (используем commands_processed из метрик)
        if self.metrics['commands_processed']:
            top_commands = sorted(
                self.metrics['commands_processed'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
            
            report += "\n🏆 *Топ-5 команд:*\n"
            for command, count in top_commands:
                report += f"• {command}: {count} раз\n"
        
        return report
    
    def save_metrics_to_file(self):
        """Сохранение метрик в JSON файл"""
        try:
            import json
            from config import BACKUP_DIR
            
            # Убедимся, что папка существует
            os.makedirs(config.BACKUP_DIR, exist_ok=True)
            
            filename = os.path.join(
                config.BACKUP_DIR, 
                f'metrics_{datetime.now().strftime("%Y%m%d")}.json'
            )
            
            # Подготавливаем данные для сохранения
            data_to_save = {
                'timestamp': datetime.now().isoformat(),
                'metrics': self.metrics.copy(),
                'summary': self.get_metrics_summary()
            }
            
            # Преобразуем set в list для JSON
            if 'active_users' in data_to_save['metrics']:
                data_to_save['metrics']['active_users'] = list(data_to_save['metrics']['active_users'])
            
            if 'active_users_today_set' in data_to_save['metrics']:
                data_to_save['metrics']['active_users_today_set'] = list(data_to_save['metrics']['active_users_today_set'])
            
            # Преобразуем defaultdict в dict
            for key in ['by_service', 'by_master', 'by_hour', 'commands_processed']:
                if key in data_to_save['metrics'] and isinstance(data_to_save['metrics'][key], defaultdict):
                    data_to_save['metrics'][key] = dict(data_to_save['metrics'][key])
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Метрики сохранены в {filename}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения метрик: {e}")
            return False
    
    def reset_daily_metrics(self):
        """Сброс ежедневных метрик (вызывается в 00:00)"""
        try:
            # Сбрасываем ежедневные счетчики
            daily_metrics_to_reset = [
                'new_users_today', 'active_users_today', 'appointments_created_today',
                'appointments_completed_today', 'appointments_cancelled_today',
                'revenue_today', 'messages_sent', 'errors',
                'rate_limit_hits', 'failed_messages'
            ]
            
            for metric in daily_metrics_to_reset:
                if metric in self.metrics:
                    self.metrics[metric] = 0
            
            # Очищаем активных пользователей за день
            if 'active_users_today_set' in self.metrics:
                self.metrics['active_users_today_set'] = set()
                self.metrics['active_users_today'] = 0
            
            # Очищаем время ответов
            if 'response_times' in self.metrics:
                self.metrics['response_times'] = []
            
            logger.info("✅ Ежедневные метрики сброшены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сброса метрик: {e}")


# Глобальный экземпляр сборщика метрик
metrics_collector = MetricsCollector()