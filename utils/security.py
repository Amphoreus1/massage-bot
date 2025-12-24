"""
Модуль безопасности и валидации
"""

import re
import hashlib
import hmac
import os
import binascii
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from contextlib import asynccontextmanager
import logging

import config

logger = logging.getLogger(__name__)


class Security:
    """Класс для обеспечения безопасности"""
    
    # Регулярные выражения для валидации
    PHONE_REGEX = r'^\+7\d{10}$|^8\d{10}$'
    EMAIL_REGEX = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    
    # Константы (добавлены недостающие из config)
    MIN_TIME_BEFORE_APPOINTMENT = 60  # минут до записи (измените по необходимости)
    RATE_LIMIT_PER_USER = 10  # запросов в минуту
    MAX_MESSAGE_LENGTH = 4096  # максимальная длина сообщения Telegram
    
    # Рабочие часа (измените по необходимости)
    WORKING_HOURS = {
        "start": datetime.strptime("10:00", "%H:%M").time(),
        "last_appointment": datetime.strptime("17:30", "%H:%M").time()
    }
    
    # Кэш для rate limiting
    _rate_limit_cache: Dict[int, List[datetime]] = {}
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Валидация номера телефона"""
        if not phone:
            return False
        return bool(re.match(Security.PHONE_REGEX, phone.strip()))
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Валидация email"""
        if not email:
            return False
        return bool(re.match(Security.EMAIL_REGEX, email.strip()))
    
    @staticmethod
    def hash_password(password: str, salt: str = None) -> Tuple[str, str]:
        """Хеширование пароля с использованием salt"""
        if salt is None:
            salt = hashlib.sha256(os.urandom(60)).hexdigest().encode('ascii')
        
        pwdhash = hashlib.pbkdf2_hmac(
            'sha512',
            password.encode('utf-8'),
            salt,
            100000
        )
        pwdhash = binascii.hexlify(pwdhash)
        
        return (salt + pwdhash).decode('ascii'), salt.decode('ascii')
    
    @staticmethod
    def verify_password(stored_password: str, provided_password: str) -> bool:
        """Проверка пароля"""
        salt = stored_password[:64]
        stored_password = stored_password[64:]
        
        pwdhash, _ = Security.hash_password(provided_password, salt.encode('ascii'))
        pwdhash = binascii.hexlify(pwdhash).decode('ascii')
        return pwdhash == stored_password
    
    @staticmethod
    def generate_api_key() -> str:
        """Генерация API ключа"""
        return hashlib.sha256(os.urandom(32)).hexdigest()
    
    @staticmethod
    def validate_appointment_time(selected_datetime: datetime) -> Tuple[bool, str]:
        """
        Проверка времени записи
        
        Returns:
            Tuple[bool, str]: (валидно ли, сообщение об ошибке)
        """
        now = datetime.now()
        
        # Проверка минимального времени до записи
        if selected_datetime - now < timedelta(minutes=Security.MIN_TIME_BEFORE_APPOINTMENT):
            return False, f"Запись возможна минимум за {Security.MIN_TIME_BEFORE_APPOINTMENT} минут до начала"
        
        # Проверка рабочих часов
        if selected_datetime.time() < Security.WORKING_HOURS["start"]:
            return False, f"Салон открывается в {Security.WORKING_HOURS['start'].strftime('%H:%M')}"
        
        if selected_datetime.time() > Security.WORKING_HOURS["last_appointment"]:
            return False, f"Последняя запись возможна до {Security.WORKING_HOURS['last_appointment'].strftime('%H:%M')}"
        
        # Проверка дня недели (если нужно)
        if selected_datetime.weekday() >= 5:  # 5 = суббота, 6 = воскресенье
            return False, "Запись на выходные дни недоступна"
        
        return True, ""
    
    @staticmethod
    def sanitize_input(text: str, max_length: int = None) -> str:
        """Очистка пользовательского ввода"""
        if not text:
            return ""
        
        # Удаляем опасные символы
        text = re.sub(r'[<>"\'&;]', '', text)
        
        # Обрезаем до максимальной длины
        if max_length and len(text) > max_length:
            text = text[:max_length]
        
        return text.strip()
    
    @staticmethod
    def check_rate_limit(user_id: int) -> bool:
        """
        Проверка rate limiting для пользователя
        
        Args:
            user_id: ID пользователя Telegram
            
        Returns:
            bool: True если лимит не превышен, False если превышен
        """
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        
        # Получаем список запросов пользователя
        user_requests = Security._rate_limit_cache.get(user_id, [])
        
        # Удаляем старые запросы (старше 1 минуты)
        user_requests = [req_time for req_time in user_requests if req_time > minute_ago]
        
        # Проверяем лимит
        if len(user_requests) >= Security.RATE_LIMIT_PER_USER:
            logger.warning(f"Rate limit exceeded for user {user_id}")
            return False
        
        # Добавляем новый запрос
        user_requests.append(now)
        Security._rate_limit_cache[user_id] = user_requests
        
        # Очищаем старые записи из кэша (опционально, для экономии памяти)
        Security._cleanup_rate_limit_cache()
        
        return True
    
    @staticmethod
    def _cleanup_rate_limit_cache():
        """Очистка старых записей из кэша rate limiting"""
        now = datetime.now()
        hour_ago = now - timedelta(hours=1)
        
        # Удаляем записи старше часа
        for user_id in list(Security._rate_limit_cache.keys()):
            user_requests = Security._rate_limit_cache[user_id]
            user_requests = [req_time for req_time in user_requests if req_time > hour_ago]
            
            if user_requests:
                Security._rate_limit_cache[user_id] = user_requests
            else:
                del Security._rate_limit_cache[user_id]


class SafeMessageSender:
    """Безопасная отправка сообщений с обработкой ошибок"""
    
    # Константы (добавлены)
    MAX_MESSAGE_LENGTH = 4096  # максимальная длина сообщения Telegram
    
    @staticmethod
    async def send_message(context, chat_id: int, text: str, **kwargs) -> bool:
        """
        Безопасная отправка сообщения
        
        Returns:
            bool: True если отправлено успешно, False если ошибка
        """
        try:
            await context.bot.send_message(chat_id, text, **kwargs)
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения пользователю {chat_id}: {e}")
            
            # Логируем полный текст ошибки для отладки
            logger.exception(f"Полная трассировка ошибки отправки:")
            
            # Попытка отправить укороченное сообщение
            if len(text) > SafeMessageSender.MAX_MESSAGE_LENGTH:
                try:
                    short_text = text[:SafeMessageSender.MAX_MESSAGE_LENGTH - 100] + "..."
                    await context.bot.send_message(chat_id, short_text, **kwargs)
                    return True
                except Exception as e2:
                    logger.error(f"Вторая попытка отправки тоже не удалась: {e2}")
            
            return False
    
    @staticmethod
    async def edit_message(query, text: str, **kwargs) -> bool:
        """
        Безопасное редактирование сообщения
        
        Returns:
            bool: True если успешно, False если ошибка
        """
        try:
            await query.edit_message_text(text, **kwargs)
            return True
        except Exception as e:
            logger.error(f"Ошибка редактирования сообщения: {e}")
            return False


# Создаем экземпляр для использования
security = Security()
safe_sender = SafeMessageSender()