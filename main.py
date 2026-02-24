"""
🎯 БОТ ДЛЯ ЗАПИСИ НА ДОНОРСТВО КРОВИ
Версия: 4.0 (ИСПРАВЛЕНА БЕЗОПАСНОСТЬ И АРХИТЕКТУРА)
Автор: AI Assistant + CodeMD Review
Дата: 2024

ОСНОВНЫЕ ИСПРАВЛЕНИЯ:
✅ Токен вынесен в переменные окружения
✅ Включена проверка SSL-сертификатов
✅ Исправлена структура классов (убраны вложенные методы)
✅ Добавлен asyncio.Lock для потокобезопасности
✅ Добавлен единый интерфейс StorageAdapter
✅ Убрано дублирование кода
✅ Константы вынесены в отдельный класс
✅ Исправлена обработка ошибок (всегда строка в data)
"""

import os
import logging
import asyncio
import json
import time
import random
import ssl
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

import aiohttp
import requests
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove, CallbackQuery
)
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# ========== КОНФИГУРАЦИЯ ==========
class Config:
    """Конфигурация бота с валидацией"""
    
    # Токен из переменных окружения
    TOKEN = os.getenv("BOT_TOKEN")
    if not TOKEN:
        raise ValueError("BOT_TOKEN не найден в переменных окружения!")
    
    # Режим работы (LOCAL, GOOGLE, HYBRID)
    MODE = os.getenv("BOT_MODE", "GOOGLE")
    
    # URL Google Apps Script
    GOOGLE_SCRIPT_URL = os.getenv("GOOGLE_SCRIPT_URL", 
        "https://script.google.com/macros/s/AKfycbyZBk0Byb-y1Z50r1r35kUXChNvJKsNO8ZUhoHOd2vVLQA3QK_XS9RyltNGCzXzKFZ-/exec")
    
    # ID администраторов (из переменных окружения, разделенных запятыми)
    ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "5097581039").split(",") if id.strip()]
    
    # Таймаут сессии в секундах
    SESSION_TIMEOUT = int(os.getenv("SESSION_TIMEOUT", "600"))
    
    # Настройки кэша
    CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))  # 5 минут
    MAX_DATES_TO_SHOW = int(os.getenv("MAX_DATES_TO_SHOW", "6"))
    
    # Настройки rate limiting
    RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "15"))
    RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))  # секунд
    
    # Режим отладки
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    
    @classmethod
    def validate(cls):
        """Проверка конфигурации"""
        if cls.MODE not in ["LOCAL", "GOOGLE", "HYBRID"]:
            raise ValueError(f"Неверный режим работы: {cls.MODE}")
        return True

# Валидируем конфигурацию при запуске
Config.validate()

# ========== КОНСТАНТЫ (Callback Data) ==========
class CallbackData(str, Enum):
    """Единое хранилище для callback_data"""
    # Навигация
    MAIN_MENU = "main_menu"
    CANCEL = "cancel"
    BACK_TO_BLOOD = "back_to_blood"
    BACK_TO_DATE = "back_to_date"
    PROGRESS_INFO = "progress_info"
    
    # Основные действия
    MAIN_RECORD = "main_record"
    MAIN_CHECK = "main_check"
    MAIN_MYBOOKINGS = "main_mybookings"
    MAIN_STATS = "main_stats"
    MAIN_HELP = "main_help"
    
    # Админские действия
    ADMIN_CLEAR_CACHE = "admin_clear_cache"
    ADMIN_REFRESH_CACHE = "admin_refresh_cache"
    ADMIN_SHOW_QUOTAS = "admin_show_quotas"
    ADMIN_RESET = "admin_reset"
    
    # Префиксы для динамических данных
    BLOOD_PREFIX = "blood_"
    DATE_PREFIX = "date_"
    TIME_PREFIX = "time_"
    CANCEL_ASK_PREFIX = "cancel_ask_"
    CANCEL_YES_PREFIX = "cancel_yes_"
    CANCEL_NO = "cancel_no"
    
    @classmethod
    def is_blood(cls, data: str) -> bool:
        return data.startswith(cls.BLOOD_PREFIX)
    
    @classmethod
    def is_date(cls, data: str) -> bool:
        return data.startswith(cls.DATE_PREFIX)
    
    @classmethod
    def is_time(cls, data: str) -> bool:
        return data.startswith(cls.TIME_PREFIX)
    
    @classmethod
    def is_cancel_ask(cls, data: str) -> bool:
        return data.startswith(cls.CANCEL_ASK_PREFIX)
    
    @classmethod
    def is_cancel_yes(cls, data: str) -> bool:
        return data.startswith(cls.CANCEL_YES_PREFIX)

# ========== МОДЕЛИ ДАННЫХ ==========
@dataclass
class Booking:
    """Модель записи на донорство"""
    ticket: str
    date: str
    time: str
    blood_group: str
    day: str
    user_id: int
    created_at: Optional[str] = None

@dataclass
class ApiResponse:
    """Стандартизированный ответ API"""
    status: str  # "success" или "error"
    data: Union[Dict, str]  # В случае ошибки - всегда строка
    
    @classmethod
    def success(cls, data: Dict) -> 'ApiResponse':
        return cls(status="success", data=data)
    
    @classmethod
    def error(cls, message: str) -> 'ApiResponse':
        return cls(status="error", data=message)

# ========== КЛИЕНТ GOOGLE SCRIPT (С SSL) ==========
class GoogleScriptClient:
    """Клиент для работы с Google Apps Script (с поддержкой SSL)"""
    
    def __init__(self, script_url: str):
        self.script_url = script_url
        self.timeout = 15
        self.cache: Dict[str, tuple] = {}  # (data, timestamp)
        
        # Создаем сессию с правильной проверкой SSL
        self.session = requests.Session()
        # НЕ отключаем verify! Используем стандартную проверку
        
    def test_connection(self) -> ApiResponse:
        """Проверить соединение с Google Script"""
        try:
            print(f"[GOOGLE] 🔗 Тестирование соединения...")
            response = self.session.post(
                self.script_url,
                json={"action": "test"},
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"[GOOGLE] ✅ Соединение успешно: {data.get('status')}")
                    return ApiResponse.success(data)
                except json.JSONDecodeError:
                    return ApiResponse.error("Неверный формат ответа от Google Script")
            else:
                return ApiResponse.error(f"HTTP ошибка: {response.status_code}")
                
        except requests.exceptions.Timeout:
            return ApiResponse.error("Таймаут подключения к Google Script")
        except requests.exceptions.ConnectionError as e:
            return ApiResponse.error(f"Ошибка соединения: {str(e)}")
        except Exception as e:
            return ApiResponse.error(f"Неизвестная ошибка: {str(e)}")
    
    def call_api(self, action: str, data: Dict = None, user_id: int = None, 
                 force_refresh: bool = False) -> ApiResponse:
        """Вызвать API Google Script с кэшированием"""
        if data is None:
            data = {}
        
        # Проверяем кэш для GET-запросов
        cache_key = None
        if not force_refresh and action in ["get_available_dates", "get_stats", "get_quotas"]:
            cache_key = f"{action}_{user_id}_{json.dumps(data, sort_keys=True)}"
            if cache_key in self.cache:
                cached_data, timestamp = self.cache[cache_key]
                if time.time() - timestamp < Config.CACHE_TTL:
                    print(f"[GOOGLE] 💾 Используем кэш для {action}")
                    return ApiResponse.success(cached_data)
        
        try:
            payload = {"action": action, **data}
            if user_id:
                payload["user_id"] = str(user_id)
            
            print(f"[GOOGLE] 📤 {action}: {data}")
            response = self.session.post(
                self.script_url,
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                return ApiResponse.error(f"HTTP ошибка: {response.status_code}")
            
            try:
                result = response.json()
            except json.JSONDecodeError:
                return ApiResponse.error("Неверный формат ответа от Google Script")
            
            # Сохраняем в кэш для GET-запросов
            if cache_key and result.get("status") == "success":
                self.cache[cache_key] = (result.get("data", {}), time.time())
            
            if result.get("status") == "success":
                return ApiResponse.success(result.get("data", {}))
            else:
                return ApiResponse.error(result.get("data", "Неизвестная ошибка"))
                
        except requests.exceptions.Timeout:
            return ApiResponse.error("Таймаут подключения к Google Script")
        except Exception as e:
            return ApiResponse.error(f"Ошибка вызова API: {str(e)}")
    
    def clear_cache(self):
        """Очистить кэш"""
        self.cache.clear()
        print("[GOOGLE] 🧹 Кэш очищен")

# ========== ЛОКАЛЬНОЕ ХРАНИЛИЩЕ (ИСПРАВЛЕННОЕ) ==========
class LocalStorage:
    """Локальное хранилище данных с поддержкой asyncio.Lock"""
    
    def __init__(self):
        self._lock = asyncio.Lock()  # Блокировка для потокобезопасности
        self.reset_data()
        print("[LOCAL] 💾 Локальное хранилище инициализировано (v4.0)")
    
    def reset_data(self):
        """Сбросить все данные"""
        self.bookings: Dict[int, Dict[str, Booking]] = {}
        self.quotas = self._get_default_quotas()
        self.working_hours = [
            "07:30", "08:00", "08:30", "09:00", "09:30", "10:00",
            "10:30", "11:00", "11:30", "12:00", "12:30", "13:00",
            "13:30", "14:00"
        ]
        
        self._add_test_data()
    
    def _get_default_quotas(self) -> Dict[str, Dict[str, int]]:
        """Получить квоты по умолчанию"""
        base_quotas = {
            "A+": 10, "A-": 5, "B+": 10, "B-": 5,
            "AB+": 5, "AB-": 3, "O+": 10, "O-": 5
        }
        weekend_quotas = {
            "A+": 8, "A-": 4, "B+": 8, "B-": 4,
            "AB+": 3, "AB-": 2, "O+": 8, "O-": 4
        }
        
        days = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        quotas = {}
        for day in days:
            if day in ["суббота", "воскресенье"]:
                quotas[day] = weekend_quotas.copy()
            else:
                quotas[day] = base_quotas.copy()
        
        return quotas
    
    def _add_test_data(self):
        """Добавить тестовые данные"""
        today = datetime.now()
        test_data = [
            (111111, today + timedelta(days=1), "09:00", "A+"),
            (222222, today + timedelta(days=2), "10:30", "B-"),
            (333333, today + timedelta(days=5), "11:00", "O+"),
        ]
        
        for user_id, date, time_slot, blood_group in test_data:
            date_str = date.strftime("%Y-%m-%d")
            day = self._get_day_of_week_ru(date)
            self._add_booking_sync(user_id, date_str, time_slot, blood_group, day)
        
        print(f"[LOCAL] 📊 Добавлено {len(test_data)} тестовых записей")
    
    def _add_booking_sync(self, user_id: int, date: str, time_slot: str, 
                         blood_group: str, day: str) -> Booking:
        """Синхронное добавление записи (внутреннее использование)"""
        ticket = f"Т-{day[:3]}-{blood_group}-{random.randint(1000, 9999)}"
        booking = Booking(
            ticket=ticket,
            date=date,
            time=time_slot,
            blood_group=blood_group,
            day=day,
            user_id=user_id,
            created_at=datetime.now().isoformat()
        )
        
        if user_id not in self.bookings:
            self.bookings[user_id] = {}
        self.bookings[user_id][date] = booking
        
        return booking
    
    async def _add_booking(self, user_id: int, date: str, time_slot: str, 
                          blood_group: str, day: str) -> Booking:
        """Асинхронное добавление записи с блокировкой"""
        async with self._lock:
            return self._add_booking_sync(user_id, date, time_slot, blood_group, day)
    
    def _get_day_of_week_ru(self, date_obj: datetime) -> str:
        """Получить день недели на русском"""
        days = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        return days[date_obj.weekday()]
    
    def get_available_dates(self, user_id: int) -> ApiResponse:
        """Получить доступные даты"""
        today = datetime.now()
        available_dates = []
        
        for i in range(1, 31):
            if len(available_dates) >= Config.MAX_DATES_TO_SHOW:
                break
                
            check_date = today + timedelta(days=i)
            day_of_week = self._get_day_of_week_ru(check_date)
            
            if day_of_week in self.quotas:
                day_quotas = self.quotas[day_of_week]
                has_quota = any(quota > 0 for quota in day_quotas.values())
                
                if has_quota:
                    date_info = {
                        "date": check_date.strftime("%Y-%m-%d"),
                        "day_of_week": day_of_week,
                        "display_date": check_date.strftime("%d.%m.%Y"),
                        "day_of_week_short": day_of_week[:3],
                        "timestamp": int(check_date.timestamp())
                    }
                    available_dates.append(date_info)
        
        return ApiResponse.success({
            "available_dates": available_dates,
            "message": f"Найдено {len(available_dates)} доступных дат",
            "count": len(available_dates)
        })
    
    def get_free_times(self, date: str, blood_group: str) -> ApiResponse:
        """Получить свободное время на дату"""
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            day_of_week = self._get_day_of_week_ru(date_obj)
            
            # Получаем все занятые времена
            busy_times = []
            for user_data in self.bookings.values():
                if date in user_data:
                    booking = user_data[date]
                    if booking.blood_group == blood_group:
                        busy_times.append(booking.time)
            
            # Фильтруем свободные времена
            free_times = [t for t in self.working_hours if t not in busy_times]
            
            # Считаем квоты
            total_quota = self.quotas[day_of_week].get(blood_group, 0)
            used_quota = len(busy_times)
            remaining_quota = max(0, total_quota - used_quota)
            
            return ApiResponse.success({
                "times": free_times,
                "quota": remaining_quota,
                "quota_total": total_quota,
                "quota_used": used_quota,
                "message": f"Найдено {len(free_times)} свободных слотов"
            })
        except Exception as e:
            return ApiResponse.error(f"Ошибка получения времени: {str(e)}")
    
    async def check_existing(self, date: str, user_id: int) -> ApiResponse:
        """Проверить существующую запись"""
        async with self._lock:
            if user_id in self.bookings and date in self.bookings[user_id]:
                booking = self.bookings[user_id][date]
                return ApiResponse.success({
                    "exists": True,
                    "ticket": booking.ticket,
                    "time": booking.time,
                    "blood_group": booking.blood_group,
                    "day": booking.day,
                    "date": date
                })
            else:
                return ApiResponse.success({"exists": False})
    
    async def register(self, date: str, blood_group: str, time_slot: str, user_id: int) -> ApiResponse:
        """Зарегистрировать новую запись (с блокировкой)"""
        async with self._lock:
            try:
                # Проверяем формат даты
                try:
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    day_of_week = self._get_day_of_week_ru(date_obj)
                except ValueError:
                    return ApiResponse.error(f"Неверный формат даты: {date}")
                
                # Проверяем существующую запись
                existing_check = await self.check_existing(date, user_id)
                if existing_check.data["exists"]:
                    return ApiResponse.error(f"У вас уже есть запись на {date}")
                
                # Проверяем, свободно ли время
                for user_data in self.bookings.values():
                    if date in user_data:
                        booking = user_data[date]
                        if booking.time == time_slot and booking.blood_group == blood_group:
                            return ApiResponse.error(f"Время {time_slot} уже занято")
                
                # Проверяем квоты
                if day_of_week not in self.quotas:
                    return ApiResponse.error(f"Нет квот для {day_of_week}")
                
                total_quota = self.quotas[day_of_week].get(blood_group, 0)
                
                # Считаем использованные места
                used_quota = 0
                for user_data in self.bookings.values():
                    if date in user_data and user_data[date].blood_group == blood_group:
                        used_quota += 1
                
                if used_quota >= total_quota:
                    return ApiResponse.error(f"На {date} все квоты заняты")
                
                # Создаем запись
                booking = self._add_booking_sync(user_id, date, time_slot, blood_group, day_of_week)
                
                return ApiResponse.success({
                    "ticket": booking.ticket,
                    "day": booking.day,
                    "date": booking.date,
                    "time": booking.time,
                    "blood_group": booking.blood_group,
                    "quota_remaining": max(0, total_quota - used_quota - 1),
                    "quota_total": total_quota,
                    "quota_used": used_quota + 1,
                    "registration_date": booking.created_at
                })
                
            except Exception as e:
                return ApiResponse.error(f"Ошибка регистрации: {str(e)}")
    
    async def cancel_booking(self, date: str, ticket: str, user_id: int) -> ApiResponse:
        """Отменить запись (с блокировкой)"""
        async with self._lock:
            try:
                if user_id in self.bookings and date in self.bookings[user_id]:
                    booking = self.bookings[user_id][date]
                    
                    if booking.ticket == ticket:
                        del self.bookings[user_id][date]
                        
                        if not self.bookings[user_id]:
                            del self.bookings[user_id]
                        
                        return ApiResponse.success({
                            "message": "Запись успешно отменена",
                            "ticket": ticket,
                            "day": booking.day,
                            "date": date,
                            "time": booking.time,
                            "blood_group": booking.blood_group
                        })
                
                return ApiResponse.error("Запись не найдена")
            except Exception as e:
                return ApiResponse.error(f"Ошибка отмены: {str(e)}")
    
    def get_user_bookings(self, user_id: int) -> ApiResponse:
        """Получить все записи пользователя"""
        if user_id in self.bookings:
            bookings_list = []
            for date, booking in self.bookings[user_id].items():
                bookings_list.append({
                    "date": date,
                    "day": booking.day,
                    "ticket": booking.ticket,
                    "time": booking.time,
                    "blood_group": booking.blood_group
                })
            
            return ApiResponse.success({
                "bookings": bookings_list,
                "count": len(bookings_list)
            })
        else:
            return ApiResponse.success({
                "bookings": [],
                "count": 0
            })
    
    def get_quotas(self) -> ApiResponse:
        """Получить информацию о квотах"""
        total_quota = 0
        total_used = 0
        by_day = {}
        
        for day, quotas in self.quotas.items():
            day_total = sum(quotas.values())
            day_used = 0
            
            # Считаем использованные места
            for user_data in self.bookings.values():
                for date, booking in user_data.items():
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    booking_day = self._get_day_of_week_ru(date_obj)
                    if booking_day == day:
                        day_used += 1
            
            total_quota += day_total
            total_used += day_used
            
            by_day[day] = {
                "total": day_total,
                "used": day_used,
                "remaining": day_total - day_used,
                "quotas": quotas
            }
        
        return ApiResponse.success({
            "quotas": {
                "totalQuota": total_quota,
                "totalUsed": total_used,
                "remaining": total_quota - total_used,
                "byDay": by_day
            },
            "message": f"Всего квот: {total_quota}, использовано: {total_used}, осталось: {total_quota - total_used}"
        })
    
    def get_stats(self) -> ApiResponse:
        """Получить статистику"""
        total_bookings = sum(len(user_bookings) for user_bookings in self.bookings.values())
        total_users = len(self.bookings)
        
        day_stats = {}
        blood_group_stats = {}
        
        for user_data in self.bookings.values():
            for date, booking in user_data.items():
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                day = self._get_day_of_week_ru(date_obj)
                day_stats[day] = day_stats.get(day, 0) + 1
                
                blood_group = booking.blood_group
                blood_group_stats[blood_group] = blood_group_stats.get(blood_group, 0) + 1
        
        most_popular_day = max(day_stats.items(), key=lambda x: x[1])[0] if day_stats else "нет данных"
        most_popular_blood = max(blood_group_stats.items(), key=lambda x: x[1])[0] if blood_group_stats else "нет данных"
        
        quota_response = self.get_quotas()
        quota_stats = quota_response.data.get("quotas", {})
        
        return ApiResponse.success({
            "total_bookings": total_bookings,
            "total_users": total_users,
            "day_stats": day_stats,
            "blood_group_stats": blood_group_stats,
            "most_popular_day": most_popular_day,
            "most_popular_blood_group": most_popular_blood,
            "quota_stats": quota_stats
        })

# ========== АДАПТЕР ХРАНИЛИЩА (ЕДИНЫЙ ИНТЕРФЕЙС) ==========
class StorageAdapter:
    """Единый интерфейс для работы с разными хранилищами"""
    
    def __init__(self, mode: str, google_client: GoogleScriptClient, local_storage: LocalStorage):
        self.mode = mode
        self.google = google_client
        self.local = local_storage
    
    async def _call_with_fallback(self, method_name: str, *args, **kwargs) -> ApiResponse:
        """Вызвать метод с возможным падением на локальное хранилище"""
        force_refresh = kwargs.pop('force_refresh', False)
        
        # Для LOCAL режима
        if self.mode == "LOCAL":
            method = getattr(self.local, method_name)
            if asyncio.iscoroutinefunction(method):
                return await method(*args, **kwargs)
            return method(*args, **kwargs)
        
        # Для GOOGLE и HYBRID режимов
        elif self.mode in ["GOOGLE", "HYBRID"]:
            # Маппинг методов на действия API
            api_action_map = {
                "get_available_dates": ("get_available_dates", {}),
                "get_free_times": ("get_free_times", {"date": args[0], "blood_group": args[1]}),
                "check_existing": ("check_existing", {"date": args[0]}),
                "register": ("register", {"date": args[0], "blood_group": args[1], "time": args[2]}),
                "cancel_booking": ("cancel_booking", {"date": args[0], "ticket": args[1]}),
                "get_user_bookings": ("get_user_bookings", {}),
                "get_quotas": ("get_quotas", {}),
                "get_stats": ("get_stats", {}),
            }
            
            if method_name not in api_action_map:
                return ApiResponse.error(f"Неизвестный метод: {method_name}")
            
            action, params = api_action_map[method_name]
            
            # Определяем user_id (последний аргумент или kwargs)
            user_id = kwargs.get('user_id')
            if not user_id and args and isinstance(args[-1], int):
                user_id = args[-1]
            
            # Вызываем Google API
            result = self.google.call_api(action, params, user_id, force_refresh)
            
            # Если успех или режим GOOGLE без фолбэка
            if result.status == "success" or self.mode == "GOOGLE":
                return result
            
            # Для HYBRID режима при ошибке - пробуем локально
            if self.mode == "HYBRID" and result.status == "error":
                print(f"[HYBRID] 🔄 Google Script недоступен, переключаемся на локальное хранилище")
                method = getattr(self.local, method_name)
                if asyncio.iscoroutinefunction(method):
                    return await method(*args, **kwargs)
                return method(*args, **kwargs)
            
            return result
        
        else:
            return ApiResponse.error(f"Неизвестный режим работы: {self.mode}")
    
    async def get_available_dates(self, user_id: int, force_refresh: bool = False) -> ApiResponse:
        return await self._call_with_fallback("get_available_dates", user_id, force_refresh=force_refresh)
    
    async def get_free_times(self, date: str, blood_group: str) -> ApiResponse:
        return await self._call_with_fallback("get_free_times", date, blood_group)
    
    async def check_existing(self, date: str, user_id: int) -> ApiResponse:
        return await self._call_with_fallback("check_existing", date, user_id)
    
    async def register(self, date: str, blood_group: str, time_slot: str, user_id: int) -> ApiResponse:
        return await self._call_with_fallback("register", date, blood_group, time_slot, user_id)
    
    async def cancel_booking(self, date: str, ticket: str, user_id: int) -> ApiResponse:
        return await self._call_with_fallback("cancel_booking", date, ticket, user_id)
    
    async def get_user_bookings(self, user_id: int) -> ApiResponse:
        return await self._call_with_fallback("get_user_bookings", user_id)
    
    async def get_quotas(self) -> ApiResponse:
        return await self._call_with_fallback("get_quotas")
    
    async def get_stats(self) -> ApiResponse:
        return await self._call_with_fallback("get_stats")
    
    def clear_cache(self):
        """Очистить кэш Google Script"""
        if self.mode in ["GOOGLE", "HYBRID"]:
            self.google.clear_cache()

# Инициализируем компоненты
google_client = GoogleScriptClient(Config.GOOGLE_SCRIPT_URL)
local_storage = LocalStorage()
storage = StorageAdapter(Config.MODE, google_client, local_storage)

# ========== СЕРВИС ДЛЯ ТАЙМАУТА СЕССИЙ ==========
class SessionTimeout:
    """Управление таймаутом сессий"""
    
    def __init__(self, timeout_seconds: int = Config.SESSION_TIMEOUT):
        self.timeout_seconds = timeout_seconds
        self.user_last_activity: Dict[int, float] = {}
    
    def update_activity(self, user_id: int):
        """Обновить время последней активности пользователя"""
        self.user_last_activity[user_id] = time.time()
    
    def is_session_expired(self, user_id: int) -> bool:
        """Проверить, истекла ли сессия пользователя"""
        if user_id not in self.user_last_activity:
            return False
        
        last_activity = self.user_last_activity[user_id]
        time_since_last_activity = time.time() - last_activity
        
        return time_since_last_activity > self.timeout_seconds
    
    def clear_session(self, user_id: int):
        """Очистить данные сессии пользователя"""
        if user_id in self.user_last_activity:
            del self.user_last_activity[user_id]

# Инициализируем сервис таймаута
session_timeout = SessionTimeout()

# ========== MIDDLEWARE ДЛЯ ПРОВЕРКИ ТАЙМАУТА ==========
async def timeout_middleware(handler, event, data):
    """Middleware для проверки таймаута сессии"""
    try:
        user_id = None
        chat_id = None
        
        if hasattr(event, 'from_user') and event.from_user:
            user_id = event.from_user.id
            chat_id = event.chat.id if hasattr(event, 'chat') and event.chat else None
        elif hasattr(event, 'message') and event.message and event.message.from_user:
            user_id = event.message.from_user.id
            chat_id = event.message.chat.id
        elif hasattr(event, 'callback_query') and event.callback_query and event.callback_query.from_user:
            user_id = event.callback_query.from_user.id
            if hasattr(event.callback_query, 'message') and event.callback_query.message:
                chat_id = event.callback_query.message.chat.id
        
        if user_id:
            if session_timeout.is_session_expired(user_id):
                print(f"[TIMEOUT] ⏰ Сессия пользователя {user_id} истекла")
                
                state = data.get('state')
                if state:
                    await state.clear()
                
                session_timeout.clear_session(user_id)
                
                bot = data.get('bot')
                
                # Проверяем, не нажал ли пользователь на главное меню
                is_main_menu_callback = False
                if hasattr(event, 'callback_query') and event.callback_query:
                    if hasattr(event.callback_query, 'data'):
                        is_main_menu_callback = event.callback_query.data == CallbackData.MAIN_MENU
                
                if is_main_menu_callback:
                    print(f"[TIMEOUT] 🔄 Игнорируем таймаут для кнопки главного меню")
                    session_timeout.update_activity(user_id)
                    return await handler(event, data)
                
                if bot and chat_id:
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text="⏳ Ваша сессия истекла из-за неактивности.\n\n"
                                 "Для продолжения работы используйте команду /start",
                            reply_markup=get_main_menu_keyboard()
                        )
                    except Exception as e:
                        print(f"[TIMEOUT] ❌ Ошибка отправки сообщения: {e}")
                
                if hasattr(event, 'callback_query'):
                    try:
                        await event.callback_query.answer(
                            "Сессия истекла. Используйте /start",
                            show_alert=True
                        )
                    except Exception as e:
                        print(f"[TIMEOUT] ❌ Ошибка ответа на callback: {e}")
                
                return False
            
            session_timeout.update_activity(user_id)
    
    except Exception as e:
        print(f"[TIMEOUT] ❌ Ошибка в middleware: {e}")
    
    return await handler(event, data)

# ========== ОГРАНИЧЕНИЕ ЧАСТОТЫ ЗАПРОСОВ ==========
class RateLimiter:
    """Ограничитель частоты запросов"""
    
    def __init__(self, max_requests: int = Config.RATE_LIMIT_REQUESTS, 
                 time_window: int = Config.RATE_LIMIT_WINDOW):
        self.max_requests = max_requests
        self.time_window = time_window
        self.user_requests: Dict[int, List[float]] = defaultdict(list)
    
    def is_allowed(self, user_id: int) -> bool:
        """Проверить, можно ли выполнить запрос"""
        now = time.time()
        
        requests = self.user_requests[user_id]
        requests = [req_time for req_time in requests if now - req_time < self.time_window]
        self.user_requests[user_id] = requests
        
        if len(requests) >= self.max_requests:
            return False
        
        requests.append(now)
        return True
    
    def get_wait_time(self, user_id: int) -> float:
        """Получить время ожидания"""
        now = time.time()
        requests = self.user_requests[user_id]
        
        if not requests:
            return 0
        
        oldest_request = min(requests)
        if now - oldest_request >= self.time_window:
            return 0
        
        return self.time_window - (now - oldest_request)

rate_limiter = RateLimiter()

# ========== СОСТОЯНИЯ БОТА ==========
class Form(StatesGroup):
    waiting_for_blood_group = State()
    waiting_for_date = State()
    waiting_for_time = State()

# ========== ИНЛАЙН-КЛАВИАТУРЫ ==========
def get_blood_group_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора группы крови"""
    builder = InlineKeyboardBuilder()
    
    blood_groups = [
        ("🅰️ A+", f"{CallbackData.BLOOD_PREFIX}A+"),
        ("🅰️ A-", f"{CallbackData.BLOOD_PREFIX}A-"),
        ("🅱️ B+", f"{CallbackData.BLOOD_PREFIX}B+"),
        ("🅱️ B-", f"{CallbackData.BLOOD_PREFIX}B-"),
        ("🆎 AB+", f"{CallbackData.BLOOD_PREFIX}AB+"),
        ("🆎 AB-", f"{CallbackData.BLOOD_PREFIX}AB-"),
        ("🅾️ O+", f"{CallbackData.BLOOD_PREFIX}O+"),
        ("🅾️ O-", f"{CallbackData.BLOOD_PREFIX}O-")
    ]
    
    for i in range(0, len(blood_groups), 2):
        row = blood_groups[i:i+2]
        buttons = [InlineKeyboardButton(text=text, callback_data=callback) for text, callback in row]
        builder.row(*buttons)
    
    builder.row(
        InlineKeyboardButton(text="🔙 Главное меню", callback_data=CallbackData.MAIN_MENU),
        InlineKeyboardButton(text="❌ Отмена", callback_data=CallbackData.CANCEL)
    )
    
    return builder.as_markup()

def get_dates_keyboard(available_dates: List[dict]) -> InlineKeyboardMarkup:
    """Клавиатура для выбора даты"""
    builder = InlineKeyboardBuilder()
    
    if not available_dates:
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.BACK_TO_BLOOD),
            InlineKeyboardButton(text="❌ Отмена", callback_data=CallbackData.CANCEL)
        )
        return builder.as_markup()
    
    for date_info in available_dates:
        button_text = f"{date_info['day_of_week']}\n{date_info['display_date']}"
        builder.row(
            InlineKeyboardButton(
                text=button_text,
                callback_data=f"{CallbackData.DATE_PREFIX}{date_info['date']}"
            )
        )
    
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.BACK_TO_BLOOD),
        InlineKeyboardButton(text="❌ Отмена", callback_data=CallbackData.CANCEL)
    )
    
    return builder.as_markup()

def get_times_keyboard(times_list: List[str], current_step: int = 1, total_steps: int = 3) -> InlineKeyboardMarkup:
    """Клавиатура для выбора времени"""
    builder = InlineKeyboardBuilder()
    
    if not times_list:
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.BACK_TO_DATE),
            InlineKeyboardButton(text="❌ Отмена", callback_data=CallbackData.CANCEL)
        )
        return builder.as_markup()
    
    time_buttons = []
    for time_str in times_list:
        time_buttons.append(
            InlineKeyboardButton(text=f"⏰ {time_str}", callback_data=f"{CallbackData.TIME_PREFIX}{time_str}")
        )
    
    for i in range(0, len(time_buttons), 3):
        builder.row(*time_buttons[i:i+3])
    
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.BACK_TO_DATE),
        InlineKeyboardButton(text="❌ Отмена", callback_data=CallbackData.CANCEL)
    )
    
    progress = get_progress_bar(current_step, total_steps)
    builder.row(InlineKeyboardButton(text=progress, callback_data=CallbackData.PROGRESS_INFO))
    
    return builder.as_markup()

def get_progress_bar(current: int, total: int, length: int = 8) -> str:
    """Создает текстовый прогресс-бар"""
    percentage = (current - 1) / (total - 1) if total > 1 else 0
    filled = int(percentage * length)
    empty = length - filled
    
    progress_bar = "🟢" * filled + "⚪" * empty
    return f"{progress_bar} {current}/{total}"

def get_confirm_cancellation_keyboard(date: str, ticket: str) -> InlineKeyboardMarkup:
    """Клавиатура для подтверждения отмены записи"""
    builder = InlineKeyboardBuilder()
    
    builder.row(
        InlineKeyboardButton(text="✅ Да, отменить", callback_data=f"{CallbackData.CANCEL_YES_PREFIX}{date}_{ticket}"),
        InlineKeyboardButton(text="❌ Нет, оставить", callback_data=CallbackData.CANCEL_NO)
    )
    
    return builder.as_markup()

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Главное меню бота"""
    builder = InlineKeyboardBuilder()
    
    builder.row(
        InlineKeyboardButton(text="📋 Записаться", callback_data=CallbackData.MAIN_RECORD),
        InlineKeyboardButton(text="🔍 Проверить время", callback_data=CallbackData.MAIN_CHECK)
    )
    builder.row(
        InlineKeyboardButton(text="📖 Мои записи", callback_data=CallbackData.MAIN_MYBOOKINGS),
        InlineKeyboardButton(text="📊 Статистика", callback_data=CallbackData.MAIN_STATS),
        InlineKeyboardButton(text="ℹ️ Помощь", callback_data=CallbackData.MAIN_HELP)
    )
    
    return builder.as_markup()

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура администратора"""
    builder = InlineKeyboardBuilder()
    
    builder.row(
        InlineKeyboardButton(text="🗑️ Очистить кэш квот", callback_data=CallbackData.ADMIN_CLEAR_CACHE),
        InlineKeyboardButton(text="🔄 Обновить кэш", callback_data=CallbackData.ADMIN_REFRESH_CACHE)
    )
    builder.row(
        InlineKeyboardButton(text="📊 Проверить квоты", callback_data=CallbackData.ADMIN_SHOW_QUOTAS),
        InlineKeyboardButton(text="🔄 Сбросить данные", callback_data=CallbackData.ADMIN_RESET)
    )
    builder.row(
        InlineKeyboardButton(text="🔙 В главное меню", callback_data=CallbackData.MAIN_MENU)
    )
    
    return builder.as_markup()

# ========== ОСНОВНЫЕ КОМАНДЫ ==========
async def start_command(message: types.Message, state: FSMContext):
    """Команда /start - показывает главное меню"""
    user = message.from_user
    
    if not rate_limiter.is_allowed(user.id):
        wait_time = int(rate_limiter.get_wait_time(user.id))
        await message.answer(
            f"⏳ Слишком много запросов. Пожалуйста, подождите {wait_time} секунд."
        )
        return
    
    await state.clear()
    session_timeout.clear_session(user.id)
    session_timeout.update_activity(user.id)
    
    if Config.MODE in ["GOOGLE", "HYBRID"]:
        print(f"[CACHE] 🔄 Очистка кэша при старте")
        storage.clear_cache()
    
    greeting_name = user.first_name if user.first_name else "пользователь"
    
    mode_info = {
        "LOCAL": "🔧 Автономный режим",
        "GOOGLE": "🌐 Режим Google Script",
        "HYBRID": "⚡ Гибридный режим"
    }.get(Config.MODE, "❓ Неизвестный режим")
    
    is_admin = user.id in Config.ADMIN_IDS
    admin_text = "\n👑 *Вы администратор* - доступны дополнительные функции" if is_admin else ""
    
    await message.answer(
        f"🎯 *Донорская станция v4.0*\n"
        f"{mode_info}\n\n"
        f"👋 Привет, {greeting_name}!{admin_text}\n\n"
        f"Я помогу вам записаться на донорство крови, "
        f"проверить доступное время или отменить запись.\n\n"
        f"*Новые возможности:*\n"
        f"• 📅 Выбор конкретной даты\n"
        f"• 🩸 8 групп крови\n"
        f"• ⏰ Автоматический поиск доступных дат\n"
        f"• 📊 Статистика из Google Таблиц\n"
        f"• 🔒 Улучшенная безопасность\n\n"
        f"*Выберите действие:*",
        parse_mode="Markdown",
        reply_markup=get_main_menu_keyboard()
    )

async def process_main_menu(callback: CallbackQuery, state: FSMContext):
    """Обработка главного меню"""
    user = callback.from_user
    
    session_timeout.update_activity(user.id)
    
    if not rate_limiter.is_allowed(user.id):
        wait_time = int(rate_limiter.get_wait_time(user.id))
        await callback.answer(f"⏳ Подождите {wait_time} секунд", show_alert=True)
        return
    
    action = callback.data
    
    if action == CallbackData.MAIN_RECORD:
        await callback.message.edit_text(
            "🩸 *Выберите вашу группу крови:*\n\n"
            "• 🅰️ A+ - первая положительная\n"
            "• 🅰️ A- - первая отрицательная\n"
            "• 🅱️ B+ - вторая положительная\n"
            "• 🅱️ B- - вторая отрицательная\n"
            "• 🆎 AB+ - третья положительная\n"
            "• 🆎 AB- - третья отрицательная\n"
            "• 🅾️ O+ - четвертая положительная\n"
            "• 🅾️ O- - четвертая отрицательная",
            parse_mode="Markdown",
            reply_markup=get_blood_group_keyboard()
        )
        await state.set_state(Form.waiting_for_blood_group)
        await state.update_data(is_check_command=False)
    
    elif action == CallbackData.MAIN_CHECK:
        await callback.message.edit_text(
            "🔍 *Проверка доступного времени*\n\n"
            "Выберите вашу группу крови:",
            parse_mode="Markdown",
            reply_markup=get_blood_group_keyboard()
        )
        await state.set_state(Form.waiting_for_blood_group)
        await state.update_data(is_check_command=True)
    
    elif action == CallbackData.MAIN_MYBOOKINGS:
        await show_my_bookings(callback.message, user)
    
    elif action == CallbackData.MAIN_STATS:
        await show_stats(callback.message)
    
    elif action == CallbackData.MAIN_HELP:
        await help_command(callback.message)
    
    await callback.answer()

async def process_blood_group(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора группы крови"""
    user = callback.from_user
    
    session_timeout.update_activity(user.id)
    
    if callback.data == CallbackData.CANCEL:
        await cancel_command(callback.message, state)
        await callback.answer()
        return
    
    if callback.data == CallbackData.MAIN_MENU:
        await show_main_menu_from_callback(callback)
        await state.clear()
        await callback.answer()
        return
    
    if callback.data == CallbackData.BACK_TO_BLOOD:
        await callback.answer()
        return
    
    if not CallbackData.is_blood(callback.data):
        await callback.answer("Пожалуйста, выберите группу крови", show_alert=True)
        return
    
    blood_group = callback.data[len(CallbackData.BLOOD_PREFIX):]
    await state.update_data(blood_group=blood_group)
    
    response = await storage.get_available_dates(user.id)
    
    if response.status == 'error':
        await callback.message.edit_text(
            f"❌ *Ошибка получения дат:* {response.data}\n\n"
            f"Попробуйте позже.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return
    
    available_dates = response.data['available_dates']
    
    if not available_dates:
        await callback.message.edit_text(
            "😔 *Нет доступных дат для записи*\n\n"
            "К сожалению, на ближайшие дни нет свободных мест.\n"
            "Попробуйте позже или обратитесь в регистратуру.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return
    
    user_data = await state.get_data()
    is_check = user_data.get('is_check_command', False)
    
    action_text = "проверки" if is_check else "записи"
    
    dates_text = ""
    for i, date_info in enumerate(available_dates[:Config.MAX_DATES_TO_SHOW]):
        dates_text += f"• *{date_info['day_of_week']}* - {date_info['display_date']}\n"
    
    await callback.message.edit_text(
        f"📅 *Выберите дату для {action_text}:*\n\n"
        f"🩸 Выбранная группа крови: *{blood_group}*\n\n"
        f"*Доступные даты:*\n{dates_text}",
        parse_mode="Markdown",
        reply_markup=get_dates_keyboard(available_dates)
    )
    
    await state.set_state(Form.waiting_for_date)
    await callback.answer(f"Выбрана группа крови {blood_group}")

async function process_date(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора даты"""
    user = callback.from_user
    
    session_timeout.update_activity(user.id)
    
    if callback.data == CallbackData.CANCEL:
        await cancel_command(callback.message, state)
        await callback.answer()
        return
    
    if callback.data == CallbackData.BACK_TO_BLOOD:
        await callback.message.edit_text(
            "🩸 *Выберите вашу группу крови:*",
            parse_mode="Markdown",
            reply_markup=get_blood_group_keyboard()
        )
        await state.set_state(Form.waiting_for_blood_group)
        await callback.answer()
        return
    
    if not CallbackData.is_date(callback.data):
        await callback.answer("Пожалуйста, выберите дату", show_alert=True)
        return
    
    selected_date = callback.data[len(CallbackData.DATE_PREFIX):]
    
    user_data = await state.get_data()
    blood_group = user_data.get('blood_group')
    
    if not blood_group:
        await callback.message.edit_text(
            "❌ *Ошибка:* Группа крови не выбрана\n\n"
            "Пожалуйста, начните запись заново.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return
    
    await state.update_data(selected_date=selected_date)
    
    try:
        date_obj = datetime.strptime(selected_date, "%Y-%m-%d")
        display_date = date_obj.strftime("%d.%m.%Y")
        
        days_ru = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        day_of_week = days_ru[date_obj.weekday()]
        
    except ValueError:
        display_date = selected_date
        day_of_week = "неизвестно"
    
    response = await storage.get_free_times(selected_date, blood_group)
    
    if response.status == 'error':
        await callback.message.edit_text(
            f"❌ *Ошибка:* {response.data}\n\n"
            f"Попробуйте выбрать другую дату.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await callback.answer()
        return
    
    times = response.data['times']
    quota = response.data['quota']
    
    is_check = user_data.get('is_check_command', False)
    
    if not times:
        if is_check:
            await callback.message.edit_text(
                f"📅 *На {display_date} ({day_of_week}) для группы {blood_group} все квоты заняты.*\n"
                f"📊 Осталось мест: {quota}\n\n"
                f"Попробуйте выбрать другую дату.",
                parse_mode="Markdown",
                reply_markup=get_main_menu_keyboard()
            )
        else:
            # Получаем актуальный список дат
            dates_response = await storage.get_available_dates(user.id)
            if dates_response.status == 'success':
                available_dates = dates_response.data['available_dates']
            else:
                available_dates = []
                
            await callback.message.edit_text(
                f"❌ *На {display_date} ({day_of_week}) для группы {blood_group} все квоты заняты.*\n"
                f"📊 Осталось мест: {quota}\n\n"
                f"*Выберите другую дату:*",
                parse_mode="Markdown",
                reply_markup=get_dates_keyboard(available_dates)
            )
        await state.clear() if is_check else None
        await callback.answer()
        return
    
    if is_check:
        # Группируем время для красивого отображения
        time_groups = {}
        for t in times:
            hour = t.split(':')[0]
            minute = t.split(':')[1]
            
            if hour not in time_groups:
                time_groups[hour] = []
            time_groups[hour].append(minute)
        
        sorted_hours = sorted(time_groups.keys())
        
        grouped_text = ""
        for hour in sorted_hours:
            minutes = time_groups[hour]
            minutes_sorted = sorted(minutes)
            minutes_str = ", ".join(minutes_sorted)
            grouped_text += f"• {hour}:{minutes_str}\n"
        
        time_count = len(times)
        slot_word = "слот" if time_count == 1 else "слота" if 2 <= time_count <= 4 else "слотов"
        
        await callback.message.edit_text(
            f"📅 *Доступное время на {display_date}:*\n"
            f"📋 {day_of_week}\n"
            f"🩸 Группа крови: {blood_group}\n"
            f"📊 Свободно {time_count} {slot_word} из {quota}\n\n"
            f"*Временные слоты:*\n{grouped_text}\n"
            f"Для записи нажмите 'Записаться' в главном меню.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
    else:
        current_step = 2
        total_steps = 3
        
        await callback.message.edit_text(
            f"✅ *Доступное время на {display_date}:*\n"
            f"📋 {day_of_week}\n"
            f"🩸 Группа крови: {blood_group}\n"
            f"📊 Свободных мест: {quota}\n\n"
            f"*Выберите удобное время:*",
            parse_mode="Markdown",
            reply_markup=get_times_keyboard(times, current_step, total_steps)
        )
        await state.set_state(Form.waiting_for_time)
    
    await callback.answer(f"Выбрана дата {display_date}")

async def process_time(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора времени"""
    user = callback.from_user
    
    session_timeout.update_activity(user.id)
    
    if callback.data == CallbackData.CANCEL:
        await cancel_command(callback.message, state)
        await callback.answer()
        return
    
    if callback.data == CallbackData.BACK_TO_DATE:
        user_data = await state.get_data()
        blood_group = user_data.get('blood_group')
        
        # Получаем актуальный список дат
        dates_response = await storage.get_available_dates(user.id, force_refresh=True)
        if dates_response.status == 'success':
            available_dates = dates_response.data['available_dates']
        else:
            available_dates = []
        
        dates_text = ""
        for i, date_info in enumerate(available_dates[:Config.MAX_DATES_TO_SHOW]):
            dates_text += f"• *{date_info['day_of_week']}* - {date_info['display_date']}\n"
        
        await callback.message.edit_text(
            f"📅 *Выберите дату:*\n\n"
            f"🩸 Группа крови: *{blood_group}*\n\n"
            f"*Доступные даты:*\n{dates_text}",
            parse_mode="Markdown",
            reply_markup=get_dates_keyboard(available_dates)
        )
        await state.set_state(Form.waiting_for_date)
        await callback.answer()
        return
    
    if callback.data == CallbackData.PROGRESS_INFO:
        await callback.answer("Прогресс записи: выбор времени", show_alert=True)
        return
    
    if not CallbackData.is_time(callback.data):
        await callback.answer("Пожалуйста, выберите время", show_alert=True)
        return
    
    selected_time = callback.data.split("_", 1)[1]
    user_data = await state.get_data()
    
    selected_date = user_data.get('selected_date')
    blood_group = user_data.get('blood_group')
    
    if not selected_date or not blood_group:
        await callback.message.edit_text(
            "❌ *Ошибка:* Отсутствуют данные записи\n\n"
            "Пожалуйста, начните запись заново.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return
    
    try:
        date_obj = datetime.strptime(selected_date, "%Y-%m-%d")
        display_date = date_obj.strftime("%d.%m.%Y")
    except ValueError:
        display_date = selected_date
    
    check_response = await storage.check_existing(selected_date, user.id)
    
    if check_response.status == 'error':
        await callback.message.edit_text(
            f"❌ *Ошибка проверки:* {check_response.data}\n\n"
            f"Попробуйте позже.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return
    
    if check_response.data['exists']:
        existing = check_response.data
        await callback.message.edit_text(
            f"⚠️ *У вас уже есть запись на {display_date}!*\n\n"
            f"🎫 Ваш талон: {existing['ticket']}\n"
            f"⏰ Время: {existing['time']}\n\n"
            f"📌 *Одна запись в день на пользователя.*\n"
            f"Для отмены перейдите в 'Мои записи'.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return
    
    response = await storage.register(selected_date, blood_group, selected_time, user.id)
    
    if response.status == 'error':
        # Получаем актуальные времена для повторного выбора
        times_response = await storage.get_free_times(selected_date, blood_group)
        if times_response.status == 'success':
            times = times_response.data['times']
        else:
            times = []
            
        await callback.message.edit_text(
            f"❌ *Ошибка регистрации:* {response.data}\n\n"
            f"Попробуйте выбрать другое время.",
            parse_mode="Markdown",
            reply_markup=get_times_keyboard(times, 2, 3)
        )
        await callback.answer()
        return
    
    ticket_data = response.data
    
    ticket_text = (
        "🎫 *ВАШ ТАЛОН НА ДОНОРСТВО*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• 🎫 Номер: *{ticket_data['ticket']}*\n"
        f"• 📅 Дата: *{display_date}*\n"
        f"• 📋 День: *{ticket_data['day']}*\n"
        f"• ⏰ Время: *{ticket_data['time']}*\n"
        f"• 🩸 Группа крови: *{ticket_data['blood_group']}*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Осталось мест в этот день: *{ticket_data['quota_remaining']}*\n\n"
        f"👤 ID пользователя: `{user.id}`\n\n"
        "⚠️ *Пожалуйста, приходите за 10 минут до назначенного времени.*\n"
        "📌 *Одна запись в день на пользователя.*"
    )
    
    await callback.message.edit_text(
        ticket_text,
        parse_mode="Markdown",
        reply_markup=get_main_menu_keyboard()
    )
    
    await state.clear()
    await callback.answer("✅ Запись успешно оформлена!")

# ========== ФУНКЦИИ КОМАНД ==========
async function cancel_command(message: types.Message, state: FSMContext):
    """Команда /cancel - отмена текущего диалога"""
    current_state = await state.get_state()
    
    if current_state is None:
        await message.answer(
            "ℹ️ *Нет активного диалога для отмены.*\n"
            "Используйте кнопки ниже для навигации:",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        return
    
    await state.clear()
    
    await message.answer(
        "✅ *Текущий диалог отменен.*\n"
        "Все данные очищены.\n\n"
        "*Выберите действие:*",
        parse_mode="Markdown",
        reply_markup=get_main_menu_keyboard()
    )

async def help_command(message: types.Message):
    """Команда /help"""
    help_text = (
        "📋 *Помощь по боту v4.0:*\n\n"
        "*Основные функции:*\n"
        "• 📋 Записаться на донорство\n"
        "• 🔍 Проверить доступное время\n"
        "• 📖 Посмотреть свои записи\n"
        "• 📊 Показать статистику\n"
        "• ❌ Отменить свою запись\n\n"
        "*Новые возможности:*\n"
        "📅 *Выбор конкретных дат*\n"
        "🩸 *8 групп крови*\n"
        "⚡ *Автоматический поиск дат*\n"
        "⏰ *Таймаут сессии* 10 минут\n"
        "🔒 *Улучшенная безопасность*\n"
        "🔄 *Исправлена совместимость с Google Script*\n\n"
        "*Правила:*\n"
        "📌 Одна запись в день на пользователя\n"
        "📅 Запись на ближайшие доступные даты\n"
        "👥 Квоты разделены по группам крови\n\n"
        "*Режимы работы:*\n"
        "🔧 *LOCAL* - автономный режим\n"
        "🌐 *GOOGLE* - данные в Google Таблицах\n"
        "⚡ *HYBRID* - автоматическое переключение\n\n"
        "*Администраторские функции:*\n"
        "🔄 Обновить кэш из Google Таблиц\n"
        "🗑️ Очистить кэш квот\n"
        "📊 Проверить квоты\n"
        "🔄 Сбросить все данные\n\n"
        "По вопросам обращайтесь к администратору."
    )
    
    await message.answer(
        help_text,
        parse_mode="Markdown",
        reply_markup=get_main_menu_keyboard()
    )

async def mybookings_command(message: types.Message):
    """Команда /mybookings - посмотреть мои записи"""
    user = message.from_user
    await show_my_bookings(message, user)

async function show_my_bookings(message: types.Message, user: types.User):
    """Показать записи пользователя"""
    response = await storage.get_user_bookings(user.id)
    
    if response.status == 'error':
        await message.answer(
            f"❌ *Ошибка получения записей:* {response.data}",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        return
    
    bookings = response.data['bookings']
    
    if not bookings:
        await message.answer(
            f"📋 *Ваши записи*\n\n"
            f"👤 Пользователь: {user.full_name or 'ID: ' + str(user.id)}\n"
            f"🔢 Ваш ID: `{user.id}`\n\n"
            f"*У вас нет активных записей.*\n\n"
            f"Для записи нажмите кнопку ниже:",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        builder = InlineKeyboardBuilder()
        
        bookings_text = ""
        for i, booking in enumerate(bookings):
            try:
                date_obj = datetime.strptime(booking['date'], "%Y-%m-%d")
                display_date = date_obj.strftime("%d.%m.%Y")
            except ValueError:
                display_date = booking['date']
            
            bookings_text += f"• *{display_date}* ({booking['day']}): {booking['time']} (талон: {booking['ticket']}, группа: {booking['blood_group']})\n"
            
            builder.row(
                InlineKeyboardButton(
                    text=f"❌ Отменить запись на {display_date}",
                    callback_data=f"{CallbackData.CANCEL_ASK_PREFIX}{booking['date']}_{booking['ticket']}"
                )
            )
        
        builder.row(
            InlineKeyboardButton(text="🔙 В главное меню", callback_data=CallbackData.MAIN_MENU)
        )
        
        await message.answer(
            f"📋 *Ваши записи*\n\n"
            f"👤 Пользователь: {user.full_name or 'ID: ' + str(user.id)}\n"
            f"🔢 Ваш ID: `{user.id}`\n\n"
            f"*Активные записи:*\n{bookings_text}\n"
            f"📌 *Одна запись в день на пользователя.*\n"
            f"ℹ️ *Для отмены записи нажмите соответствующую кнопку ниже.*",
            parse_mode="Markdown",
            reply_markup=builder.as_markup()
        )

async def stats_command(message: types.Message):
    """Команда /stats - показать статистику"""
    await show_stats(message)

async function show_stats(message: types.Message):
    """Показать статистику"""
    stats_response = await storage.get_stats()
    
    if stats_response.status == 'error':
        await message.answer(
            f"❌ *Ошибка получения статистики:* {stats_response.data}",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        return
    
    stats_data = stats_response.data
    
    # Безопасно получаем данные со значениями по умолчанию
    total_bookings = stats_data.get("total_bookings", 0)
    total_users = stats_data.get("total_users", 0)
    most_popular_day = stats_data.get("most_popular_day", "нет данных")
    most_popular_blood = stats_data.get("most_popular_blood_group", "нет данных")
    
    day_stats = stats_data.get("day_stats", {})
    blood_group_stats = stats_data.get("blood_group_stats", {})
    quota_stats = stats_data.get("quota_stats", {})
    
    # Форматируем статистику по дням
    day_stats_text = ""
    if isinstance(day_stats, dict):
        valid_days = []
        for day, count in day_stats.items():
            if isinstance(count, (int, float)) and count > 0:
                valid_days.append((day, count))
        
        if valid_days:
            sorted_days = sorted(valid_days, key=lambda x: x[1], reverse=True)[:5]
            for day, count in sorted_days:
                day_stats_text += f"• *{day}*: {count} зап.\n"
    
    if not day_stats_text:
        day_stats_text = "• Нет данных\n"
    
    # Форматируем статистику по группам крови
    blood_stats_text = ""
    if isinstance(blood_group_stats, dict):
        valid_blood = []
        for bg, count in blood_group_stats.items():
            if isinstance(count, (int, float)) and count > 0:
                valid_blood.append((bg, count))
        
        if valid_blood:
            sorted_bg = sorted(valid_blood, key=lambda x: x[1], reverse=True)
            for bg, count in sorted_bg:
                blood_stats_text += f"• *{bg}*: {count} зап.\n"
    
    if not blood_stats_text:
        blood_stats_text = "• Нет данных\n"
    
    # Форматируем информацию о квотах
    quota_info = ""
    if isinstance(quota_stats, dict):
        total_quota = quota_stats.get('totalQuota', 0)
        total_used = quota_stats.get('totalUsed', 0)
        remaining = quota_stats.get('remaining', total_quota - total_used)
        
        quota_info = f"📊 *Общая квота:* {total_quota} мест\n"
        quota_info += f"✅ *Использовано:* {total_used} мест\n"
        quota_info += f"⏳ *Осталось:* {remaining} мест\n\n"
    
    mode_info = {
        "LOCAL": "🔧 *АВТОНОМНЫЙ РЕЖИМ*",
        "GOOGLE": "🌐 *РЕЖИМ GOOGLE SCRIPT*",
        "HYBRID": "⚡ *ГИБРИДНЫЙ РЕЖИМ*"
    }.get(Config.MODE, "")
    
    stats_text = (
        f"📊 *Статистика донорской станции*\n\n"
        f"👥 *Всего пользователей:* {total_users}\n"
        f"📋 *Всего записей:* {total_bookings}\n"
        f"📅 *Популярный день:* {most_popular_day}\n"
        f"🩸 *Популярная группа:* {most_popular_blood}\n\n"
        f"{quota_info}"
        f"*Записи по дням:*\n{day_stats_text}"
        f"*Записи по группам крови:*\n{blood_stats_text}"
        f"{mode_info}"
    )
    
    # Создаем клавиатуру
    builder = InlineKeyboardBuilder()
    
    if message.from_user.id in Config.ADMIN_IDS:
        builder.row(
            InlineKeyboardButton(text="🗑️ Очистить кэш", callback_data=CallbackData.ADMIN_CLEAR_CACHE),
            InlineKeyboardButton(text="🔄 Обновить кэш", callback_data=CallbackData.ADMIN_REFRESH_CACHE)
        )
        builder.row(
            InlineKeyboardButton(text="📊 Проверить квоты", callback_data=CallbackData.ADMIN_SHOW_QUOTAS),
            InlineKeyboardButton(text="🔄 Сбросить данные", callback_data=CallbackData.ADMIN_RESET)
        )
    
    builder.row(
        InlineKeyboardButton(text="🔙 В главное меню", callback_data=CallbackData.MAIN_MENU)
    )
    
    await message.answer(
        stats_text,
        parse_mode="Markdown",
        reply_markup=builder.as_markup()
    )

async def show_quotas(message: types.Message):
    """Показать информацию о квотах (только для админов)"""
    if message.from_user.id not in Config.ADMIN_IDS:
        await message.answer(
            "⛔ *У вас нет прав для просмотра квот.*",
            parse_mode="Markdown"
        )
        return
    
    quotas_response = await storage.get_quotas()
    
    if quotas_response.status == 'error':
        await message.answer(
            f"❌ *Ошибка получения квот:* {quotas_response.data}",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
        return
    
    quotas_data = quotas_response.data
    
    # Проверяем структуру ответа
    if isinstance(quotas_data, dict) and 'quotas' in quotas_data:
        quotas = quotas_data['quotas']
        message_text = quotas_data.get('message', 'Информация о квотах')
    else:
        await message.answer(
            f"📊 *Информация о квотах*\n\n{quotas_data}",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
        return
    
    # Получаем данные с безопасными значениями по умолчанию
    total_quota = quotas.get('totalQuota', 0)
    total_used = quotas.get('totalUsed', 0)
    remaining = quotas.get('remaining', total_quota - total_used)
    by_day = quotas.get('byDay', {})
    
    text = f"📊 *КВОТЫ ДОНОРСКОЙ СТАНЦИИ*\n\n"
    text += f"📋 *Всего квот:* {total_quota}\n"
    text += f"✅ *Использовано:* {total_used}\n"
    text += f"⏳ *Осталось:* {remaining}\n\n"
    
    if by_day:
        text += f"*Детали по дням:*\n"
        for day, day_data in by_day.items():
            day_total = day_data.get('total', 0)
            day_used = day_data.get('used', 0)
            day_remaining = day_data.get('remaining', day_total - day_used)
            text += f"\n📅 *{day}*:\n"
            text += f"  Всего: {day_total}, Использовано: {day_used}, Осталось: {day_remaining}\n"
            
            day_quotas = day_data.get('quotas', {})
            if day_quotas:
                # Форматируем квоты по группам крови в строку
                quotas_list = []
                for bg, q in day_quotas.items():
                    if q > 0:
                        quotas_list.append(f"{bg}: {q}")
                if quotas_list:
                    text += f"  Квоты по группам: {', '.join(quotas_list)}\n"
    else:
        text += f"\n*Детали по дням:*\n• Нет данных\n"
    
    # Создаем клавиатуру
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data=CallbackData.ADMIN_REFRESH_CACHE),
        InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.MAIN_STATS)
    )
    
    await message.answer(
        text,
        parse_mode="Markdown",
        reply_markup=builder.as_markup()
    )

async def reset_command(message: types.Message):
    """Команда /reset - сбросить кэш и обновить данные (только для админов)"""
    try:
        if message.from_user.id not in Config.ADMIN_IDS:
            await message.answer(
                "⛔ У вас нет прав для выполнения этой команды.",
                reply_markup=get_admin_keyboard()
            )
            return
        
        # Сообщение о начале процесса
        msg = await message.answer(
            "🔄 Очистка кэша и обновление данных...",
            reply_markup=None
        )
        
        # Очищаем кэш
        storage.clear_cache()
        
        # Принудительно обновляем данные
        refresh_result = await storage.get_available_dates(message.from_user.id, force_refresh=True)
        
        if refresh_result.status == 'success':
            await msg.edit_text(
                "✅ Кэш успешно сброшен и данные обновлены!\n\n"
                f"📊 Доступно дат: {refresh_result.data.get('count', 0)}",
                parse_mode="Markdown",
                reply_markup=get_admin_keyboard()
            )
        else:
            await msg.edit_text(
                f"⚠️ Кэш очищен, но ошибка обновления данных: {refresh_result.data}",
                parse_mode="Markdown",
                reply_markup=get_admin_keyboard()
            )
    
    except Exception as e:
        print(f"[RESET] ❌ Критическая ошибка: {str(e)}")
        await message.answer(
            f"❌ Критическая ошибка при выполнении /reset: {str(e)}",
            reply_markup=get_admin_keyboard()
        )

async def clear_cache_command(message: types.Message):
    """Команда /clearcache - очистить кэш квот (только для админов)"""
    try:
        if message.from_user.id not in Config.ADMIN_IDS:
            await message.answer(
                "⛔ *У вас нет прав для выполнения этой команды.*",
                parse_mode="Markdown"
            )
            return
        
        msg = await message.answer(
            "🔄 *Очистка кэша Google Script...*",
            parse_mode="Markdown"
        )
        
        storage.clear_cache()
        
        await msg.edit_text(
            "✅ *Кэш квот успешно очищен!*\n\n"
            "Теперь будут загружены свежие данные из Google Таблиц.",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
    
    except Exception as e:
        print(f"[ERROR] clear_cache_command: {e}")
        await message.answer(
            f"❌ Критическая ошибка: {str(e)}",
            reply_markup=get_admin_keyboard()
        )

async def refresh_cache_command(message: types.Message):
    """Команда /refresh - обновить кэш из Google Таблиц (только для админов)"""
    try:
        if message.from_user.id not in Config.ADMIN_IDS:
            await message.answer(
                "⛔ *У вас нет прав для выполнения этой команды.*",
                parse_mode="Markdown"
            )
            return
        
        if Config.MODE in ["GOOGLE", "HYBRID"]:
            msg = await message.answer("🔄 *Обновление кэша из Google Таблиц...*", parse_mode="Markdown")
            
            result = await storage.get_available_dates(message.from_user.id, force_refresh=True)
            
            if result.status == "success":
                await msg.edit_text(
                    "✅ *Кэш успешно обновлен из Google Таблиц!*\n\n"
                    "Теперь отображаются актуальные данные.\n"
                    f"📊 Доступно дат: {result.data.get('count', 0)}",
                    parse_mode="Markdown",
                    reply_markup=get_admin_keyboard()
                )
            else:
                await msg.edit_text(
                    f"❌ *Ошибка обновления кэша:* {result.data}",
                    parse_mode="Markdown",
                    reply_markup=get_admin_keyboard()
                )
        else:
            await message.answer(
                "ℹ️ *В локальном режиме кэш не используется.*\n"
                "Данные берутся напрямую из памяти бота.",
                parse_mode="Markdown",
                reply_markup=get_admin_keyboard()
            )
    
    except Exception as e:
        print(f"[ERROR] refresh_cache_command: {e}")
        await message.answer(
            f"❌ Критическая ошибка: {str(e)}",
            reply_markup=get_admin_keyboard()
        )

async def process_cancel_booking(callback: CallbackQuery, state: FSMContext):
    """Обработка отмены записи и админских действий"""
    try:
        session_timeout.update_activity(callback.from_user.id)
        
        if callback.data == CallbackData.CANCEL_NO:
            await callback.message.edit_text(
                "✅ *Отмена записи отменена.*\n\n"
                "Ваша запись сохранена.",
                parse_mode="Markdown",
                reply_markup=get_main_menu_keyboard()
            )
            await callback.answer()
            return
        
        if CallbackData.is_cancel_yes(callback.data):
            parts = callback.data.split("_")
            if len(parts) >= 4:
                date = parts[2]
                ticket = "_".join(parts[3:])
                
                response = await storage.cancel_booking(date, ticket, callback.from_user.id)
                
                if response.status == 'success':
                    try:
                        date_obj = datetime.strptime(date, "%Y-%m-%d")
                        display_date = date_obj.strftime("%d.%m.%Y")
                    except ValueError:
                        display_date = date
                    
                    await callback.message.edit_text(
                        f"✅ *Запись успешно отменена!*\n\n"
                        f"📅 Дата: *{display_date}*\n"
                        f"🎫 Талон: *{ticket}*\n\n"
                        f"Теперь вы можете записаться на другое время.",
                        parse_mode="Markdown",
                        reply_markup=get_main_menu_keyboard()
                    )
                else:
                    await callback.message.edit_text(
                        f"❌ *Ошибка отмены записи:* {response.data}\n\n"
                        f"Попробуйте позже или обратитесь к администратору.",
                        parse_mode="Markdown",
                        reply_markup=get_main_menu_keyboard()
                    )
            else:
                await callback.message.edit_text(
                    "❌ *Ошибка обработки запроса на отмену.*",
                    parse_mode="Markdown",
                    reply_markup=get_main_menu_keyboard()
                )
            
            await callback.answer()
            return
        
        if CallbackData.is_cancel_ask(callback.data):
            parts = callback.data.split("_")
            if len(parts) >= 4:
                date = parts[2]
                ticket = "_".join(parts[3:])
                
                try:
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    display_date = date_obj.strftime("%d.%m.%Y")
                except ValueError:
                    display_date = date
                
                await callback.message.edit_text(
                    f"⚠️ *Подтверждение отмены записи*\n\n"
                    f"📅 Дата: *{display_date}*\n"
                    f"🎫 Номер талона: *{ticket}*\n\n"
                    f"Вы уверены, что хотите отменить эту запись?",
                    parse_mode="Markdown",
                    reply_markup=get_confirm_cancellation_keyboard(date, ticket)
                )
            
            await callback.answer()
            return
        
        if callback.data == CallbackData.MAIN_MENU:
            await show_main_menu_from_callback(callback)
            await state.clear()
            await callback.answer()
            return
        
        if callback.data == CallbackData.ADMIN_SHOW_QUOTAS:
            if callback.from_user.id not in Config.ADMIN_IDS:
                await callback.answer("⛔ У вас нет прав для этой операции", show_alert=True)
                return
            
            quotas_response = await storage.get_quotas()
            
            if quotas_response.status == 'error':
                await callback.message.edit_text(
                    f"❌ *Ошибка получения квот:* {quotas_response.data}",
                    parse_mode="Markdown",
                    reply_markup=get_admin_keyboard()
                )
                await callback.answer()
                return
            
            quotas_data = quotas_response.data
            
            if isinstance(quotas_data, dict) and 'quotas' in quotas_data:
                quotas = quotas_data['quotas']
            else:
                await callback.message.edit_text(
                    f"📊 *Информация о квотах*\n\n{quotas_data}",
                    parse_mode="Markdown",
                    reply_markup=get_admin_keyboard()
                )
                await callback.answer()
                return
            
            total_quota = quotas.get('totalQuota', 0)
            total_used = quotas.get('totalUsed', 0)
            remaining = quotas.get('remaining', total_quota - total_used)
            by_day = quotas.get('byDay', {})
            
            text = f"📊 *КВОТЫ ДОНОРСКОЙ СТАНЦИИ*\n\n"
            text += f"📋 *Всего квот:* {total_quota}\n"
            text += f"✅ *Использовано:* {total_used}\n"
            text += f"⏳ *Осталось:* {remaining}\n\n"
            text += f"*Детали по дням:*\n"
            
            for day, day_data in by_day.items():
                day_total = day_data.get('total', 0)
                day_used = day_data.get('used', 0)
                day_remaining = day_data.get('remaining', day_total - day_used)
                text += f"\n📅 *{day}*:\n"
                text += f"  Всего: {day_total}, Использовано: {day_used}, Осталось: {day_remaining}\n"
                
                day_quotas = day_data.get('quotas', {})
                if day_quotas:
                    quotas_text = ", ".join([f"{bg}: {q}" for bg, q in day_quotas.items()])
                    text += f"  Квоты по группам: {quotas_text}\n"
            
            builder = InlineKeyboardBuilder()
            builder.row(
                InlineKeyboardButton(text="🔄 Обновить", callback_data=CallbackData.ADMIN_REFRESH_CACHE),
                InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.MAIN_STATS)
            )
            
            await callback.message.edit_text(
                text,
                parse_mode="Markdown",
                reply_markup=builder.as_markup()
            )
            await callback.answer()
            return
        
        if callback.data == CallbackData.ADMIN_RESET:
            if callback.from_user.id not in Config.ADMIN_IDS:
                await callback.answer("⛔ У вас нет прав для этой операции", show_alert=True)
                return
            
            # Очищаем кэш
            storage.clear_cache()
            
            # Принудительно обновляем данные
            result = await storage.get_available_dates(callback.from_user.id, force_refresh=True)
            
            if result.status == 'success':
                await callback.message.edit_text(
                    f"✅ *Кэш успешно сброшен и данные обновлены!*\n\n"
                    f"📊 Доступно дат: {result.data.get('count', 0)}",
                    parse_mode="Markdown",
                    reply_markup=get_admin_keyboard()
                )
            else:
                await callback.message.edit_text(
                    f"⚠️ Кэш очищен, но ошибка: {result.data}",
                    parse_mode="Markdown",
                    reply_markup=get_admin_keyboard()
                )
            
            await callback.answer()
            return
        
        if callback.data == CallbackData.ADMIN_CLEAR_CACHE:
            if callback.from_user.id not in Config.ADMIN_IDS:
                await callback.answer("⛔ У вас нет прав для этой операции", show_alert=True)
                return
            
            storage.clear_cache()
            
            await callback.message.edit_text(
                "✅ *Кэш квот успешно очищен!*\n\n"
                "Теперь будут загружены свежие данные из Google Таблиц.",
                parse_mode="Markdown",
                reply_markup=get_admin_keyboard()
            )
            await callback.answer()
            return
        
        if callback.data == CallbackData.ADMIN_REFRESH_CACHE:
            if callback.from_user.id not in Config.ADMIN_IDS:
                await callback.answer("⛔ У вас нет прав для этой операции", show_alert=True)
                return
            
            result = await storage.get_available_dates(callback.from_user.id, force_refresh=True)
            
            if result.status == 'success':
                await callback.message.edit_text(
                    f"✅ *Кэш успешно обновлен из Google Таблиц!*\n\n"
                    f"Теперь отображаются актуальные данные.\n"
                    f"📊 Доступно дат: {result.data.get('count', 0)}",
                    parse_mode="Markdown",
                    reply_markup=get_admin_keyboard()
                )
            else:
                await callback.message.edit_text(
                    f"❌ *Ошибка обновления кэша:* {result.data}",
                    parse_mode="Markdown",
                    reply_markup=get_admin_keyboard()
                )
            await callback.answer()
            return
        
    except Exception as e:
        print(f"❌ Ошибка в обработке отмены: {e}")
        await callback.message.edit_text(
            "❌ *Произошла ошибка при обработке запроса.*\n"
            "Попробуйте позже или обратитесь к администратору.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard()
        )
        await callback.answer()

async def show_main_menu_from_callback(callback: CallbackQuery):
    """Показать главное меню из callback"""
    user = callback.from_user
    greeting_name = user.first_name if user.first_name else "пользователь"
    
    session_timeout.update_activity(user.id)
    
    mode_info = {
        "LOCAL": "🔧 Автономный режим",
        "GOOGLE": "🌐 Режим Google Script",
        "HYBRID": "⚡ Гибридный режим"
    }.get(Config.MODE, "❓ Неизвестный режим")
    
    is_admin = user.id in Config.ADMIN_IDS
    admin_text = "\n👑 *Вы администратор* - доступны дополнительные функции" if is_admin else ""
    
    await callback.message.edit_text(
        f"🎯 *Донорская станция v4.0*\n"
        f"{mode_info}\n\n"
        f"👋 Привет, {greeting_name}!{admin_text}\n\n"
        f"Я помогу вам записаться на донорство крови, "
        f"проверить доступное время или отменить запись.\n\n"
        f"*Выберите действие:*",
        parse_mode="Markdown",
        reply_markup=get_main_menu_keyboard()
    )

async def process_main_menu_button(callback: CallbackQuery, state: FSMContext):
    """Обработка кнопки 'В главное меню'"""
    if callback.data == CallbackData.MAIN_MENU:
        session_timeout.update_activity(callback.from_user.id)
        await show_main_menu_from_callback(callback)
        await state.clear()
        await callback.answer("Главное меню")

# ========== ЗАПУСК БОТА ==========
async def main():
    """Основная функция запуска бота"""
    logging.basicConfig(
        level=logging.INFO if Config.DEBUG else logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    print("=" * 60)
    print("🚀 ЗАПУСК ДОНОРСКОГО БОТА v4.0")
    print("=" * 60)
    
    if Config.MODE in ["GOOGLE", "HYBRID"]:
        print("🔗 Тестирование соединения с Google Script...")
        test_result = google_client.test_connection()
        
        if test_result.status == "success":
            print(f"✅ Google Script доступен")
        else:
            print(f"⚠️ Google Script недоступен: {test_result.data}")
            
            if Config.MODE == "GOOGLE":
                print("❌ Режим GOOGLE выбран, но сервис недоступен!")
                print("🔄 Переключите MODE на 'HYBRID' или 'LOCAL' в .env файле")
                return
            elif Config.MODE == "HYBRID":
                print("🔄 Гибридный режим: будет использоваться локальное хранилище")
    
    print(f"⚡ РЕЖИМ РАБОТЫ: {Config.MODE}")
    print(f"⏰ ТАЙМАУТ СЕССИИ: {Config.SESSION_TIMEOUT} секунд")
    print(f"🔒 SSL ПРОВЕРКА: Включена")
    
    if Config.MODE == "LOCAL":
        print("💾 Данные хранятся в памяти бота")
        print("⚠️ Внимание: При перезапуске бота данные будут сброшены!")
    elif Config.MODE == "GOOGLE":
        print("🌐 Данные хранятся в Google Таблицах")
        print("🔄 Кэш автоматически обновляется")
    elif Config.MODE == "HYBRID":
        print("⚡ Гибридный режим: Google Script + локальное хранилище")
        print("🔄 Автоматическое переключение при ошибках")
    
    print("=" * 60)
    
    # Создаем SSL контекст с правильной проверкой
    ssl_context = ssl.create_default_context()
    # НЕ отключаем проверку! Используем стандартную
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    aiohttp_session = aiohttp.ClientSession(connector=connector)
    
    session = AiohttpSession()
    session._session = aiohttp_session
    
    bot = Bot(token=Config.TOKEN, session=session)
    
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    dp.update.middleware(timeout_middleware)
    
    # Регистрируем команды
    dp.message.register(start_command, Command("start"))
    dp.message.register(cancel_command, Command("cancel"))
    dp.message.register(help_command, Command("help"))
    dp.message.register(mybookings_command, Command("mybookings"))
    dp.message.register(stats_command, Command("stats"))
    dp.message.register(reset_command, Command("reset"))
    dp.message.register(clear_cache_command, Command("clearcache"))
    dp.message.register(refresh_cache_command, Command("refresh"))
    
    # Регистрируем callback-обработчики
    dp.callback_query.register(process_main_menu_button, F.data == CallbackData.MAIN_MENU)
    dp.callback_query.register(process_main_menu, F.data.startswith(("main_", "main_")))
    dp.callback_query.register(process_blood_group, Form.waiting_for_blood_group)
    dp.callback_query.register(process_date, Form.waiting_for_date)
    dp.callback_query.register(process_time, Form.waiting_for_time)
    dp.callback_query.register(process_cancel_booking)
    
    print("✅ Бот инициализирован и готов к работе!")
    print("📱 Отправьте /start в Telegram для начала работы")
    print("=" * 60)
    print("Для остановки нажмите Ctrl+C")
    print("=" * 60)
    
    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        print("\n⚠️ Бот остановлен пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
    finally:
        await aiohttp_session.close()
        print("✅ Сессии закрыты")

if __name__ == "__main__":
    asyncio.run(main())
