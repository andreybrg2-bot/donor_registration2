"""
🎯 БОТ ДЛЯ ЗАПИСИ НА ДОНОРСТВО КРОВИ
Версия: 5.0 (ФИНАЛЬНАЯ, ИСПРАВЛЕНА)
Основана на архитектуре v4.3 + добавлены недостающие обработчики из v3.5
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
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

import aiohttp
import requests
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv

load_dotenv()

# ========== КОНФИГУРАЦИЯ ==========
class Config:
    TOKEN = os.getenv("BOT_TOKEN", "8598969347:AAEqsFqoW0sTO1yeKF49DHIB4-VlOsOESMQ")
    MODE = os.getenv("BOT_MODE", "GOOGLE")
    GOOGLE_SCRIPT_URL = os.getenv("GOOGLE_SCRIPT_URL",
        "https://script.google.com/macros/s/AKfycbyZBk0Byb-y1Z50r1r35kUXChNvJKsNO8ZUhoHOd2vVLQA3QK_XS9RyltNGCzXzKFZ-/exec")
    ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "5097581039").split(",") if id.strip()]
    SESSION_TIMEOUT = 600
    CACHE_TTL = 300
    MAX_DATES_TO_SHOW = 6
    RATE_LIMIT_REQUESTS = 15
    RATE_LIMIT_WINDOW = 60
    DEBUG = True

# ========== КОНСТАНТЫ ==========
class CallbackData(str, Enum):
    MAIN_MENU = "main_menu"
    CANCEL = "cancel"
    BACK_TO_BLOOD = "back_to_blood"
    BACK_TO_DATE = "back_to_date"
    PROGRESS_INFO = "progress_info"
    MAIN_RECORD = "main_record"
    MAIN_CHECK = "main_check"
    MAIN_MYBOOKINGS = "main_mybookings"
    MAIN_STATS = "main_stats"
    MAIN_HELP = "main_help"
    ADMIN_CLEAR_CACHE = "admin_clear_cache"
    ADMIN_REFRESH_CACHE = "admin_refresh_cache"
    ADMIN_SHOW_QUOTAS = "admin_show_quotas"
    ADMIN_RESET = "admin_reset"
    CANCEL_NO = "cancel_no"

    BLOOD_PREFIX = "blood_"
    DATE_PREFIX = "date_"
    TIME_PREFIX = "time_"
    CANCEL_ASK_PREFIX = "cancel_ask_"
    CANCEL_YES_PREFIX = "cancel_yes_"

    @classmethod
    def is_blood(cls, data: str) -> bool:
        return data.startswith(cls.BLOOD_PREFIX)

    @classmethod
    def is_date(cls, data: str) -> bool:
        return data.startswith(cls.DATE_PREFIX)

    @classmethod
    def is_time(cls, data: str) -> bool:
        return data.startswith(cls.TIME_PREFIX)

# ========== МОДЕЛИ ==========
@dataclass
class Booking:
    ticket: str
    date: str
    time: str
    blood_group: str
    day: str
    user_id: int
    created_at: Optional[str] = None

@dataclass
class ApiResponse:
    status: str
    data: Union[Dict, str]

    @classmethod
    def success(cls, data: Dict):
        return cls(status="success", data=data)

    @classmethod
    def error(cls, message: str):
        return cls(status="error", data=message)

# ========== КЛИЕНТ GOOGLE SCRIPT ==========
class GoogleScriptClient:
    def __init__(self, script_url: str):
        self.script_url = script_url
        self.timeout = 15
        self.cache = {}
        self.session = requests.Session()

    def test_connection(self) -> ApiResponse:
        try:
            response = self.session.post(
                self.script_url,
                json={"action": "test"},
                timeout=self.timeout
            )
            if response.status_code == 200:
                return ApiResponse.success(response.json())
            return ApiResponse.error(f"HTTP ошибка: {response.status_code}")
        except Exception as e:
            return ApiResponse.error(str(e))

    def call_api(self, action: str, data: Dict = None, user_id: int = None,
                 force_refresh: bool = False) -> ApiResponse:
        if data is None:
            data = {}

        try:
            payload = {"action": action, **data}
            if user_id:
                payload["user_id"] = str(user_id)

            response = self.session.post(
                self.script_url,
                json=payload,
                timeout=self.timeout
            )

            if response.status_code != 200:
                return ApiResponse.error(f"HTTP ошибка: {response.status_code}")

            result = response.json()
            if result.get("status") == "success":
                return ApiResponse.success(result.get("data", {}))
            return ApiResponse.error(result.get("data", "Неизвестная ошибка"))

        except Exception as e:
            return ApiResponse.error(str(e))

    def clear_cache(self):
        self.cache.clear()

# ========== ЛОКАЛЬНОЕ ХРАНИЛИЩЕ ==========
class LocalStorage:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.bookings: Dict[int, Dict[str, Booking]] = {}
        self.working_hours = [
            "07:30", "08:00", "08:30", "09:00", "09:30", "10:00",
            "10:30", "11:00", "11:30", "12:00", "12:30", "13:00", "13:30", "14:00"
        ]
        self.quotas = self._get_default_quotas()
        self._add_test_data()
        print("[LOCAL] Локальное хранилище инициализировано")

    def _get_default_quotas(self):
        base = {"A+": 10, "A-": 5, "B+": 10, "B-": 5, "AB+": 5, "AB-": 3, "O+": 10, "O-": 5}
        weekend = {"A+": 8, "A-": 4, "B+": 8, "B-": 4, "AB+": 3, "AB-": 2, "O+": 8, "O-": 4}
        days = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        quotas = {}
        for day in days:
            quotas[day] = weekend.copy() if day in ["суббота", "воскресенье"] else base.copy()
        return quotas

    def _add_test_data(self):
        today = datetime.now()
        test_data = [
            (111111, today + timedelta(days=1), "09:00", "A+"),
            (222222, today + timedelta(days=2), "10:30", "B-"),
        ]
        for user_id, date, time_slot, blood_group in test_data:
            date_str = date.strftime("%Y-%m-%d")
            day = self._get_day_of_week_ru(date)
            self._add_booking_sync(user_id, date_str, time_slot, blood_group, day)
        print(f"[LOCAL] Добавлено тестовых записей: {len(test_data)}")

    def _add_booking_sync(self, user_id, date, time_slot, blood_group, day):
        ticket = f"Т-{day[:3]}-{blood_group}-{random.randint(1000, 9999)}"
        booking = Booking(ticket, date, time_slot, blood_group, day, user_id, datetime.now().isoformat())
        if user_id not in self.bookings:
            self.bookings[user_id] = {}
        self.bookings[user_id][date] = booking
        return booking

    def _get_day_of_week_ru(self, date_obj):
        days = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        return days[date_obj.weekday()]

    def get_available_dates(self, user_id: int) -> ApiResponse:
        today = datetime.now()
        available_dates = []
        for i in range(1, 31):
            if len(available_dates) >= Config.MAX_DATES_TO_SHOW:
                break
            check_date = today + timedelta(days=i)
            day_of_week = self._get_day_of_week_ru(check_date)
            if day_of_week in self.quotas:
                if any(q > 0 for q in self.quotas[day_of_week].values()):
                    available_dates.append({
                        "date": check_date.strftime("%Y-%m-%d"),
                        "day_of_week": day_of_week,
                        "display_date": check_date.strftime("%d.%m.%Y"),
                        "day_of_week_short": day_of_week[:3],
                        "timestamp": int(check_date.timestamp())
                    })
        return ApiResponse.success({"available_dates": available_dates, "count": len(available_dates)})

    def get_free_times(self, date: str, blood_group: str) -> ApiResponse:
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            day_of_week = self._get_day_of_week_ru(date_obj)
            busy_times = []
            for user_data in self.bookings.values():
                if date in user_data and user_data[date].blood_group == blood_group:
                    busy_times.append(user_data[date].time)
            free_times = [t for t in self.working_hours if t not in busy_times]
            total_quota = self.quotas[day_of_week].get(blood_group, 0)
            return ApiResponse.success({
                "times": free_times,
                "quota": max(0, total_quota - len(busy_times)),
                "quota_total": total_quota,
                "quota_used": len(busy_times)
            })
        except Exception as e:
            return ApiResponse.error(str(e))

    async def check_existing(self, date: str, user_id: int) -> ApiResponse:
        async with self._lock:
            if user_id in self.bookings and date in self.bookings[user_id]:
                b = self.bookings[user_id][date]
                return ApiResponse.success({
                    "exists": True, "ticket": b.ticket, "time": b.time,
                    "blood_group": b.blood_group, "day": b.day, "date": date
                })
            return ApiResponse.success({"exists": False})

    async def register(self, date: str, blood_group: str, time_slot: str, user_id: int) -> ApiResponse:
        async with self._lock:
            try:
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                day_of_week = self._get_day_of_week_ru(date_obj)

                existing = await self.check_existing(date, user_id)
                if existing.data.get("exists"):
                    return ApiResponse.error("У вас уже есть запись на эту дату")

                for u in self.bookings.values():
                    if date in u and u[date].time == time_slot and u[date].blood_group == blood_group:
                        return ApiResponse.error("Время уже занято")

                total = self.quotas[day_of_week].get(blood_group, 0)
                used = sum(1 for u in self.bookings.values()
                          if date in u and u[date].blood_group == blood_group)

                if used >= total:
                    return ApiResponse.error("Все квоты заняты")

                booking = self._add_booking_sync(user_id, date, time_slot, blood_group, day_of_week)
                return ApiResponse.success({
                    "ticket": booking.ticket, "day": booking.day, "date": booking.date,
                    "time": booking.time, "blood_group": booking.blood_group,
                    "quota_remaining": total - used - 1
                })
            except Exception as e:
                return ApiResponse.error(str(e))

    async def cancel_booking(self, date: str, ticket: str, user_id: int) -> ApiResponse:
        async with self._lock:
            if user_id in self.bookings and date in self.bookings[user_id]:
                if self.bookings[user_id][date].ticket == ticket:
                    del self.bookings[user_id][date]
                    if not self.bookings[user_id]:
                        del self.bookings[user_id]
                    return ApiResponse.success({"message": "Запись отменена"})
            return ApiResponse.error("Запись не найдена")

    def get_user_bookings(self, user_id: int) -> ApiResponse:
        if user_id in self.bookings:
            bookings = [{"date": d, "day": b.day, "ticket": b.ticket,
                        "time": b.time, "blood_group": b.blood_group}
                       for d, b in self.bookings[user_id].items()]
            return ApiResponse.success({"bookings": bookings, "count": len(bookings)})
        return ApiResponse.success({"bookings": [], "count": 0})

    def get_stats(self) -> ApiResponse:
        total_bookings = sum(len(u) for u in self.bookings.values())
        total_users = len(self.bookings)
        day_stats = {}
        blood_stats = {}

        for user_data in self.bookings.values():
            for b in user_data.values():
                day_stats[b.day] = day_stats.get(b.day, 0) + 1
                blood_stats[b.blood_group] = blood_stats.get(b.blood_group, 0) + 1

        most_popular_day = "нет данных"
        most_popular_blood = "нет данных"

        if day_stats:
            most_popular_day = max(day_stats.items(), key=lambda x: x[1])[0]
        if blood_stats:
            most_popular_blood = max(blood_stats.items(), key=lambda x: x[1])[0]

        return ApiResponse.success({
            "total_bookings": total_bookings,
            "total_users": total_users,
            "day_stats": day_stats,
            "blood_group_stats": blood_stats,
            "most_popular_day": most_popular_day,
            "most_popular_blood_group": most_popular_blood
        })

# ========== АДАПТЕР ==========
class StorageAdapter:
    def __init__(self, mode: str, google: GoogleScriptClient, local: LocalStorage):
        self.mode = mode
        self.google = google
        self.local = local

    async def get_available_dates(self, user_id: int, **kwargs) -> ApiResponse:
        if self.mode == "LOCAL":
            return self.local.get_available_dates(user_id)
        result = self.google.call_api("get_available_dates", {}, user_id, kwargs.get('force_refresh', False))
        if self.mode == "HYBRID" and result.status == "error":
            return self.local.get_available_dates(user_id)
        return result

    async def get_free_times(self, date: str, blood_group: str) -> ApiResponse:
        if self.mode == "LOCAL":
            return self.local.get_free_times(date, blood_group)
        result = self.google.call_api("get_free_times", {"date": date, "blood_group": blood_group})
        if self.mode == "HYBRID" and result.status == "error":
            return self.local.get_free_times(date, blood_group)
        return result

    async def check_existing(self, date: str, user_id: int) -> ApiResponse:
        if self.mode == "LOCAL":
            return await self.local.check_existing(date, user_id)
        result = self.google.call_api("check_existing", {"date": date}, user_id)
        if self.mode == "HYBRID" and result.status == "error":
            return await self.local.check_existing(date, user_id)
        return result

    async def register(self, date: str, blood_group: str, time_slot: str, user_id: int) -> ApiResponse:
        if self.mode == "LOCAL":
            return await self.local.register(date, blood_group, time_slot, user_id)
        result = self.google.call_api("register", {"date": date, "blood_group": blood_group, "time": time_slot}, user_id)
        if self.mode == "HYBRID" and result.status == "error":
            return await self.local.register(date, blood_group, time_slot, user_id)
        return result

    async def cancel_booking(self, date: str, ticket: str, user_id: int) -> ApiResponse:
        if self.mode == "LOCAL":
            return await self.local.cancel_booking(date, ticket, user_id)
        result = self.google.call_api("cancel_booking", {"date": date, "ticket": ticket}, user_id)
        if self.mode == "HYBRID" and result.status == "error":
            return await self.local.cancel_booking(date, ticket, user_id)
        return result

    async def get_user_bookings(self, user_id: int) -> ApiResponse:
        if self.mode == "LOCAL":
            return self.local.get_user_bookings(user_id)
        result = self.google.call_api("get_user_bookings", {}, user_id)
        if self.mode == "HYBRID" and result.status == "error":
            return self.local.get_user_bookings(user_id)
        return result

    async def get_stats(self) -> ApiResponse:
        if self.mode == "LOCAL":
            return self.local.get_stats()
        result = self.google.call_api("get_stats", {})
        if self.mode == "HYBRID" and result.status == "error":
            return self.local.get_stats()
        return result

    def clear_cache(self):
        if self.mode in ["GOOGLE", "HYBRID"]:
            self.google.clear_cache()

# Инициализация
google_client = GoogleScriptClient(Config.GOOGLE_SCRIPT_URL)
local_storage = LocalStorage()
storage = StorageAdapter(Config.MODE, google_client, local_storage)

# ========== СЕРВИСЫ ==========
class SessionTimeout:
    def __init__(self, timeout: int = Config.SESSION_TIMEOUT):
        self.timeout = timeout
        self.activities: Dict[int, float] = {}

    def update(self, user_id: int):
        self.activities[user_id] = time.time()

    def is_expired(self, user_id: int) -> bool:
        if user_id not in self.activities:
            return False
        return time.time() - self.activities[user_id] > self.timeout

    def clear(self, user_id: int):
        self.activities.pop(user_id, None)

session_timeout = SessionTimeout()

class RateLimiter:
    def __init__(self, max_req: int = Config.RATE_LIMIT_REQUESTS, window: int = Config.RATE_LIMIT_WINDOW):
        self.max_req = max_req
        self.window = window
        self.requests: Dict[int, List[float]] = defaultdict(list)

    def is_allowed(self, user_id: int) -> bool:
        now = time.time()
        self.requests[user_id] = [t for t in self.requests[user_id] if now - t < self.window]
        if len(self.requests[user_id]) >= self.max_req:
            return False
        self.requests[user_id].append(now)
        return True

rate_limiter = RateLimiter()

# ========== СОСТОЯНИЯ ==========
class Form(StatesGroup):
    waiting_for_blood_group = State()
    waiting_for_date = State()
    waiting_for_time = State()

# ========== КЛАВИАТУРЫ ==========
def get_blood_group_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    groups = [
        ("🅰️ A+", f"{CallbackData.BLOOD_PREFIX}A+"), ("🅰️ A-", f"{CallbackData.BLOOD_PREFIX}A-"),
        ("🅱️ B+", f"{CallbackData.BLOOD_PREFIX}B+"), ("🅱️ B-", f"{CallbackData.BLOOD_PREFIX}B-"),
        ("🆎 AB+", f"{CallbackData.BLOOD_PREFIX}AB+"), ("🆎 AB-", f"{CallbackData.BLOOD_PREFIX}AB-"),
        ("🅾️ O+", f"{CallbackData.BLOOD_PREFIX}O+"), ("🅾️ O-", f"{CallbackData.BLOOD_PREFIX}O-")
    ]
    for i in range(0, len(groups), 2):
        builder.row(*[InlineKeyboardButton(text=t, callback_data=d) for t, d in groups[i:i+2]])
    builder.row(
        InlineKeyboardButton(text="🔙 Главное меню", callback_data=CallbackData.MAIN_MENU),
        InlineKeyboardButton(text="❌ Отмена", callback_data=CallbackData.CANCEL)
    )
    return builder.as_markup()

def get_dates_keyboard(dates: List[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if not dates:
        builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.BACK_TO_BLOOD))
        return builder.as_markup()
    for d in dates:
        builder.row(InlineKeyboardButton(
            text=f"{d['day_of_week']}\n{d['display_date']}",
            callback_data=f"{CallbackData.DATE_PREFIX}{d['date']}"
        ))
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.BACK_TO_BLOOD),
        InlineKeyboardButton(text="❌ Отмена", callback_data=CallbackData.CANCEL)
    )
    return builder.as_markup()

def get_times_keyboard(times: List[str]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if not times:
        builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.BACK_TO_DATE))
        return builder.as_markup()
    buttons = [InlineKeyboardButton(text=f"⏰ {t}", callback_data=f"{CallbackData.TIME_PREFIX}{t}") for t in times]
    for i in range(0, len(buttons), 3):
        builder.row(*buttons[i:i+3])
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data=CallbackData.BACK_TO_DATE),
        InlineKeyboardButton(text="❌ Отмена", callback_data=CallbackData.CANCEL)
    )
    return builder.as_markup()

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
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

def get_confirm_cancellation_keyboard(date: str, ticket: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Да, отменить", callback_data=f"{CallbackData.CANCEL_YES_PREFIX}{date}_{ticket}"),
        InlineKeyboardButton(text="❌ Нет, оставить", callback_data=CallbackData.CANCEL_NO)
    )
    return builder.as_markup()

def get_admin_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🗑️ Очистить кэш", callback_data=CallbackData.ADMIN_CLEAR_CACHE),
        InlineKeyboardButton(text="🔄 Обновить кэш", callback_data=CallbackData.ADMIN_REFRESH_CACHE)
    )
    builder.row(InlineKeyboardButton(text="🔙 В главное меню", callback_data=CallbackData.MAIN_MENU))
    return builder.as_markup()

# ========== MIDDLEWARE ДЛЯ ТАЙМАУТА ==========
async def timeout_middleware(handler, event, data):
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
            if session_timeout.is_expired(user_id):
                print(f"[TIMEOUT] Сессия пользователя {user_id} истекла")
                state = data.get('state')
                if state:
                    await state.clear()
                session_timeout.clear(user_id)

                # Игнорируем таймаут для кнопки главного меню
                is_main_menu = False
                if hasattr(event, 'callback_query') and event.callback_query:
                    if hasattr(event.callback_query, 'data') and event.callback_query.data == CallbackData.MAIN_MENU:
                        is_main_menu = True

                if is_main_menu:
                    session_timeout.update(user_id)
                    return await handler(event, data)

                bot = data.get('bot')
                if bot and chat_id:
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text="⏳ Ваша сессия истекла. Используйте /start",
                            reply_markup=get_main_menu_keyboard()
                        )
                    except:
                        pass

                if hasattr(event, 'callback_query'):
                    try:
                        await event.callback_query.answer("Сессия истекла", show_alert=True)
                    except:
                        pass
                return False

            session_timeout.update(user_id)
    except Exception as e:
        print(f"[TIMEOUT] Ошибка: {e}")
    return await handler(event, data)

# ========== ОБРАБОТЧИКИ ==========
async def start_command(message: types.Message, state: FSMContext):
    user = message.from_user
    if not rate_limiter.is_allowed(user.id):
        return await message.answer("⏳ Слишком много запросов")

    await state.clear()
    session_timeout.update(user.id)

    if Config.MODE in ["GOOGLE", "HYBRID"]:
        storage.clear_cache()

    text = (f"🎯 *Донорская станция v5.0*\n"
            f"👋 Привет, {user.first_name or 'пользователь'}!\n\n"
            f"Я помогу вам записаться на донорство крови.\n"
            f"*Выберите действие:*")

    await message.answer(text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())

async def process_main_menu(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    session_timeout.update(user.id)

    if callback.data == CallbackData.MAIN_RECORD:
        await callback.message.edit_text(
            "🩸 *Выберите вашу группу крови:*",
            parse_mode="Markdown",
            reply_markup=get_blood_group_keyboard()
        )
        await state.set_state(Form.waiting_for_blood_group)
        await state.update_data(is_check=False)

    elif callback.data == CallbackData.MAIN_CHECK:
        await callback.message.edit_text(
            "🔍 *Проверка времени*\nВыберите группу крови:",
            parse_mode="Markdown",
            reply_markup=get_blood_group_keyboard()
        )
        await state.set_state(Form.waiting_for_blood_group)
        await state.update_data(is_check=True)

    elif callback.data == CallbackData.MAIN_MYBOOKINGS:
        await show_my_bookings(callback.message, user)

    elif callback.data == CallbackData.MAIN_STATS:
        await show_stats(callback.message)

    elif callback.data == CallbackData.MAIN_HELP:
        await help_command(callback.message)

    await callback.answer()

async def process_blood_group(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    session_timeout.update(user.id)

    # Обработка системных кнопок
    if callback.data == CallbackData.CANCEL:
        await cancel_command(callback.message, state)
        await callback.answer()
        return

    if callback.data == CallbackData.MAIN_MENU:
        await show_main_menu(callback.message)
        await state.clear()
        await callback.answer()
        return

    if callback.data == CallbackData.BACK_TO_BLOOD:
        await callback.answer()
        return

    if not CallbackData.is_blood(callback.data):
        await callback.answer("Пожалуйста, выберите группу крови", show_alert=True)
        return

    blood = callback.data[len(CallbackData.BLOOD_PREFIX):]
    await state.update_data(blood_group=blood)

    data = await state.get_data()
    is_check = data.get('is_check', False)

    resp = await storage.get_available_dates(user.id)

    if resp.status == 'error':
        await callback.message.edit_text(
            f"❌ Ошибка: {resp.data}",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return

    dates = resp.data.get('available_dates', [])
    if not dates:
        await callback.message.edit_text(
            "😔 Нет доступных дат",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return

    action = "проверки" if is_check else "записи"
    text = f"📅 *Выберите дату для {action}:*\n🩸 Группа: {blood}"

    await callback.message.edit_text(
        text, parse_mode="Markdown", reply_markup=get_dates_keyboard(dates)
    )
    await state.set_state(Form.waiting_for_date)
    await callback.answer()

async def process_date(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    session_timeout.update(user.id)

    if callback.data == CallbackData.CANCEL:
        await cancel_command(callback.message, state)
        await callback.answer()
        return

    if callback.data == CallbackData.BACK_TO_BLOOD:
        await callback.message.edit_text(
            "🩸 Выберите группу крови:",
            reply_markup=get_blood_group_keyboard()
        )
        await state.set_state(Form.waiting_for_blood_group)
        await callback.answer()
        return

    if not CallbackData.is_date(callback.data):
        await callback.answer("Выберите дату", show_alert=True)
        return

    date = callback.data[len(CallbackData.DATE_PREFIX):]
    data = await state.get_data()
    blood = data.get('blood_group')

    if not blood:
        await callback.message.edit_text("❌ Ошибка", reply_markup=get_main_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    await state.update_data(selected_date=date)

    try:
        d_obj = datetime.strptime(date, "%Y-%m-%d")
        display = d_obj.strftime("%d.%m.%Y")
        day = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"][d_obj.weekday()]
    except:
        display, day = date, "?"

    resp = await storage.get_free_times(date, blood)

    if resp.status == 'error':
        await callback.message.edit_text(f"❌ {resp.data}", reply_markup=get_main_menu_keyboard())
        await callback.answer()
        return

    times = resp.data.get('times', [])
    quota = resp.data.get('quota', 0)
    is_check = data.get('is_check', False)

    if not times:
        if is_check:
            await callback.message.edit_text(
                f"📅 На {display} ({day}) для {blood} все заняты\n📊 Осталось: {quota}",
                reply_markup=get_main_menu_keyboard()
            )
            await state.clear()
        else:
            dates_resp = await storage.get_available_dates(user.id)
            dates = dates_resp.data.get('available_dates', []) if dates_resp.status == 'success' else []
            await callback.message.edit_text(
                f"❌ На {display} все заняты\nВыберите другую дату:",
                reply_markup=get_dates_keyboard(dates)
            )
        await callback.answer()
        return

    if is_check:
        text = f"📅 *Доступное время на {display}:*\n📋 {day}\n🩸 {blood}\n📊 Свободно {len(times)} из {quota}\n\n"
        text += "\n".join(f"• {t}" for t in times)
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())
        await state.clear()
    else:
        await callback.message.edit_text(
            f"✅ *Доступное время на {display}:*\n📊 Свободно {quota} мест\n\nВыберите время:",
            parse_mode="Markdown", reply_markup=get_times_keyboard(times)
        )
        await state.set_state(Form.waiting_for_time)

    await callback.answer()

async def process_time(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    session_timeout.update(user.id)

    if callback.data == CallbackData.CANCEL:
        await cancel_command(callback.message, state)
        await callback.answer()
        return

    if callback.data == CallbackData.BACK_TO_DATE:
        data = await state.get_data()
        blood = data.get('blood_group')
        resp = await storage.get_available_dates(user.id)
        dates = resp.data.get('available_dates', []) if resp.status == 'success' else []
        await callback.message.edit_text(
            f"📅 Выберите дату:\n🩸 {blood}",
            reply_markup=get_dates_keyboard(dates)
        )
        await state.set_state(Form.waiting_for_date)
        await callback.answer()
        return

    if not CallbackData.is_time(callback.data):
        await callback.answer("Выберите время", show_alert=True)
        return

    time = callback.data[len(CallbackData.TIME_PREFIX):]
    data = await state.get_data()
    date = data.get('selected_date')
    blood = data.get('blood_group')

    if not date or not blood:
        await callback.message.edit_text("❌ Ошибка", reply_markup=get_main_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    try:
        display = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
    except:
        display = date

    check = await storage.check_existing(date, user.id)
    if check.status == 'success' and check.data.get('exists'):
        await callback.message.edit_text(
            f"⚠️ У вас уже есть запись на {display}!",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        await callback.answer()
        return

    resp = await storage.register(date, blood, time, user.id)

    if resp.status == 'error':
        times_resp = await storage.get_free_times(date, blood)
        times = times_resp.data.get('times', []) if times_resp.status == 'success' else []
        await callback.message.edit_text(
            f"❌ {resp.data}\nВыберите другое время:",
            reply_markup=get_times_keyboard(times)
        )
        await callback.answer()
        return

    ticket_data = resp.data

    # Обновляем квоту после регистрации (как в рабочей версии)
    updated_times = await storage.get_free_times(date, blood)
    if updated_times.status == 'success':
        ticket_data['quota_remaining'] = updated_times.data.get('quota', 0)

    text = (f"🎫 *ВАШ ТАЛОН*\n"
            f"• Номер: *{ticket_data.get('ticket', '?')}*\n"
            f"• Дата: *{display}*\n"
            f"• Время: *{ticket_data.get('time', '?')}*\n"
            f"• Группа: *{ticket_data.get('blood_group', '?')}*\n"
            f"📊 Осталось: *{ticket_data.get('quota_remaining', 0)}*")

    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())
    await state.clear()
    await callback.answer("✅ Запись оформлена!")

async def show_my_bookings(message: types.Message, user: types.User):
    resp = await storage.get_user_bookings(user.id)

    if resp.status == 'error':
        return await message.answer(f"❌ {resp.data}", reply_markup=get_main_menu_keyboard())

    bookings = resp.data.get('bookings', [])
    if not bookings:
        return await message.answer(
            f"📋 *Ваши записи*\n\nУ вас нет записей.",
            parse_mode="Markdown", reply_markup=get_main_menu_keyboard()
        )

    builder = InlineKeyboardBuilder()
    text = f"📋 *Ваши записи*\n\n"
    for b in bookings:
        try:
            d = datetime.strptime(b['date'], "%Y-%m-%d").strftime("%d.%m.%Y")
        except:
            d = b['date']
        text += f"• *{d}*: {b['time']} ({b['blood_group']})\n"
        builder.row(InlineKeyboardButton(
            text=f"❌ Отменить {d}",
            callback_data=f"{CallbackData.CANCEL_ASK_PREFIX}{b['date']}_{b['ticket']}"
        ))
    builder.row(InlineKeyboardButton(text="🔙 В главное меню", callback_data=CallbackData.MAIN_MENU))

    await message.answer(text, parse_mode="Markdown", reply_markup=builder.as_markup())

async def show_stats(message: types.Message):
    resp = await storage.get_stats()

    if resp.status == 'error':
        return await message.answer(f"❌ {resp.data}", reply_markup=get_main_menu_keyboard())

    d = resp.data
    text = (f"📊 *Статистика*\n\n"
            f"👥 Пользователей: {d.get('total_users', 0)}\n"
            f"📝 Записей: {d.get('total_bookings', 0)}\n"
            f"📈 Популярный день: {d.get('most_popular_day', 'нет')}\n"
            f"🩸 Популярная группа: {d.get('most_popular_blood_group', 'нет')}")

    await message.answer(text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())

async def help_command(message: types.Message):
    text = ("📋 *Помощь*\n\n"
            "• 📋 Записаться на донорство\n"
            "• 🔍 Проверить свободное время\n"
            "• 📖 Мои записи\n"
            "• 📊 Статистика\n\n"
            "📌 Одна запись в день")
    await message.answer(text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())

async def cancel_command(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("✅ Диалог отменен", reply_markup=get_main_menu_keyboard())

async def show_main_menu(message: types.Message):
    await message.answer("🎯 *Главное меню*", parse_mode="Markdown", reply_markup=get_main_menu_keyboard())

async def process_cancel_booking(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    session_timeout.update(user.id)

    if callback.data == CallbackData.CANCEL_NO:
        await callback.message.edit_text("✅ Отмена отменена", reply_markup=get_main_menu_keyboard())
        await callback.answer()
        return

    if callback.data.startswith(CallbackData.CANCEL_YES_PREFIX):
        parts = callback.data.split("_")
        if len(parts) >= 4:
            date = parts[2]
            ticket = "_".join(parts[3:])
            resp = await storage.cancel_booking(date, ticket, user.id)
            if resp.status == 'success':
                await callback.message.edit_text("✅ Запись отменена", reply_markup=get_main_menu_keyboard())
            else:
                await callback.message.edit_text(f"❌ {resp.data}", reply_markup=get_main_menu_keyboard())
        await callback.answer()
        return

    if callback.data.startswith(CallbackData.CANCEL_ASK_PREFIX):
        parts = callback.data.split("_")
        if len(parts) >= 4:
            date = parts[2]
            ticket = "_".join(parts[3:])
            try:
                d = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
            except:
                d = date
            await callback.message.edit_text(
                f"⚠️ Отменить запись на {d}?",
                reply_markup=get_confirm_cancellation_keyboard(date, ticket)
            )
        await callback.answer()
        return

# ========== КОМАНДЫ ==========
async def mybookings_command(message: types.Message, state: FSMContext):
    user = message.from_user
    await show_my_bookings(message, user)

async def stats_command(message: types.Message, state: FSMContext):
    await show_stats(message)

async def reset_command(message: types.Message, state: FSMContext):
    if message.from_user.id not in Config.ADMIN_IDS:
        await message.answer("⛔ Нет прав")
        return
    storage.clear_cache()
    await message.answer("✅ Кэш очищен", reply_markup=get_main_menu_keyboard())

async def clear_cache_command(message: types.Message, state: FSMContext):
    if message.from_user.id not in Config.ADMIN_IDS:
        await message.answer("⛔ Нет прав")
        return
    storage.clear_cache()
    await message.answer("✅ Кэш очищен", reply_markup=get_main_menu_keyboard())

async def refresh_cache_command(message: types.Message, state: FSMContext):
    if message.from_user.id not in Config.ADMIN_IDS:
        await message.answer("⛔ Нет прав")
        return
    # Принудительное обновление при следующем запросе get_available_dates
    await message.answer("🔄 Кэш будет обновлён при следующем запросе", reply_markup=get_main_menu_keyboard())

# ========== ЗАПУСК ==========
async def main():
    logging.basicConfig(level=logging.INFO)

    print("=" * 50)
    print("🚀 ЗАПУСК БОТА v5.0")
    print("=" * 50)

    if Config.MODE in ["GOOGLE", "HYBRID"]:
        test = google_client.test_connection()
        if test.status == "success":
            print("✅ Google Script доступен")
        else:
            print(f"⚠️ Google Script недоступен: {test.data}")
            if Config.MODE == "GOOGLE":
                print("❌ Режим GOOGLE выбран, но сервис недоступен!")
                return

    context = ssl.create_default_context()
    connector = aiohttp.TCPConnector(ssl=context)
    aiohttp_session = aiohttp.ClientSession(connector=connector)
    session = AiohttpSession()
    session._session = aiohttp_session

    bot = Bot(token=Config.TOKEN, session=session)
    dp = Dispatcher(storage=MemoryStorage())

    # Middleware
    dp.update.middleware(timeout_middleware)

    # Команды
    dp.message.register(start_command, Command("start"))
    dp.message.register(cancel_command, Command("cancel"))
    dp.message.register(help_command, Command("help"))
    dp.message.register(mybookings_command, Command("mybookings"))
    dp.message.register(stats_command, Command("stats"))
    dp.message.register(reset_command, Command("reset"))
    dp.message.register(clear_cache_command, Command("clearcache"))
    dp.message.register(refresh_cache_command, Command("refresh"))

    # Callback-обработчики
    dp.callback_query.register(process_main_menu, F.data.in_([
        CallbackData.MAIN_RECORD, CallbackData.MAIN_CHECK,
        CallbackData.MAIN_MYBOOKINGS, CallbackData.MAIN_STATS, CallbackData.MAIN_HELP
    ]))
    dp.callback_query.register(process_blood_group, Form.waiting_for_blood_group)
    dp.callback_query.register(process_date, Form.waiting_for_date)
    dp.callback_query.register(process_time, Form.waiting_for_time)
    dp.callback_query.register(process_cancel_booking)
    dp.callback_query.register(show_main_menu, F.data == CallbackData.MAIN_MENU)

    print("✅ Бот готов")
    print("=" * 50)

    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        print("\n⚠️ Бот остановлен")
    finally:
        await aiohttp_session.close()
        print("✅ Сессии закрыты")

if __name__ == "__main__":
    asyncio.run(main())
