#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ЛОР-Помощник - Telegram бот для управления приемом лекарств и отслеживания симптомов
Версия: 5.0.2 (Финальная стабильная)
Автор: Денис Казарин (врач-оториноларинголог)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from time import time
import pytz
import json
import re
import warnings
import signal
from sqlalchemy.exc import MovedIn20Warning, OperationalError
from telegram.warnings import PTBUserWarning
from sqlalchemy import text

# Отключаем предупреждения
warnings.filterwarnings('ignore', category=MovedIn20Warning)
warnings.filterwarnings('ignore', category=PTBUserWarning)

# ============== ОПТИМИЗАЦИЯ EVENT LOOP ==============
# Устанавливаем uvloop для лучшей производительности (опционально)
try:
    import uvloop
    uvloop.install()
    print("✅ uvloop установлен и активен")
except ImportError:
    print("⚠️ uvloop не установлен, используем стандартный asyncio")

# Применяем nest_asyncio для работы в уже запущенном цикле
try:
    import nest_asyncio
    nest_asyncio.apply()
    print("✅ nest_asyncio применен")
except ImportError:
    print("⚠️ nest_asyncio не установлен, устанавливаем...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "nest_asyncio"])
    import nest_asyncio
    nest_asyncio.apply()
    print("✅ nest_asyncio установлен и применен")

# ============== УСТАНОВКА ЗАВИСИМОСТЕЙ ==============
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler,
        ConversationHandler, MessageHandler, filters, ContextTypes
    )
    from telegram.constants import ParseMode
    from telegram.error import RetryAfter, TimedOut, BadRequest, Conflict
except ImportError:
    print("Устанавливаем python-telegram-bot...")
    os.system(f"{sys.executable} -m pip install python-telegram-bot==20.3")
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler,
        ConversationHandler, MessageHandler, filters, ContextTypes
    )
    from telegram.constants import ParseMode
    from telegram.error import RetryAfter, TimedOut, BadRequest, Conflict

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
    from apscheduler.executors.asyncio import AsyncIOExecutor
    from apscheduler.jobstores.base import JobLookupError
    from apscheduler.triggers.date import DateTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.cron import CronTrigger
except ImportError:
    print("Устанавливаем APScheduler...")
    os.system(f"{sys.executable} -m pip install apscheduler==3.10.4")
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
    from apscheduler.executors.asyncio import AsyncIOExecutor
    from apscheduler.jobstores.base import JobLookupError
    from apscheduler.triggers.date import DateTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.cron import CronTrigger

try:
    from sqlalchemy import (
        create_engine, Column, Integer, String, DateTime, Text, 
        Boolean, BigInteger, Index, func, select, and_, or_
    )
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, scoped_session
    from sqlalchemy.pool import QueuePool
except ImportError:
    print("Устанавливаем SQLAlchemy...")
    os.system(f"{sys.executable} -m pip install sqlalchemy==2.0.23")
    from sqlalchemy import (
        create_engine, Column, Integer, String, DateTime, Text, 
        Boolean, BigInteger, Index, func, select, and_, or_
    )
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, scoped_session
    from sqlalchemy.pool import QueuePool

# ============== КОНФИГУРАЦИЯ ==============
BOT_TOKEN = os.environ.get("BOT_TOKEN", "ВАШ_ТОКЕН_ЗДЕСЬ")
DATABASE_URL = "sqlite:///lor_reminder.db"
JOB_STORE_URL = "sqlite:///apscheduler_jobs.db"

# Контакты клиник
KIT_CLINIC = {
    "name": "🏥 КИТ-клиника (Куркино)",
    "address": "125466, Москва, ул. Соколово-Мещерская, 16/114",
    "phone": "84957775580",
    "phone_display": "8 (495) 777-55-80",
    "site": "https://kit-clinic.ru/doctors/kazarin-denis-sergeevich/",
    "maps": "https://yandex.ru/maps/-/CPQZIPYD",
    "coords": "55.897085, 37.389648"
}

FAMILY_CLINIC = {
    "name": "🏥 Семейная клиника (Путилково)",
    "address": "Красногорск г.о., пгт Путилково, Спасо-Тушинский бульвар, д. 5",
    "phone": "84987317555",
    "phone_display": "8 (498) 731-75-55",
    "site": "https://klinika-bz.ru/speczialistyi/kazarin-denis-sergeevich",
    "maps": "https://yandex.ru/maps/-/CPEBA46u"
}

# Информация о враче (ОБНОВЛЕНО: добавлена информация о приеме детей)
DOCTOR_INFO = """👨‍⚕️ *Денис Сергеевич Казарин* - врач-оториноларинголог

👶 *Ведет прием детей с 0 лет и взрослых*

🎓 *Образование:*
• 2001-2007: МГМСУ им. А.И. Евдокимова (Лечебное дело)
• 2007-2009: Ординатура, РМАПО (Оториноларингология)
• Доп. образование: Лазерная медицина (НПЦ лазерной медицины им. Скобелкина)

🏥 *Принимает в клиниках:*
• КИТ-клиника (Куркино)
• Семейная клиника (Путилково)"""

# ============== НАСТРОЙКА ЛОГГЕРА ==============
def setup_logging():
    """Настройка логгера для напоминаний."""
    logger = logging.getLogger('reminders')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        try:
            file_handler = logging.FileHandler('reminders.log')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except:
            pass
    
    return logger

reminder_logger = setup_logging()

# ============== МОДЕЛИ БАЗЫ ДАННЫХ ==============
Base = declarative_base()

class UserTimezone(Base):
    __tablename__ = 'user_timezones'
    user_id = Column(BigInteger, primary_key=True)
    timezone = Column(String(50), nullable=False, default='Europe/Moscow')
    created_at = Column(DateTime, default=datetime.utcnow)

class Medicine(Base):
    __tablename__ = 'medicines'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    schedule = Column(String(200), nullable=False)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    user_timezone = Column(String(50), nullable=False)
    status = Column(String(20), default='active')
    course_type = Column(String(20), default='unlimited')
    course_days = Column(Integer, nullable=True)
    repeat_type = Column(String(20), default='none')
    repeat_days = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_medicines_user_status', 'user_id', 'status'),
    )

class Analysis(Base):
    __tablename__ = 'analyses'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    scheduled_date = Column(DateTime, nullable=False)
    scheduled_time = Column(String(10), nullable=False, default='12:00')
    repeat_type = Column(String(20), default='once')
    repeat_interval = Column(Integer, nullable=True)
    reminder_before = Column(Integer, default=24)
    notes = Column(Text, nullable=True)
    status = Column(String(20), default='pending')
    user_timezone = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_analyses_user_status', 'user_id', 'status'),
        Index('ix_analyses_scheduled_date', 'scheduled_date'),
    )

class Reminder(Base):
    __tablename__ = 'reminders'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    reminder_type = Column(String(20))
    item_id = Column(Integer, nullable=False)
    scheduled_time = Column(DateTime(timezone=True), nullable=False)
    user_timezone = Column(String(50), nullable=False)
    status = Column(String(20), default='pending')
    retry_count = Column(Integer, default=0)
    last_error = Column(Text, nullable=True)
    postponed_until = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_reminders_status_time', 'status', 'scheduled_time'),
    )

class MedicineLog(Base):
    __tablename__ = 'medicine_logs'
    id = Column(Integer, primary_key=True)
    medicine_id = Column(Integer, nullable=False, index=True)
    user_id = Column(BigInteger, nullable=False)
    status = Column(String(20))
    taken_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.UTC))
    error_details = Column(Text, nullable=True)

class AnalysisLog(Base):
    __tablename__ = 'analysis_logs'
    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, nullable=False, index=True)
    user_id = Column(BigInteger, nullable=False)
    status = Column(String(20))
    completed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.UTC))
    notes = Column(Text, nullable=True)

class MoodLog(Base):
    __tablename__ = 'mood_logs'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    mood_score = Column(Integer, nullable=False)
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.UTC))

class SymptomLog(Base):
    __tablename__ = 'symptom_logs'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    symptom = Column(String(100), nullable=False)
    severity = Column(Integer, nullable=False)
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.UTC))

# ============== СОЕДИНЕНИЕ С БД ==============
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    """Инициализация базы данных с проверкой существующих колонок."""
    Base.metadata.create_all(bind=engine)
    
    db = SessionLocal()
    try:
        from sqlalchemy import inspect
        inspector = inspect(engine)
        
        # Проверяем таблицу analyses
        try:
            columns = [col['name'] for col in inspector.get_columns('analyses')]
            
            if 'repeat_interval' not in columns:
                reminder_logger.info("Добавляем колонку repeat_interval в таблицу analyses")
                db.execute(text('ALTER TABLE analyses ADD COLUMN repeat_interval INTEGER'))
                db.commit()
            
            if 'reminder_before' not in columns:
                reminder_logger.info("Добавляем колонку reminder_before в таблицу analyses")
                db.execute(text('ALTER TABLE analyses ADD COLUMN reminder_before INTEGER DEFAULT 24'))
                db.commit()
                
            if 'scheduled_time' not in columns:
                reminder_logger.info("Добавляем колонку scheduled_time в таблицу analyses")
                db.execute(text('ALTER TABLE analyses ADD COLUMN scheduled_time VARCHAR(10) DEFAULT "12:00"'))
                db.commit()
        except:
            pass
            
        # Проверяем таблицу medicines
        try:
            med_columns = [col['name'] for col in inspector.get_columns('medicines')]
            if 'course_days' not in med_columns:
                reminder_logger.info("Добавляем колонку course_days в таблицу medicines")
                db.execute(text('ALTER TABLE medicines ADD COLUMN course_days INTEGER'))
                db.commit()
        except:
            pass
            
    except Exception as e:
        reminder_logger.error(f"Ошибка при инициализации БД: {e}")
        db.rollback()
    finally:
        db.close()

init_db()

def get_db():
    """Получение сессии БД."""
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()

# ============== RATE LIMITER ==============
class RateLimiter:
    """Rate limiting для защиты от бана Telegram."""
    
    def __init__(self, global_rate: int = 30, per_user_rate: int = 1):
        self.global_semaphore = asyncio.Semaphore(global_rate)
        self.per_user_rate = per_user_rate
        self.user_last_message = defaultdict(float)
    
    async def acquire(self, user_id: Optional[int] = None):
        """Acquire rate limit permit."""
        await self.global_semaphore.acquire()
        
        if user_id:
            now = time()
            last_msg = self.user_last_message[user_id]
            if now - last_msg < self.per_user_rate:
                wait_time = self.per_user_rate - (now - last_msg)
                await asyncio.sleep(wait_time)
            self.user_last_message[user_id] = now

# ============== ПЛАНИРОВЩИК ==============
class PersistentScheduler:
    """Планировщик с persistent storage."""
    
    def __init__(self):
        jobstores = {
            'default': SQLAlchemyJobStore(url=JOB_STORE_URL)
        }
        executors = {
            'default': AsyncIOExecutor()
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 3,
            'misfire_grace_time': 3600
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=pytz.UTC
        )
    
    def start(self):
        """Запуск планировщика."""
        self.scheduler.start()
        reminder_logger.info("SCHEDULER - Планировщик запущен")
    
    def shutdown(self):
        """Остановка планировщика."""
        self.scheduler.shutdown()
        reminder_logger.info("SCHEDULER - Планировщик остановлен")
    
    async def restore_reminders(self):
        """Восстановление напоминаний при старте."""
        db = get_db()
        try:
            now_utc = datetime.now(pytz.UTC)
            pending = db.query(Reminder).filter(
                Reminder.status == 'pending',
                Reminder.scheduled_time > now_utc
            ).all()
            
            restored_count = 0
            for reminder in pending:
                job_id = f"{reminder.reminder_type}_{reminder.id}"
                
                try:
                    self.scheduler.remove_job(job_id)
                except JobLookupError:
                    pass
                
                self.scheduler.add_job(
                    send_reminder_job,
                    trigger=DateTrigger(run_date=reminder.scheduled_time),
                    id=job_id,
                    args=[reminder.id],
                    replace_existing=True
                )
                restored_count += 1
            
            reminder_logger.info(f"RESTORE - Восстановлено {restored_count} напоминаний")
            return restored_count
        
        finally:
            db.close()

# ============== СОЗДАНИЕ ГЛОБАЛЬНЫХ ОБЪЕКТОВ ==============
scheduler = PersistentScheduler()
rate_limiter = RateLimiter()

# ============== СОСТОЯНИЯ ДЛЯ CONVERSATION HANDLER ==============
(
    MEDICINE_NAME, MEDICINE_TIME, MEDICINE_COURSE_TYPE, MEDICINE_COURSE_DAYS,
    MEDICINE_REPEAT, MEDICINE_START_DATE, MEDICINE_CONFIRM,
    ANALYSIS_NAME, ANALYSIS_DATE, ANALYSIS_TIME, ANALYSIS_REPEAT, 
    ANALYSIS_REMINDER, ANALYSIS_NOTES, ANALYSIS_CONFIRM,
    SYMPTOM_TEXT, SYMPTOM_SEVERITY
) = range(16)

# ============== ФУНКЦИИ ДЛЯ РАБОТЫ С ЧАСОВЫМИ ПОЯСАМИ ==============
def get_user_timezone(user_id: int) -> str:
    """Получение часового пояса пользователя."""
    db = get_db()
    try:
        user_tz = db.query(UserTimezone).filter_by(user_id=user_id).first()
        return user_tz.timezone if user_tz else 'Europe/Moscow'
    finally:
        db.close()

def set_user_timezone(user_id: int, timezone: str):
    """Установка часового пояса пользователя."""
    db = get_db()
    try:
        user_tz = db.query(UserTimezone).filter_by(user_id=user_id).first()
        if user_tz:
            user_tz.timezone = timezone
        else:
            user_tz = UserTimezone(user_id=user_id, timezone=timezone)
            db.add(user_tz)
        db.commit()
    finally:
        db.close()

def local_to_utc(local_time_str: str, user_timezone: str, base_date: Optional[datetime] = None) -> datetime:
    """Конвертация локального времени в UTC."""
    if base_date is None:
        base_date = datetime.now(pytz.timezone(user_timezone))
    
    hour, minute = map(int, local_time_str.split(':'))
    local_dt = base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    if not local_dt.tzinfo:
        tz = pytz.timezone(user_timezone)
        local_dt = tz.localize(local_dt)
    
    return local_dt.astimezone(pytz.UTC)

def utc_to_local(utc_dt: datetime, user_timezone: str) -> datetime:
    """Конвертация UTC в локальное время."""
    if utc_dt.tzinfo is None:
        utc_dt = pytz.UTC.localize(utc_dt)
    tz = pytz.timezone(user_timezone)
    return utc_dt.astimezone(tz)

def parse_date(date_str: str, user_timezone: str) -> Optional[datetime]:
    """Парсинг даты из строки."""
    try:
        formats = [
            '%d.%m.%Y', '%d/%m/%Y', '%d-%m-%Y',
            '%d.%m.%y', '%d/%m/%y', '%d-%m-%y',
            '%Y-%m-%d', '%Y/%m/%d'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                dt = dt.replace(hour=12, minute=0, second=0)
                tz = pytz.timezone(user_timezone)
                return tz.localize(dt)
            except ValueError:
                continue
        
        return None
    except:
        return None

def check_existing_analysis(user_id: int, scheduled_date: datetime, scheduled_time: str) -> bool:
    """Проверка существующего анализа на указанную дату и время."""
    db = get_db()
    try:
        if scheduled_date.tzinfo is None:
            scheduled_date = pytz.UTC.localize(scheduled_date)
        
        existing = db.query(Analysis).filter(
            Analysis.user_id == user_id,
            Analysis.status == 'pending',
            func.date(Analysis.scheduled_date) == func.date(scheduled_date),
            Analysis.scheduled_time == scheduled_time
        ).first()
        
        return existing is not None
    finally:
        db.close()

# ============== ФУНКЦИИ ДЛЯ КНОПОК НАЗАД/ГЛАВНАЯ ==============
def add_navigation_buttons(keyboard):
    """Добавляет кнопки навигации к клавиатуре."""
    if keyboard and isinstance(keyboard, list):
        has_nav = False
        for row in keyboard:
            for btn in row:
                if btn.callback_data in ["start", "back"]:
                    has_nav = True
                    break
        
        if not has_nav:
            keyboard.append([
                InlineKeyboardButton("🔙 Назад", callback_data="back"),
                InlineKeyboardButton("🏠 Главная", callback_data="start")
            ])
    return keyboard

# ============== КЛАВИАТУРЫ ==============
def get_start_keyboard():
    """Клавиатура для /start."""
    keyboard = [
        [
            InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine"),
            InlineKeyboardButton("🩺 Добавить анализ/исследование", callback_data="add_analysis"),
        ],
        [
            InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines"),
            InlineKeyboardButton("📋 Список анализов/исследований", callback_data="list_analyses"),
        ],
        [
            InlineKeyboardButton("📊 Самочувствие", callback_data="mood"),
            InlineKeyboardButton("📈 Статистика", callback_data="stats"),
        ],
        [
            InlineKeyboardButton("🏥 КИТ-клиника", url=KIT_CLINIC['site']),
            InlineKeyboardButton("🏥 Семейная клиника", url=FAMILY_CLINIC['site']),
        ],
        [
            InlineKeyboardButton("🗺️ Карты Куркино", url=KIT_CLINIC['maps']),
            InlineKeyboardButton("🗺️ Карты Путилково", url=FAMILY_CLINIC['maps']),
        ],
        [
            InlineKeyboardButton("👨‍⚕️ О враче", callback_data="about"),
            InlineKeyboardButton("❓ Помощь", callback_data="help"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_back_keyboard():
    """Клавиатура с кнопками назад и главная."""
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="back")],
        [InlineKeyboardButton("🏠 Главная", callback_data="start")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_navigation_keyboard(back_callback="start"):
    """Клавиатура с навигацией."""
    keyboard = [
        [
            InlineKeyboardButton("🔙 Назад", callback_data=back_callback),
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_about_keyboard():
    """Клавиатура для /about (ОБНОВЛЕНО: добавлены Telegram-ссылки)."""
    keyboard = [
        [
            InlineKeyboardButton("👨‍⚕️ Личный Telegram", url="https://t.me/DENISKAZARIN"),
            InlineKeyboardButton("👥 Группа ЛОР", url="https://t.me/KAZARIN_LOR"),
        ],
        [
            InlineKeyboardButton("🏥 КИТ-клиника", url=KIT_CLINIC['site']),
            InlineKeyboardButton("📞 Позвонить", callback_data="phone_kit"),
            InlineKeyboardButton("🗺️ Карты", url=KIT_CLINIC['maps']),
        ],
        [
            InlineKeyboardButton("🏥 Семейная", url=FAMILY_CLINIC['site']),
            InlineKeyboardButton("📞 Позвонить", callback_data="phone_family"),
            InlineKeyboardButton("🗺️ Карты", url=FAMILY_CLINIC['maps']),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="start"),
            InlineKeyboardButton("🏠 Главная", callback_data="start"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_help_keyboard():
    """Клавиатура для /help - упрощенная."""
    keyboard = [
        [
            InlineKeyboardButton("❓ Как очистить историю", callback_data="help_clear"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="start"),
            InlineKeyboardButton("🏠 Главная", callback_data="start"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_medicine_inline_keyboard(medicine_id: int):
    """Клавиатура для напоминания о лекарстве."""
    keyboard = [
        [
            InlineKeyboardButton("✅ Принял(а)", callback_data=f"take_{medicine_id}"),
            InlineKeyboardButton("⏸ Отложить", callback_data=f"postpone_{medicine_id}"),
        ],
        [
            InlineKeyboardButton("❌ Пропустил(а)", callback_data=f"skip_{medicine_id}"),
            InlineKeyboardButton("⏸ Пауза курса", callback_data=f"pause_{medicine_id}"),
        ],
        [
            InlineKeyboardButton("🔙 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_analysis_inline_keyboard(analysis_id: int):
    """Клавиатура для напоминания об анализе/исследовании."""
    keyboard = [
        [
            InlineKeyboardButton("✅ Сдал(а)", callback_data=f"analysis_take_{analysis_id}"),
            InlineKeyboardButton("⏸ Отложить", callback_data=f"analysis_postpone_{analysis_id}"),
        ],
        [
            InlineKeyboardButton("❌ Пропустил(а)", callback_data=f"analysis_skip_{analysis_id}"),
            InlineKeyboardButton("📝 Заметки", callback_data=f"analysis_notes_{analysis_id}"),
        ],
        [
            InlineKeyboardButton("🔙 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_mood_keyboard():
    """Клавиатура для оценки самочувствия - горизонтальное расположение."""
    keyboard = [
        [
            InlineKeyboardButton("1 😢", callback_data="mood_1"),
            InlineKeyboardButton("2 🙁", callback_data="mood_2"),
            InlineKeyboardButton("3 😐", callback_data="mood_3"),
            InlineKeyboardButton("4 🙂", callback_data="mood_4"),
            InlineKeyboardButton("5 😊", callback_data="mood_5"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="start"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_symptom_severity_keyboard():
    """Клавиатура для оценки тяжести симптома с возрастанием от 1 до 5."""
    keyboard = [
        [
            InlineKeyboardButton("1 🔴 Минимальная", callback_data="severity_1"),
            InlineKeyboardButton("2 🟠 Легкая", callback_data="severity_2"),
        ],
        [
            InlineKeyboardButton("3 🟡 Умеренная", callback_data="severity_3"),
            InlineKeyboardButton("4 🟢 Сильная", callback_data="severity_4"),
        ],
        [
            InlineKeyboardButton("5 🔵 Максимальная", callback_data="severity_5"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="mood"),
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_timezone_keyboard():
    """Клавиатура для выбора часового пояса."""
    keyboard = [
        [
            InlineKeyboardButton("Москва (UTC+3)", callback_data="tz_Europe/Moscow"),
            InlineKeyboardButton("СПб (UTC+3)", callback_data="tz_Europe/Moscow"),
        ],
        [
            InlineKeyboardButton("Калининград (UTC+2)", callback_data="tz_Europe/Kaliningrad"),
            InlineKeyboardButton("Самара (UTC+4)", callback_data="tz_Europe/Samara"),
        ],
        [
            InlineKeyboardButton("Екатеринбург (UTC+5)", callback_data="tz_Asia/Yekaterinburg"),
            InlineKeyboardButton("Омск (UTC+6)", callback_data="tz_Asia/Omsk"),
        ],
        [
            InlineKeyboardButton("Красноярск (UTC+7)", callback_data="tz_Asia/Krasnoyarsk"),
            InlineKeyboardButton("Иркутск (UTC+8)", callback_data="tz_Asia/Irkutsk"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="help"),
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_analysis_date_keyboard():
    """Клавиатура для выбора даты анализа/исследования."""
    today = datetime.now()
    dates = []
    
    for i in range(7):
        date = today + timedelta(days=i)
        date_str = date.strftime('%d.%m.%Y')
        day_name = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][date.weekday()]
        dates.append(InlineKeyboardButton(
            f"{date_str} ({day_name})", 
            callback_data=f"analysis_date_{date_str}"
        ))
    
    keyboard = []
    for i in range(0, len(dates), 2):
        keyboard.append(dates[i:i+2])
    
    keyboard.append([InlineKeyboardButton("📅 Своя дата", callback_data="analysis_date_custom")])
    keyboard.append([
        InlineKeyboardButton("🔙 Назад", callback_data="add_analysis"),
        InlineKeyboardButton("🏠 Главная", callback_data="start")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def get_analysis_time_keyboard():
    """Клавиатура для выбора времени анализа/исследования с минутами от 0 до 9."""
    keyboard = []
    
    hours = list(range(8, 22))
    hour_buttons = []
    for h in hours:
        hour_buttons.append(InlineKeyboardButton(f"{h:02d}:00", callback_data=f"time_{h:02d}:00"))
        if len(hour_buttons) == 4:
            keyboard.append(hour_buttons)
            hour_buttons = []
    if hour_buttons:
        keyboard.append(hour_buttons)
    
    minute_row = []
    for m in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
        minute_row.append(InlineKeyboardButton(f"⚙️ :{m:02d}", callback_data=f"time_minute_{m:02d}"))
        if len(minute_row) == 4:
            keyboard.append(minute_row)
            minute_row = []
    if minute_row:
        keyboard.append(minute_row)
    
    keyboard.append([InlineKeyboardButton("⚙️ Свое время", callback_data="time_custom")])
    keyboard.append([
        InlineKeyboardButton("🔙 Назад", callback_data="analysis_date_back"),
        InlineKeyboardButton("🏠 Главная", callback_data="start")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def get_analysis_repeat_keyboard():
    """Клавиатура для выбора повторения анализа/исследования."""
    keyboard = [
        [
            InlineKeyboardButton("🕐 Одноразово", callback_data="repeat_once"),
            InlineKeyboardButton("📅 Ежедневно", callback_data="repeat_daily"),
        ],
        [
            InlineKeyboardButton("📆 Еженедельно", callback_data="repeat_weekly"),
            InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly"),
        ],
        [
            InlineKeyboardButton("📊 Ежегодно", callback_data="repeat_yearly"),
            InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="analysis_time_back"),
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_reminder_before_keyboard():
    """Клавиатура для выбора времени напоминания."""
    keyboard = [
        [
            InlineKeyboardButton("⏰ 1ч", callback_data="remind_1"),
            InlineKeyboardButton("⏰ 3ч", callback_data="remind_3"),
            InlineKeyboardButton("⏰ 12ч", callback_data="remind_12"),
        ],
        [
            InlineKeyboardButton("⏰ 24ч", callback_data="remind_24"),
            InlineKeyboardButton("⏰ 2д", callback_data="remind_48"),
            InlineKeyboardButton("⏰ 3д", callback_data="remind_72"),
        ],
        [
            InlineKeyboardButton("⏰ 7д", callback_data="remind_168"),
            InlineKeyboardButton("⚙️ Свое", callback_data="remind_custom"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="analysis_repeat_back"),
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_course_days_keyboard():
    """Клавиатура для выбора количества дней курса лекарства."""
    keyboard = [
        [
            InlineKeyboardButton("📅 3 дня", callback_data="course_days_3"),
            InlineKeyboardButton("📅 5 дней", callback_data="course_days_5"),
            InlineKeyboardButton("📅 7 дней", callback_data="course_days_7"),
        ],
        [
            InlineKeyboardButton("📅 10 дней", callback_data="course_days_10"),
            InlineKeyboardButton("📅 14 дней", callback_data="course_days_14"),
            InlineKeyboardButton("📅 21 день", callback_data="course_days_21"),
        ],
        [
            InlineKeyboardButton("📅 30 дней", callback_data="course_days_30"),
            InlineKeyboardButton("📅 60 дней", callback_data="course_days_60"),
            InlineKeyboardButton("📅 90 дней", callback_data="course_days_90"),
        ],
        [
            InlineKeyboardButton("⚙️ Свой вариант", callback_data="course_days_custom"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_stats_keyboard():
    """Клавиатура для статистики."""
    keyboard = [
        [
            InlineKeyboardButton("📊 За неделю", callback_data="stats_week"),
            InlineKeyboardButton("📊 За месяц", callback_data="stats_month"),
        ],
        [
            InlineKeyboardButton("📊 За все время", callback_data="stats_all"),
            InlineKeyboardButton("📊 Самочувствие", callback_data="stats_mood"),
        ],
        [
            InlineKeyboardButton("📊 Симптомы", callback_data="stats_symptoms"),
            InlineKeyboardButton("💊 По лекарствам", callback_data="stats_medicine_detail"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="start"),
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_medicine_stats_keyboard(user_id: int):
    """Клавиатура для выбора лекарства в статистике."""
    db = get_db()
    try:
        medicines = db.query(Medicine).filter(
            Medicine.user_id == user_id,
            Medicine.status == 'active'
        ).all()
        
        keyboard = []
        for med in medicines:
            keyboard.append([InlineKeyboardButton(
                f"💊 {med.name}",
                callback_data=f"stats_medicine_{med.id}"
            )])
        
        keyboard.append([
            InlineKeyboardButton("🔙 Назад", callback_data="stats"),
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    finally:
        db.close()

# ============== ФУНКЦИИ ОТПРАВКИ НАПОМИНАНИЙ ==============
async def send_reminder_job(reminder_id: int):
    """Job для отправки напоминания."""
    global application
    
    db = get_db()
    try:
        reminder = db.query(Reminder).filter_by(id=reminder_id).first()
        if not reminder or reminder.status != 'pending':
            return
        
        user_id = reminder.user_id
        
        if reminder.reminder_type == 'medicine':
            medicine = db.query(Medicine).filter_by(id=reminder.item_id).first()
            if not medicine or medicine.status != 'active':
                reminder.status = 'cancelled'
                db.commit()
                return
            
            text = f"💊 *Время принять лекарство!*\n\n{medicine.name}"
            reply_markup = get_medicine_inline_keyboard(medicine.id)
            
        elif reminder.reminder_type == 'analysis':
            analysis = db.query(Analysis).filter_by(id=reminder.item_id).first()
            if not analysis or analysis.status != 'pending':
                reminder.status = 'cancelled'
                db.commit()
                return
            
            if analysis.scheduled_date.tzinfo is None:
                analysis_date = pytz.UTC.localize(analysis.scheduled_date)
            else:
                analysis_date = analysis.scheduled_date.astimezone(pytz.UTC)
            
            scheduled_local = utc_to_local(analysis_date, analysis.user_timezone)
            text = f"🩺 *Напоминание об анализе/исследовании!*\n\n{analysis.name}\n📅 {scheduled_local.strftime('%d.%m.%Y')} в {analysis.scheduled_time}"
            if analysis.notes:
                text += f"\n\n📝 *Заметки:* {analysis.notes}"
            
            reply_markup = get_analysis_inline_keyboard(analysis.id)
        
        else:
            return
        
        for attempt in range(3):
            try:
                await rate_limiter.acquire(user_id)
                await application.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.MARKDOWN
                )
                
                reminder.status = 'sent'
                reminder.retry_count = attempt + 1
                db.commit()
                
                reminder_logger.info(f"SUCCESS - {reminder.reminder_type} reminder {reminder_id} sent to {user_id}")
                return
                
            except (RetryAfter, TimedOut) as e:
                reminder.retry_count = attempt + 1
                reminder.last_error = str(e)
                db.commit()
                
                reminder_logger.warning(f"RETRY - Attempt {attempt+1} failed for {reminder_id}. Error: {e}")
                
                if attempt < 2:
                    await asyncio.sleep(5 * (attempt + 1))
            
            except Exception as e:
                reminder.status = 'failed'
                reminder.last_error = str(e)
                db.commit()
                
                reminder_logger.error(f"FAILED - {reminder.reminder_type} reminder {reminder_id}. Error: {e}")
                return
        
        reminder.status = 'failed'
        db.commit()
        reminder_logger.error(f"FAILED - {reminder.reminder_type} reminder {reminder_id} after 3 attempts")
        
    except Exception as e:
        reminder_logger.error(f"CRITICAL ERROR in send_reminder_job: {e}")
    finally:
        db.close()

# ============== ПРОВЕРКА ЦЕЛОСТНОСТИ ==============
async def integrity_check(context: ContextTypes.DEFAULT_TYPE):
    """Ежечасная проверка целостности."""
    db = get_db()
    try:
        now_utc = datetime.now(pytz.UTC)
        pending_db = db.query(Reminder).filter(
            Reminder.status == 'pending',
            Reminder.scheduled_time > now_utc
        ).all()
        
        pending_db_ids = {f"{r.reminder_type}_{r.id}" for r in pending_db}
        
        scheduler_jobs = scheduler.scheduler.get_jobs()
        scheduler_job_ids = {job.id for job in scheduler_jobs}
        
        missing_jobs = pending_db_ids - scheduler_job_ids
        for job_id in missing_jobs:
            reminder_id = int(job_id.split('_')[1])
            reminder = db.query(Reminder).filter_by(id=reminder_id).first()
            
            if reminder and reminder.scheduled_time > now_utc:
                scheduler.scheduler.add_job(
                    send_reminder_job,
                    trigger=DateTrigger(run_date=reminder.scheduled_time),
                    id=job_id,
                    args=[reminder_id],
                    replace_existing=True
                )
                reminder_logger.warning(f"INTEGRITY - Восстановлено отсутствующее задание {job_id}")
        
        dead_jobs = scheduler_job_ids - pending_db_ids
        for job_id in dead_jobs:
            if job_id.startswith(('medicine_', 'analysis_')):
                try:
                    scheduler.scheduler.remove_job(job_id)
                    reminder_logger.info(f"INTEGRITY - Удалено мертвое задание {job_id}")
                except JobLookupError:
                    pass
        
        overdue = db.query(Reminder).filter(
            Reminder.status == 'pending',
            Reminder.scheduled_time <= now_utc
        ).all()
        
        for reminder in overdue:
            reminder.status = 'failed'
            reminder.last_error = 'Overdue'
            reminder_logger.warning(f"INTEGRITY - Найдено просроченное напоминание {reminder.id}")
        
        db.commit()
        
        reminder_logger.info(
            f"INTEGRITY - Проверка завершена. "
            f"Восстановлено: {len(missing_jobs)}, "
            f"Удалено: {len(dead_jobs)}, "
            f"Просрочено: {len(overdue)}"
        )
        
    finally:
        db.close()

# ============== ОБРАБОТЧИКИ КОМАНД ==============
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    user = update.effective_user
    reminder_logger.info(f"🔥 ПОЛУЧЕНА КОМАНДА START от {user.id} - {user.first_name}")
    
    welcome_text = f"""👋 *Здравствуйте, {user.first_name}!*

Я *ЛОР-Помощник* — персональный медицинский бот, созданный врачом-оториноларингологом Денисом Казариным.

👶 *Врач ведет прием детей с 0 лет и взрослых*

🤖 *Мои возможности:*
• 💊 Напоминания о приеме лекарств
• 🩺 Напоминания об анализах и исследованиях
• 📊 Отслеживание самочувствия
• 📈 Статистика и отчеты для врача

Начните с добавления лекарства или анализа/исследования!"""

    await update.message.reply_text(
        welcome_text,
        reply_markup=get_start_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    reminder_logger.info("✅ Сообщение отправлено пользователю")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help - упрощенный."""
    help_text = """❓ *Как очистить историю переписки*

Чтобы удалить всю переписку с ботом:

1️⃣ В правом верхнем углу нажмите на свой профиль
2️⃣ В меню выберите пункт "Еще" (или "More")
3️⃣ Прокрутите вниз и нажмите "Удалить переписку" (или "Delete chat")

✅ После этого откроется начальная страница бота
💾 Все ваши сохраненные данные (лекарства, анализы, статистика) останутся без изменений

*Ваши данные в безопасности!*"""

    if update.callback_query:
        await update.callback_query.edit_message_text(
            help_text,
            reply_markup=get_help_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            help_text,
            reply_markup=get_help_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

async def about_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /about (ОБНОВЛЕНО: добавлены Telegram-ссылки)."""
    about_text = DOCTOR_INFO + f"""

📍 *КИТ-клиника:*
{KIT_CLINIC['address']}
📞 {KIT_CLINIC['phone_display']}

📍 *Семейная клиника:*
{FAMILY_CLINIC['address']}
📞 {FAMILY_CLINIC['phone_display']}

📱 *Telegram:*
• Личный: @DENISKAZARIN
• Группа: @KAZARIN_LOR"""

    if update.callback_query:
        await update.callback_query.edit_message_text(
            about_text,
            reply_markup=get_about_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            about_text,
            reply_markup=get_about_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

async def set_timezone_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик установки часового пояса."""
    user_id = update.effective_user.id
    current_tz = get_user_timezone(user_id)
    
    text = f"""🕒 *Настройка часового пояса*

Ваш текущий часовой пояс: *{current_tz}*

Выберите ваш часовой пояс из списка:"""
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=get_timezone_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=get_timezone_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды статистики."""
    text = "📈 *Статистика*\n\nВыберите период или тип статистики:"
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=get_stats_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=get_stats_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

async def stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора статистики."""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    db = get_db()
    
    try:
        if query.data == "stats_week":
            week_ago = datetime.now(pytz.UTC) - timedelta(days=7)
            
            mood_stats = db.query(MoodLog).filter(
                MoodLog.user_id == user_id,
                MoodLog.created_at >= week_ago
            ).all()
            
            symptom_stats = db.query(SymptomLog).filter(
                SymptomLog.user_id == user_id,
                SymptomLog.created_at >= week_ago
            ).all()
            
            medicine_stats = db.query(MedicineLog).filter(
                MedicineLog.user_id == user_id,
                MedicineLog.taken_at >= week_ago
            ).all()
            
            avg_mood = sum(m.mood_score for m in mood_stats) / len(mood_stats) if mood_stats else 0
            
            text = f"""📊 *Статистика за неделю*

😊 *Настроение:*
• Записей: {len(mood_stats)}
• Среднее: {avg_mood:.1f}/5

🩺 *Симптомы:*
• Записей: {len(symptom_stats)}

💊 *Лекарства:*
• Приемов: {len([m for m in medicine_stats if m.status == 'taken'])}
• Пропусков: {len([m for m in medicine_stats if m.status == 'skipped'])}"""
            
        elif query.data == "stats_month":
            month_ago = datetime.now(pytz.UTC) - timedelta(days=30)
            
            mood_stats = db.query(MoodLog).filter(
                MoodLog.user_id == user_id,
                MoodLog.created_at >= month_ago
            ).all()
            
            avg_mood = sum(m.mood_score for m in mood_stats) / len(mood_stats) if mood_stats else 0
            
            text = f"""📊 *Статистика за месяц*

😊 *Настроение:*
• Записей: {len(mood_stats)}
• Среднее: {avg_mood:.1f}/5"""
            
        elif query.data == "stats_all":
            mood_stats = db.query(MoodLog).filter(MoodLog.user_id == user_id).all()
            symptom_stats = db.query(SymptomLog).filter(SymptomLog.user_id == user_id).all()
            medicine_stats = db.query(MedicineLog).filter(MedicineLog.user_id == user_id).all()
            
            avg_mood = sum(m.mood_score for m in mood_stats) / len(mood_stats) if mood_stats else 0
            
            text = f"""📊 *Вся статистика*

😊 *Настроение:*
• Всего записей: {len(mood_stats)}
• Среднее: {avg_mood:.1f}/5

🩺 *Симптомы:*
• Всего записей: {len(symptom_stats)}

💊 *Лекарства:*
• Всего приемов: {len(medicine_stats)}"""
            
        elif query.data == "stats_mood":
            month_ago = datetime.now(pytz.UTC) - timedelta(days=30)
            mood_stats = db.query(MoodLog).filter(
                MoodLog.user_id == user_id,
                MoodLog.created_at >= month_ago
            ).order_by(MoodLog.created_at).all()
            
            if mood_stats:
                text = "📈 *Динамика настроения:*\n\n"
                for mood in mood_stats[-10:]:
                    local_time = utc_to_local(mood.created_at, get_user_timezone(user_id))
                    emoji = "😢" if mood.mood_score <=2 else "😐" if mood.mood_score==3 else "😊"
                    text += f"{local_time.strftime('%d.%m')}: {emoji} {mood.mood_score}/5\n"
            else:
                text = "📊 Нет данных о настроении"
                
        elif query.data == "stats_symptoms":
            month_ago = datetime.now(pytz.UTC) - timedelta(days=30)
            symptoms = db.query(SymptomLog).filter(
                SymptomLog.user_id == user_id,
                SymptomLog.created_at >= month_ago
            ).all()
            
            if symptoms:
                symptom_counts = defaultdict(int)
                symptom_severity = defaultdict(list)
                for s in symptoms:
                    symptom_counts[s.symptom] += 1
                    symptom_severity[s.symptom].append(s.severity)
                
                text = "🩺 *Статистика симптомов:*\n\n"
                for symptom, count in sorted(symptom_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                    avg_severity = sum(symptom_severity[symptom]) / len(symptom_severity[symptom])
                    text += f"• {symptom}: {count} раз(а), средняя тяжесть {avg_severity:.1f}/5\n"
            else:
                text = "📊 Нет данных о симптомах"
                
        elif query.data == "stats_medicine_detail":
            await query.edit_message_text(
                "💊 *Выберите лекарство для просмотра статистики:*",
                reply_markup=get_medicine_stats_keyboard(user_id),
                parse_mode=ParseMode.MARKDOWN
            )
            return
            
        elif query.data.startswith("stats_medicine_"):
            medicine_id = int(query.data.replace("stats_medicine_", ""))
            medicine = db.query(Medicine).filter_by(id=medicine_id).first()
            
            if not medicine:
                await query.edit_message_text(
                    "❌ Лекарство не найдено",
                    reply_markup=get_navigation_keyboard("stats")
                )
                return
            
            month_ago = datetime.now(pytz.UTC) - timedelta(days=30)
            logs = db.query(MedicineLog).filter(
                MedicineLog.medicine_id == medicine_id,
                MedicineLog.user_id == user_id,
                MedicineLog.taken_at >= month_ago
            ).all()
            
            taken = len([l for l in logs if l.status == 'taken'])
            skipped = len([l for l in logs if l.status == 'skipped'])
            total = taken + skipped
            adherence = (taken / total * 100) if total > 0 else 0
            
            text = f"""💊 *Статистика по препарату:* {medicine.name}

📅 За последние 30 дней:
✅ Принято: {taken}
❌ Пропущено: {skipped}
📊 Приверженность: {adherence:.1f}%

⏰ Расписание: {medicine.schedule}"""
            
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 К списку лекарств", callback_data="stats_medicine_detail")],
                    [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return
            
        else:
            text = "📈 Выберите тип статистики"
        
        await query.edit_message_text(
            text,
            reply_markup=get_navigation_keyboard("stats"),
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        reminder_logger.error(f"STATS ERROR: {e}")
        await query.edit_message_text(
            "❌ Ошибка при получении статистики",
            reply_markup=get_back_keyboard()
        )
    finally:
        db.close()

# ============== ОБРАБОТЧИКИ ДОБАВЛЕНИЯ ЛЕКАРСТВ ==============
async def add_medicine_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало добавления лекарства."""
    query = update.callback_query
    await query.answer()
    
    context.user_data['medicine_data'] = {}
    
    await query.edit_message_text(
        "💊 *Добавление лекарства*\n\n"
        "Шаг 1/7: Введите *название лекарства*",
        parse_mode=ParseMode.MARKDOWN
    )
    
    return MEDICINE_NAME

async def add_medicine_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получение названия лекарства."""
    context.user_data['medicine_data']['name'] = update.message.text
    
    keyboard = [
        [
            InlineKeyboardButton("08:00", callback_data="time_08:00"),
            InlineKeyboardButton("08:00,20:00", callback_data="time_08:00,20:00"),
        ],
        [
            InlineKeyboardButton("09:00,13:00,21:00", callback_data="time_09:00,13:00,21:00"),
            InlineKeyboardButton("⚙️ Свой вариант", callback_data="time_custom"),
        ],
        [
            InlineKeyboardButton("🔙 Отмена", callback_data="start"),
        ]
    ]
    
    await update.message.reply_text(
        "Шаг 2/7: Выберите *время приема*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return MEDICINE_TIME

async def add_medicine_time_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора времени."""
    if not update.callback_query:
        time_text = update.message.text.strip()
        
        if re.match(r'^\d{1,2}:\d{2}$', time_text):
            parts = time_text.split(':')
            hour = int(parts[0])
            minute = int(parts[1])
            context.user_data['medicine_data']['schedule'] = f"{hour:02d}:{minute:02d}"
        elif re.match(r'^\d{1,2}:\d{2},\s*\d{1,2}:\d{2}$', time_text.replace(' ', '')):
            times = []
            for t in time_text.replace(' ', '').split(','):
                parts = t.split(':')
                hour = int(parts[0])
                minute = int(parts[1])
                times.append(f"{hour:02d}:{minute:02d}")
            context.user_data['medicine_data']['schedule'] = ','.join(times)
        else:
            await update.message.reply_text(
                "❌ Неверный формат. Используйте ЧЧ:ММ (например: 9:42, 11:08)",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="add_medicine")]
                ])
            )
            return MEDICINE_TIME
        
        keyboard = [
            [
                InlineKeyboardButton("📅 Дни", callback_data="course_days"),
                InlineKeyboardButton("🗓️ Месяцы", callback_data="course_months"),
            ],
            [
                InlineKeyboardButton("∞ Бессрочно", callback_data="course_unlimited"),
            ],
            [
                InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
            ]
        ]
        
        await update.message.reply_text(
            "Шаг 3/7: Выберите *тип курса*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_COURSE_TYPE
    
    query = update.callback_query
    await query.answer()
    
    if query.data == "time_custom":
        await query.edit_message_text(
            "Введите время в формате *ЧЧ:ММ*\n"
            "Минуты можно указывать любые (например: 9:42, 11:08, 15:30)\n"
            "Для нескольких приемов укажите через запятую (например: 9:00,18:30)",
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_TIME
    
    context.user_data['medicine_data']['schedule'] = query.data.replace("time_", "")
    
    keyboard = [
        [
            InlineKeyboardButton("📅 Дни", callback_data="course_days"),
            InlineKeyboardButton("🗓️ Месяцы", callback_data="course_months"),
        ],
        [
            InlineKeyboardButton("∞ Бессрочно", callback_data="course_unlimited"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
        ]
    ]
    
    await query.edit_message_text(
        "Шаг 3/7: Выберите *тип курса*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return MEDICINE_COURSE_TYPE

async def add_medicine_course_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка типа курса."""
    query = update.callback_query
    await query.answer()
    
    course_type = query.data.replace("course_", "")
    context.user_data['medicine_data']['course_type'] = course_type
    
    if course_type == 'unlimited':
        context.user_data['medicine_data']['repeat_type'] = 'none'
        keyboard = [
            [
                InlineKeyboardButton("Сегодня", callback_data="start_today"),
                InlineKeyboardButton("Завтра", callback_data="start_tomorrow"),
            ],
            [
                InlineKeyboardButton("📅 Выбрать дату", callback_data="start_custom"),
            ],
            [
                InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
            ]
        ]
        
        await query.edit_message_text(
            "Шаг 4/7: Выберите *дату начала* приема",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_START_DATE
    elif course_type == 'days':
        await query.edit_message_text(
            "Шаг 4/7: Выберите *количество дней* курса:",
            reply_markup=get_course_days_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_COURSE_DAYS
    else:
        keyboard = [
            [
                InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                InlineKeyboardButton("📅 Еженедельно", callback_data="repeat_weekly"),
            ],
            [
                InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly"),
                InlineKeyboardButton("🔢 Каждые N дней", callback_data="repeat_custom"),
            ],
            [
                InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
            ]
        ]
        
        await query.edit_message_text(
            "Шаг 4/7: Выберите *повторение курса*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_REPEAT

async def add_medicine_course_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора количества дней курса."""
    if not update.callback_query:
        try:
            days = int(update.message.text.strip())
            if days < 1 or days > 365:
                raise ValueError
            context.user_data['medicine_data']['course_days'] = days
        except:
            await update.message.reply_text(
                "❌ Введите число от 1 до 365",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="add_medicine")]
                ])
            )
            return MEDICINE_COURSE_DAYS
        
        keyboard = [
            [
                InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                InlineKeyboardButton("📅 Еженедельно", callback_data="repeat_weekly"),
            ],
            [
                InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly"),
                InlineKeyboardButton("🔢 Каждые N дней", callback_data="repeat_custom"),
            ],
            [
                InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
            ]
        ]
        
        await update.message.reply_text(
            "Шаг 5/7: Выберите *повторение курса*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_REPEAT
    
    query = update.callback_query
    await query.answer()
    
    if query.data == "course_days_custom":
        await query.edit_message_text(
            "Введите количество дней (от 1 до 365):",
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_COURSE_DAYS
    
    days = int(query.data.replace("course_days_", ""))
    context.user_data['medicine_data']['course_days'] = days
    
    keyboard = [
        [
            InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
            InlineKeyboardButton("📅 Еженедельно", callback_data="repeat_weekly"),
        ],
        [
            InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly"),
            InlineKeyboardButton("🔢 Каждые N дней", callback_data="repeat_custom"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
        ]
    ]
    
    await query.edit_message_text(
        "Шаг 5/7: Выберите *повторение курса*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return MEDICINE_REPEAT

async def add_medicine_repeat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка повторения курса."""
    if not update.callback_query:
        try:
            days = int(update.message.text.strip())
            if days < 1 or days > 365:
                raise ValueError
            context.user_data['medicine_data']['repeat_days'] = days
            context.user_data['medicine_data']['repeat_type'] = 'custom'
        except:
            await update.message.reply_text(
                "❌ Введите число от 1 до 365",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="add_medicine")]
                ])
            )
            return MEDICINE_REPEAT
        
        keyboard = [
            [
                InlineKeyboardButton("Сегодня", callback_data="start_today"),
                InlineKeyboardButton("Завтра", callback_data="start_tomorrow"),
            ],
            [
                InlineKeyboardButton("📅 Выбрать дату", callback_data="start_custom"),
            ],
            [
                InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
            ]
        ]
        
        await update.message.reply_text(
            "Шаг 6/7: Выберите *дату начала* приема",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_START_DATE
    
    query = update.callback_query
    await query.answer()
    
    if query.data == "repeat_custom":
        await query.edit_message_text(
            "Введите количество дней для повторения (от 1 до 365):",
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_REPEAT
    
    context.user_data['medicine_data']['repeat_type'] = query.data.replace("repeat_", "")
    
    keyboard = [
        [
            InlineKeyboardButton("Сегодня", callback_data="start_today"),
            InlineKeyboardButton("Завтра", callback_data="start_tomorrow"),
        ],
        [
            InlineKeyboardButton("📅 Выбрать дату", callback_data="start_custom"),
        ],
        [
            InlineKeyboardButton("🔙 Назад", callback_data="add_medicine"),
        ]
    ]
    
    await query.edit_message_text(
        "Шаг 6/7: Выберите *дату начала* приема",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return MEDICINE_START_DATE

async def add_medicine_start_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка даты начала."""
    user_id = update.effective_user.id
    tz_name = get_user_timezone(user_id)
    
    if not update.callback_query:
        date_str = update.message.text.strip()
        date = parse_date(date_str, tz_name)
        
        if not date:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="add_medicine")]
                ])
            )
            return MEDICINE_START_DATE
        
        context.user_data['medicine_data']['start_date'] = date
        
        medicine_data = context.user_data['medicine_data']
        
        confirm_text = f"""✅ *Проверьте данные:*

💊 *Название:* {medicine_data['name']}
⏰ *Время:* {medicine_data['schedule']}
📅 *Тип курса:* {medicine_data['course_type']}
{'📊 *Дней курса:* ' + str(medicine_data.get('course_days', '')) if medicine_data.get('course_days') else ''}
🔄 *Повторение:* {medicine_data.get('repeat_type', 'none')}
📆 *Дата начала:* {date.strftime('%d.%m.%Y')}

Всё верно?"""
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Добавить", callback_data="confirm_medicine"),
                InlineKeyboardButton("✏️ Исправить", callback_data="add_medicine"),
            ],
            [
                InlineKeyboardButton("❌ Отмена", callback_data="start"),
            ]
        ]
        
        await update.message.reply_text(
            confirm_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_CONFIRM
    
    query = update.callback_query
    await query.answer()
    
    tz = pytz.timezone(tz_name)
    
    if query.data == "start_today":
        context.user_data['medicine_data']['start_date'] = datetime.now(tz)
    elif query.data == "start_tomorrow":
        context.user_data['medicine_data']['start_date'] = datetime.now(tz) + timedelta(days=1)
    elif query.data == "start_custom":
        await query.edit_message_text(
            "Введите дату в формате *ДД.ММ.ГГГГ*",
            parse_mode=ParseMode.MARKDOWN
        )
        return MEDICINE_START_DATE
    else:
        return MEDICINE_START_DATE
    
    medicine_data = context.user_data['medicine_data']
    
    confirm_text = f"""✅ *Проверьте данные:*

💊 *Название:* {medicine_data['name']}
⏰ *Время:* {medicine_data['schedule']}
📅 *Тип курса:* {medicine_data['course_type']}
{'📊 *Дней курса:* ' + str(medicine_data.get('course_days', '')) if medicine_data.get('course_days') else ''}
🔄 *Повторение:* {medicine_data.get('repeat_type', 'none')}
📆 *Дата начала:* {medicine_data['start_date'].strftime('%d.%m.%Y')}

Всё верно?"""
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Добавить", callback_data="confirm_medicine"),
            InlineKeyboardButton("✏️ Исправить", callback_data="add_medicine"),
        ],
        [
            InlineKeyboardButton("❌ Отмена", callback_data="start"),
        ]
    ]
    
    await query.edit_message_text(
        confirm_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return MEDICINE_CONFIRM

async def add_medicine_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение добавления лекарства."""
    query = update.callback_query
    await query.answer()
    
    if query.data != "confirm_medicine":
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    tz_name = get_user_timezone(user_id)
    medicine_data = context.user_data['medicine_data']
    
    db = get_db()
    try:
        medicine = Medicine(
            user_id=user_id,
            name=medicine_data['name'],
            schedule=medicine_data['schedule'],
            start_date=medicine_data['start_date'],
            user_timezone=tz_name,
            course_type=medicine_data['course_type'],
            course_days=medicine_data.get('course_days'),
            repeat_type=medicine_data.get('repeat_type', 'none'),
            repeat_days=medicine_data.get('repeat_days')
        )
        db.add(medicine)
        db.flush()
        
        times = medicine_data['schedule'].split(',')
        for time_str in times:
            scheduled_utc = local_to_utc(time_str.strip(), tz_name, medicine.start_date)
            
            reminder = Reminder(
                user_id=user_id,
                reminder_type='medicine',
                item_id=medicine.id,
                scheduled_time=scheduled_utc,
                user_timezone=tz_name
            )
            db.add(reminder)
            db.flush()
            
            job_id = f"medicine_{reminder.id}"
            scheduler.scheduler.add_job(
                send_reminder_job,
                trigger=DateTrigger(run_date=scheduled_utc),
                id=job_id,
                args=[reminder.id],
                replace_existing=True
            )
            reminder_logger.info(f"SCHEDULED - medicine reminder {reminder.id} for {scheduled_utc}")
        
        db.commit()
        
        keyboard = [
            [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines")],
            [
                InlineKeyboardButton("➕ Добавить еще", callback_data="add_medicine"),
                InlineKeyboardButton("🏠 Главная", callback_data="start")
            ]
        ]
        
        await query.edit_message_text(
            "✅ *Лекарство успешно добавлено!*\n\n"
            f"💊 {medicine.name}\n"
            f"⏰ {medicine.schedule}\n\n"
            "Напоминания настроены и будут приходить по расписанию.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        reminder_logger.info(f"MEDICINE - Добавлено лекарство {medicine.id} для пользователя {user_id}")
        
    except Exception as e:
        db.rollback()
        reminder_logger.error(f"MEDICINE ERROR - {e}")
        await query.edit_message_text(
            "❌ *Ошибка при добавлении лекарства*\n\n"
            f"Пожалуйста, попробуйте позже.",
            reply_markup=get_back_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    finally:
        db.close()
        if 'medicine_data' in context.user_data:
            del context.user_data['medicine_data']
    
    return ConversationHandler.END

# ============== ОБРАБОТЧИКИ ДОБАВЛЕНИЯ АНАЛИЗОВ ==============
async def add_analysis_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало добавления анализа/исследования."""
    query = update.callback_query
    await query.answer()
    
    context.user_data['analysis_data'] = {}
    
    await query.edit_message_text(
        "🩺 *Добавление анализа или исследования*\n\n"
        "Шаг 1/6: Введите *название анализа/исследования*",
        parse_mode=ParseMode.MARKDOWN
    )
    
    return ANALYSIS_NAME

async def add_analysis_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получение названия анализа/исследования."""
    context.user_data['analysis_data']['name'] = update.message.text
    
    await update.message.reply_text(
        "Шаг 2/6: Выберите *дату* анализа/исследования",
        reply_markup=get_analysis_date_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return ANALYSIS_DATE

async def add_analysis_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора даты анализа/исследования."""
    user_id = update.effective_user.id
    tz_name = get_user_timezone(user_id)
    
    if not update.callback_query:
        date_str = update.message.text.strip()
        date = parse_date(date_str, tz_name)
        
        if not date:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ",
                reply_markup=get_navigation_keyboard("add_analysis")
            )
            return ANALYSIS_DATE
        
        context.user_data['analysis_data']['scheduled_date'] = date
    else:
        query = update.callback_query
        await query.answer()
        
        if query.data == "analysis_date_custom":
            await query.edit_message_text(
                "Введите дату в формате *ДД.ММ.ГГГГ*",
                parse_mode=ParseMode.MARKDOWN
            )
            return ANALYSIS_DATE
        
        if query.data == "analysis_date_back":
            await add_analysis_start(update, context)
            return ANALYSIS_NAME
        
        date_str = query.data.replace("analysis_date_", "")
        date = parse_date(date_str, tz_name)
        
        if date:
            context.user_data['analysis_data']['scheduled_date'] = date
        else:
            await query.edit_message_text(
                "❌ Ошибка в формате даты",
                reply_markup=get_navigation_keyboard("add_analysis")
            )
            return ANALYSIS_DATE
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "Шаг 3/6: Выберите *время* анализа/исследования\n\n"
            "Вы можете выбрать час, затем уточнить минуты:",
            reply_markup=get_analysis_time_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            "Шаг 3/6: Выберите *время* анализа/исследования\n\n"
            "Вы можете выбрать час, затем уточнить минуты:",
            reply_markup=get_analysis_time_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    return ANALYSIS_TIME

async def add_analysis_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора времени анализа/исследования."""
    if not update.callback_query:
        time_text = update.message.text.strip()
        
        if re.match(r'^\d{1,2}:\d{2}$', time_text):
            parts = time_text.split(':')
            hour = int(parts[0])
            minute = int(parts[1])
            selected_time = f"{hour:02d}:{minute:02d}"
            
            user_id = update.effective_user.id
            scheduled_date = context.user_data['analysis_data'].get('scheduled_date')
            
            if scheduled_date and check_existing_analysis(user_id, scheduled_date, selected_time):
                await update.message.reply_text(
                    "⚠️ *Внимание!*\n\n"
                    f"На {scheduled_date.strftime('%d.%m.%Y')} в {selected_time} "
                    "уже запланирован анализ/исследование.\n\n"
                    "Вы можете:\n"
                    "• Выбрать другое время\n"
                    "• Создать запись на это же время (будет дублирование)",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⏰ Выбрать другое время", callback_data="analysis_time_back")],
                        [InlineKeyboardButton("✅ Все равно создать", callback_data=f"time_{selected_time}")],
                        [InlineKeyboardButton("🔙 Главная", callback_data="start")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                return ANALYSIS_TIME
            
            context.user_data['analysis_data']['scheduled_time'] = selected_time
        else:
            await update.message.reply_text(
                "❌ Неверный формат. Используйте ЧЧ:ММ (например: 9:42, 11:08)",
                reply_markup=get_navigation_keyboard("analysis_time_back")
            )
            return ANALYSIS_TIME
        
        await update.message.reply_text(
            "Шаг 4/6: Выберите *повторение* анализа/исследования",
            reply_markup=get_analysis_repeat_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        return ANALYSIS_REPEAT
    
    query = update.callback_query
    await query.answer()
    
    if query.data == "time_custom":
        await query.edit_message_text(
            "Введите время в формате *ЧЧ:ММ*\n"
            "Минуты можно указывать любые (например: 9:42, 11:08, 15:30)",
            parse_mode=ParseMode.MARKDOWN
        )
        return ANALYSIS_TIME
    
    if query.data == "analysis_time_back":
        return await add_analysis_date(update, context)
    
    if query.data.startswith("time_minute_"):
        minute = query.data.replace("time_minute_", "")
        current_time = context.user_data['analysis_data'].get('temp_time', '12:00')
        hour = current_time.split(':')[0]
        selected_time = f"{hour}:{minute}"
        
        user_id = update.effective_user.id
        scheduled_date = context.user_data['analysis_data'].get('scheduled_date')
        
        if scheduled_date and check_existing_analysis(user_id, scheduled_date, selected_time):
            await query.edit_message_text(
                "⚠️ *Внимание!*\n\n"
                f"На {scheduled_date.strftime('%d.%m.%Y')} в {selected_time} "
                "уже запланирован анализ/исследование.\n\n"
                "Вы можете:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏰ Выбрать другое время", callback_data="analysis_time_back")],
                    [InlineKeyboardButton("✅ Все равно создать", callback_data=f"time_{selected_time}")],
                    [InlineKeyboardButton("🔙 Главная", callback_data="start")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return ANALYSIS_TIME
        
        context.user_data['analysis_data']['scheduled_time'] = selected_time
        await query.edit_message_text(
            "Шаг 4/6: Выберите *повторение* анализа/исследования",
            reply_markup=get_analysis_repeat_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        return ANALYSIS_REPEAT
    
    if query.data.startswith("time_"):
        selected_time = query.data.replace("time_", "")
        context.user_data['analysis_data']['temp_time'] = selected_time
        
        await query.edit_message_text(
            f"Вы выбрали время *{selected_time}*. Теперь уточните минуты:",
            reply_markup=get_analysis_time_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        return ANALYSIS_TIME

async def add_analysis_repeat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора повторения анализа/исследования."""
    if not update.callback_query:
        try:
            interval = int(update.message.text.strip())
            if interval < 1 or interval > 365:
                raise ValueError
            context.user_data['analysis_data']['repeat_type'] = 'custom'
            context.user_data['analysis_data']['repeat_interval'] = interval
        except:
            await update.message.reply_text(
                "❌ Введите число от 1 до 365",
                reply_markup=get_navigation_keyboard("analysis_repeat_back")
            )
            return ANALYSIS_REPEAT
        
        await update.message.reply_text(
            "Шаг 5/6: *Когда напомнить?*",
            reply_markup=get_reminder_before_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        return ANALYSIS_REMINDER
    
    query = update.callback_query
    await query.answer()
    
    if query.data == "repeat_custom":
        await query.edit_message_text(
            "Введите интервал в днях (от 1 до 365):",
            parse_mode=ParseMode.MARKDOWN
        )
        return ANALYSIS_REPEAT
    
    if query.data == "analysis_repeat_back":
        return await add_analysis_time(update, context)
    
    repeat_map = {
        "repeat_once": "once",
        "repeat_daily": "daily",
        "repeat_weekly": "weekly",
        "repeat_monthly": "monthly",
        "repeat_yearly": "yearly"
    }
    
    context.user_data['analysis_data']['repeat_type'] = repeat_map.get(query.data, "once")
    
    await query.edit_message_text(
        "Шаг 5/6: *Когда напомнить?*\n\n"
        "Выберите за сколько времени до исследования отправить напоминание:",
        reply_markup=get_reminder_before_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return ANALYSIS_REMINDER

async def add_analysis_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора времени напоминания."""
    if not update.callback_query:
        try:
            hours = int(update.message.text.strip())
            if hours < 1 or hours > 720:
                raise ValueError
            context.user_data['analysis_data']['reminder_before'] = hours
        except:
            await update.message.reply_text(
                "❌ Введите число часов от 1 до 720",
                reply_markup=get_navigation_keyboard("analysis_reminder_back")
            )
            return ANALYSIS_REMINDER
        
        await update.message.reply_text(
            "Шаг 6/6: Введите *заметки* к анализу/исследованию (или отправьте /skip чтобы пропустить)",
            parse_mode=ParseMode.MARKDOWN
        )
        return ANALYSIS_NOTES
    
    query = update.callback_query
    await query.answer()
    
    if query.data == "remind_custom":
        await query.edit_message_text(
            "Введите количество часов (от 1 до 720):",
            parse_mode=ParseMode.MARKDOWN
        )
        return ANALYSIS_REMINDER
    
    if query.data == "analysis_reminder_back":
        return await add_analysis_repeat(update, context)
    
    hours_map = {
        "remind_1": 1,
        "remind_3": 3,
        "remind_12": 12,
        "remind_24": 24,
        "remind_48": 48,
        "remind_72": 72,
        "remind_168": 168
    }
    
    context.user_data['analysis_data']['reminder_before'] = hours_map.get(query.data, 24)
    
    await query.edit_message_text(
        "Шаг 6/6: Введите *заметки* к анализу/исследованию (или отправьте /skip чтобы пропустить)",
        parse_mode=ParseMode.MARKDOWN
    )
    
    return ANALYSIS_NOTES

async def add_analysis_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка заметок к анализу/исследованию."""
    if update.message.text == "/skip":
        context.user_data['analysis_data']['notes'] = None
    else:
        context.user_data['analysis_data']['notes'] = update.message.text
    
    analysis_data = context.user_data['analysis_data']
    user_id = update.effective_user.id
    tz_name = get_user_timezone(user_id)
    
    if 'scheduled_date' not in analysis_data:
        reminder_logger.error(f"ANALYSIS ERROR: scheduled_date not in analysis_data for user {user_id}")
        await update.message.reply_text(
            "❌ Ошибка данных. Пожалуйста, начните заново.",
            reply_markup=get_start_keyboard()
        )
        return ConversationHandler.END
    
    scheduled_date_local = analysis_data['scheduled_date'].astimezone(pytz.timezone(tz_name))
    
    repeat_text = {
        "once": "Одноразово",
        "daily": "Ежедневно",
        "weekly": "Еженедельно",
        "monthly": "Ежемесячно",
        "yearly": "Ежегодно",
        "custom": f"Каждые {analysis_data.get('repeat_interval', 'N')} дней"
    }.get(analysis_data['repeat_type'], "Одноразово")
    
    confirm_text = f"""✅ *Проверьте данные анализа/исследования:*

🩺 *Название:* {analysis_data['name']}
📅 *Дата:* {scheduled_date_local.strftime('%d.%m.%Y')}
⏰ *Время:* {analysis_data.get('scheduled_time', '12:00')}
🔄 *Повторение:* {repeat_text}
⏰ *Напомнить за:* {analysis_data['reminder_before']} ч."""

    if analysis_data.get('notes'):
        confirm_text += f"\n📝 *Заметки:* {analysis_data['notes']}"
    
    confirm_text += "\n\nВсё верно?"
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Добавить", callback_data="confirm_analysis"),
            InlineKeyboardButton("✏️ Исправить", callback_data="add_analysis"),
        ],
        [
            InlineKeyboardButton("❌ Отмена", callback_data="start"),
        ]
    ]
    
    await update.message.reply_text(
        confirm_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return ANALYSIS_CONFIRM

async def skip_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пропуск заметок."""
    context.user_data['analysis_data']['notes'] = None
    return await add_analysis_notes(update, context)

async def add_analysis_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение добавления анализа/исследования."""
    query = update.callback_query
    await query.answer()
    
    if query.data != "confirm_analysis":
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    tz_name = get_user_timezone(user_id)
    analysis_data = context.user_data['analysis_data']
    
    if 'scheduled_date' not in analysis_data:
        reminder_logger.error(f"ANALYSIS CONFIRM ERROR: scheduled_date missing for user {user_id}")
        await query.edit_message_text(
            "❌ Ошибка данных. Пожалуйста, начните заново.",
            reply_markup=get_start_keyboard()
        )
        return ConversationHandler.END
    
    db = get_db()
    try:
        scheduled_time = analysis_data.get('scheduled_time', '12:00')
        hour, minute = map(int, scheduled_time.split(':'))
        scheduled_datetime = analysis_data['scheduled_date'].replace(hour=hour, minute=minute)
        
        analysis = Analysis(
            user_id=user_id,
            name=analysis_data['name'],
            scheduled_date=scheduled_datetime,
            scheduled_time=scheduled_time,
            repeat_type=analysis_data['repeat_type'],
            repeat_interval=analysis_data.get('repeat_interval'),
            reminder_before=analysis_data['reminder_before'],
            notes=analysis_data.get('notes'),
            user_timezone=tz_name
        )
        db.add(analysis)
        db.flush()
        
        reminder_time = scheduled_datetime - timedelta(hours=analysis.reminder_before)
        if reminder_time > datetime.now(pytz.UTC):
            reminder = Reminder(
                user_id=user_id,
                reminder_type='analysis',
                item_id=analysis.id,
                scheduled_time=reminder_time,
                user_timezone=tz_name
            )
            db.add(reminder)
            db.flush()
            
            job_id = f"analysis_{reminder.id}"
            scheduler.scheduler.add_job(
                send_reminder_job,
                trigger=DateTrigger(run_date=reminder_time),
                id=job_id,
                args=[reminder.id],
                replace_existing=True
            )
            reminder_logger.info(f"SCHEDULED - analysis reminder {reminder.id} for {reminder_time}")
        
        db.commit()
        
        keyboard = [
            [InlineKeyboardButton("📋 Список анализов/исследований", callback_data="list_analyses")],
            [
                InlineKeyboardButton("➕ Добавить еще", callback_data="add_analysis"),
                InlineKeyboardButton("🏠 Главная", callback_data="start")
            ]
        ]
        
        scheduled_local = scheduled_datetime.astimezone(pytz.timezone(tz_name))
        
        await query.edit_message_text(
            "✅ *Анализ/исследование успешно добавлен!*\n\n"
            f"🩺 {analysis.name}\n"
            f"📅 {scheduled_local.strftime('%d.%m.%Y %H:%M')}\n\n"
            "Напоминание настроено.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        reminder_logger.info(f"ANALYSIS - Добавлен анализ {analysis.id} для пользователя {user_id}")
        
    except Exception as e:
        db.rollback()
        reminder_logger.error(f"ANALYSIS ERROR: {e}")
        await query.edit_message_text(
            "❌ *Ошибка при добавлении анализа/исследования*\n\n"
            f"Пожалуйста, попробуйте позже.",
            reply_markup=get_back_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    finally:
        db.close()
        if 'analysis_data' in context.user_data:
            del context.user_data['analysis_data']
    
    return ConversationHandler.END

# ============== ОБРАБОТЧИКИ СПИСКА ЛЕКАРСТВ ==============
async def list_medicines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр списка лекарств."""
    user_id = update.effective_user.id
    
    query = update.callback_query
    if query:
        await query.answer()
    
    db = get_db()
    try:
        medicines = db.query(Medicine).filter(
            Medicine.user_id == user_id,
            Medicine.status == 'active'
        ).order_by(Medicine.created_at.desc()).all()
        
        if not medicines:
            text = "📋 *У вас нет активных лекарств*"
            keyboard = [
                [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")],
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]
        else:
            text = "📋 *Ваши лекарства:*\n\n"
            keyboard = []
            
            for i, med in enumerate(medicines, 1):
                text += f"{i}. *{med.name}*\n"
                text += f"   ⏰ {med.schedule}\n"
                if med.start_date:
                    if med.start_date.tzinfo is None:
                        start_date = pytz.UTC.localize(med.start_date)
                    else:
                        start_date = med.start_date.astimezone(pytz.UTC)
                    start_local = utc_to_local(start_date, med.user_timezone)
                    text += f"   📅 с {start_local.strftime('%d.%m.%Y')}\n"
                text += f"   📊 {med.course_type}"
                if med.course_days:
                    text += f" ({med.course_days} дн.)"
                text += "\n\n"
                
                keyboard.append([InlineKeyboardButton(
                    f"🗑️ Удалить {med.name}",
                    callback_data=f"delete_medicine_{med.id}"
                )])
            
            keyboard.append([InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")])
            keyboard.append([InlineKeyboardButton("🏠 Главная", callback_data="start")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if query:
            await query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
    
    finally:
        db.close()

async def list_analyses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр списка анализов/исследований."""
    user_id = update.effective_user.id
    
    query = update.callback_query
    if query:
        await query.answer()
    
    db = get_db()
    try:
        analyses = db.query(Analysis).filter(
            Analysis.user_id == user_id,
            Analysis.status == 'pending'
        ).order_by(Analysis.scheduled_date.asc()).all()
        
        if not analyses:
            text = "📋 *У вас нет запланированных анализов/исследований*"
            keyboard = [
                [InlineKeyboardButton("🩺 Добавить анализ/исследование", callback_data="add_analysis")],
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]
        else:
            text = "📋 *Запланированные анализы/исследования:*\n\n"
            keyboard = []
            
            now = datetime.now(pytz.UTC)
            for i, analysis in enumerate(analyses, 1):
                if analysis.scheduled_date.tzinfo is None:
                    analysis_date = pytz.UTC.localize(analysis.scheduled_date)
                else:
                    analysis_date = analysis.scheduled_date.astimezone(pytz.UTC)
                
                scheduled_local = utc_to_local(analysis_date, analysis.user_timezone)
                days_left = (analysis_date - now).days
                
                if days_left < 0:
                    status = "🔴 Просрочен"
                elif days_left == 0:
                    status = "🟡 Сегодня"
                elif days_left == 1:
                    status = "🟡 Завтра"
                else:
                    status = f"🟢 Через {days_left} дн."
                
                text += f"{i}. *{analysis.name}*\n"
                text += f"   📅 {scheduled_local.strftime('%d.%m.%Y')} в {analysis.scheduled_time}\n"
                text += f"   📊 {status}\n"
                text += f"   ⏰ Напомнить за {analysis.reminder_before} ч.\n"
                if analysis.notes:
                    text += f"   📝 {analysis.notes}\n"
                text += "\n"
                
                keyboard.append([InlineKeyboardButton(
                    f"🗑️ Удалить {analysis.name}",
                    callback_data=f"delete_analysis_{analysis.id}"
                )])
            
            keyboard.append([InlineKeyboardButton("🩺 Добавить анализ/исследование", callback_data="add_analysis")])
            keyboard.append([InlineKeyboardButton("🏠 Главная", callback_data="start")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if query:
            await query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
    
    finally:
        db.close()

async def delete_medicine(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление лекарства."""
    query = update.callback_query
    await query.answer()
    
    medicine_id = int(query.data.replace("delete_medicine_", ""))
    
    db = get_db()
    try:
        medicine = db.query(Medicine).filter_by(id=medicine_id).first()
        if medicine:
            medicine.status = 'deleted'
            
            reminders = db.query(Reminder).filter(
                Reminder.item_id == medicine_id,
                Reminder.reminder_type == 'medicine',
                Reminder.status == 'pending'
            ).all()
            
            for reminder in reminders:
                reminder.status = 'cancelled'
                try:
                    scheduler.scheduler.remove_job(f"medicine_{reminder.id}")
                except JobLookupError:
                    pass
            
            db.commit()
            
            await query.edit_message_text(
                f"✅ Лекарство *{medicine.name}* удалено",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines")],
                    [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            
            reminder_logger.info(f"MEDICINE - Удалено лекарство {medicine_id}")
    
    finally:
        db.close()

async def delete_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление анализа/исследования."""
    query = update.callback_query
    await query.answer()
    
    analysis_id = int(query.data.replace("delete_analysis_", ""))
    
    db = get_db()
    try:
        analysis = db.query(Analysis).filter_by(id=analysis_id).first()
        if analysis:
            analysis.status = 'cancelled'
            
            reminders = db.query(Reminder).filter(
                Reminder.item_id == analysis_id,
                Reminder.reminder_type == 'analysis',
                Reminder.status == 'pending'
            ).all()
            
            for reminder in reminders:
                reminder.status = 'cancelled'
                try:
                    scheduler.scheduler.remove_job(f"analysis_{reminder.id}")
                except JobLookupError:
                    pass
            
            db.commit()
            
            await query.edit_message_text(
                f"✅ Анализ/исследование *{analysis.name}* удален",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Список анализов/исследований", callback_data="list_analyses")],
                    [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            
            reminder_logger.info(f"ANALYSIS - Удален анализ {analysis_id}")
    
    finally:
        db.close()

# ============== ОБРАБОТЧИКИ САМОЧУВСТВИЯ ==============
async def mood_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оценка самочувствия."""
    text = "📊 *Как вы себя чувствуете сегодня?*\n\nОцените по 5-балльной шкале:"
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=get_mood_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=get_mood_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

async def mood_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка оценки самочувствия."""
    query = update.callback_query
    await query.answer()
    
    mood_score = int(query.data.replace("mood_", ""))
    user_id = update.effective_user.id
    
    db = get_db()
    try:
        mood_log = MoodLog(
            user_id=user_id,
            mood_score=mood_score
        )
        db.add(mood_log)
        db.commit()
        
        recent_moods = db.query(MoodLog).filter(
            MoodLog.user_id == user_id
        ).order_by(MoodLog.created_at.desc()).limit(2).all()
        
        if len(recent_moods) == 2:
            if all(m.mood_score <= 2 for m in recent_moods):
                warning_text = """⚠️ *Внимание!*

Зафиксировано ухудшение самочувствия два дня подряд.

Рекомендуется обратиться к врачу."""
                
                keyboard = [
                    [
                        InlineKeyboardButton("👨‍⚕️ Записаться", callback_data="about"),
                    ],
                    [InlineKeyboardButton("✅ Отметить визит", callback_data="doctor_visited")],
                ]
                
                await rate_limiter.acquire(user_id)
                await context.bot.send_message(
                    chat_id=user_id,
                    text=warning_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
        
        mood_texts = {
            1: "😢 Очень плохо. Берегите себя!",
            2: "🙁 Плохо. Надеюсь, скоро станет лучше!",
            3: "😐 Нормально. Это уже хорошо!",
            4: "🙂 Хорошо! Отличное настроение!",
            5: "😊 Отлично! Так держать!"
        }
        
        keyboard = [
            [InlineKeyboardButton("🩺 Отметить симптомы", callback_data="symptoms")],
            [InlineKeyboardButton("🔙 Главная", callback_data="start")]
        ]
        
        await query.edit_message_text(
            f"✅ {mood_texts[mood_score]}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
    finally:
        db.close()

async def symptoms_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отслеживание симптомов."""
    text = "🩺 *Какие симптомы вас беспокоят?*\n\nВведите симптом текстом:"
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=get_navigation_keyboard("mood"),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=get_navigation_keyboard("mood"),
            parse_mode=ParseMode.MARKDOWN
        )
    
    return SYMPTOM_TEXT

async def symptom_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получение текста симптома."""
    context.user_data['symptom'] = update.message.text
    
    await update.message.reply_text(
        "🩺 *Оцените тяжесть симптома*\n\n"
        "Шкала тяжести (возрастание от 1 до 5):\n"
        "1 - Минимальная\n"
        "2 - Легкая\n"
        "3 - Умеренная\n"
        "4 - Сильная\n"
        "5 - Максимальная",
        reply_markup=get_symptom_severity_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    
    return SYMPTOM_SEVERITY

async def symptom_severity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка тяжести симптома."""
    query = update.callback_query
    await query.answer()
    
    severity = int(query.data.replace("severity_", ""))
    symptom = context.user_data.get('symptom', 'Не указан')
    user_id = update.effective_user.id
    
    db = get_db()
    try:
        symptom_log = SymptomLog(
            user_id=user_id,
            symptom=symptom,
            severity=severity
        )
        db.add(symptom_log)
        db.commit()
        
        severity_texts = {
            1: "1️⃣ Минимальная (🔴)",
            2: "2️⃣ Легкая (🟠)",
            3: "3️⃣ Умеренная (🟡)",
            4: "4️⃣ Сильная (🟢)",
            5: "5️⃣ Максимальная (🔵)"
        }
        
        await query.edit_message_text(
            f"✅ *Симптом зафиксирован:*\n\n"
            f"🤒 {symptom}\n"
            f"📊 {severity_texts[severity]}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Добавить еще симптом", callback_data="symptoms")],
                [InlineKeyboardButton("🔙 Главная", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
    finally:
        db.close()
        del context.user_data['symptom']
    
    return ConversationHandler.END

# ============== ОБРАБОТЧИКИ НАПОМИНАНИЙ ==============
async def medicine_take(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отметка о приеме лекарства."""
    query = update.callback_query
    await query.answer()
    
    medicine_id = int(query.data.replace("take_", ""))
    user_id = update.effective_user.id
    
    db = get_db()
    try:
        log = MedicineLog(
            medicine_id=medicine_id,
            user_id=user_id,
            status='taken'
        )
        db.add(log)
        
        reminder = db.query(Reminder).filter(
            Reminder.item_id == medicine_id,
            Reminder.reminder_type == 'medicine',
            Reminder.status == 'sent'
        ).order_by(Reminder.scheduled_time.desc()).first()
        
        if reminder:
            reminder.status = 'completed'
        
        db.commit()
        
        await query.edit_message_text(
            "✅ *Отлично!*\n\nПрием лекарства отмечен.",
            reply_markup=get_navigation_keyboard("list_medicines"),
            parse_mode=ParseMode.MARKDOWN
        )
        
    finally:
        db.close()

async def medicine_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пропуск приема лекарства."""
    query = update.callback_query
    await query.answer()
    
    medicine_id = int(query.data.replace("skip_", ""))
    user_id = update.effective_user.id
    
    db = get_db()
    try:
        log = MedicineLog(
            medicine_id=medicine_id,
            user_id=user_id,
            status='skipped'
        )
        db.add(log)
        
        reminder = db.query(Reminder).filter(
            Reminder.item_id == medicine_id,
            Reminder.reminder_type == 'medicine',
            Reminder.status == 'sent'
        ).order_by(Reminder.scheduled_time.desc()).first()
        
        if reminder:
            reminder.status = 'skipped'
        
        db.commit()
        
        await query.edit_message_text(
            "❌ *Прием пропущен*",
            reply_markup=get_navigation_keyboard("list_medicines"),
            parse_mode=ParseMode.MARKDOWN
        )
        
    finally:
        db.close()

async def analysis_take(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отметка о сдаче анализа/исследования."""
    query = update.callback_query
    await query.answer()
    
    analysis_id = int(query.data.replace("analysis_take_", ""))
    user_id = update.effective_user.id
    
    db = get_db()
    try:
        log = AnalysisLog(
            analysis_id=analysis_id,
            user_id=user_id,
            status='completed'
        )
        db.add(log)
        
        analysis = db.query(Analysis).filter_by(id=analysis_id).first()
        if analysis:
            analysis.status = 'completed'
        
        reminder = db.query(Reminder).filter(
            Reminder.item_id == analysis_id,
            Reminder.reminder_type == 'analysis',
            Reminder.status == 'sent'
        ).order_by(Reminder.scheduled_time.desc()).first()
        
        if reminder:
            reminder.status = 'completed'
        
        db.commit()
        
        await query.edit_message_text(
            "✅ *Отлично!*\n\nСдача анализа/исследования отмечена.",
            reply_markup=get_navigation_keyboard("list_analyses"),
            parse_mode=ParseMode.MARKDOWN
        )
        
    finally:
        db.close()

async def analysis_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пропуск анализа/исследования."""
    query = update.callback_query
    await query.answer()
    
    analysis_id = int(query.data.replace("analysis_skip_", ""))
    user_id = update.effective_user.id
    
    db = get_db()
    try:
        log = AnalysisLog(
            analysis_id=analysis_id,
            user_id=user_id,
            status='skipped'
        )
        db.add(log)
        
        analysis = db.query(Analysis).filter_by(id=analysis_id).first()
        if analysis:
            analysis.status = 'skipped'
        
        reminder = db.query(Reminder).filter(
            Reminder.item_id == analysis_id,
            Reminder.reminder_type == 'analysis',
            Reminder.status == 'sent'
        ).order_by(Reminder.scheduled_time.desc()).first()
        
        if reminder:
            reminder.status = 'skipped'
        
        db.commit()
        
        await query.edit_message_text(
            "❌ *Анализ/исследование пропущен*",
            reply_markup=get_navigation_keyboard("list_analyses"),
            parse_mode=ParseMode.MARKDOWN
        )
        
    finally:
        db.close()

# ============== ОБРАБОТЧИКИ ЧАСОВЫХ ПОЯСОВ ==============
async def timezone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Установка часового пояса."""
    query = update.callback_query
    await query.answer()
    
    tz_name = query.data.replace("tz_", "")
    user_id = update.effective_user.id
    
    set_user_timezone(user_id, tz_name)
    
    await query.edit_message_text(
        f"✅ *Часовой пояс установлен*\n\n"
        f"Ваш часовой пояс: *{tz_name}*",
        reply_markup=get_navigation_keyboard("help"),
        parse_mode=ParseMode.MARKDOWN
    )

# ============== ОБРАБОТЧИКИ КНОПОК ==============
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Общий обработчик callback запросов."""
    query = update.callback_query
    data = query.data
    
    if data == "start":
        await start_callback(update, context)
    elif data == "back":
        await start_callback(update, context)
    elif data == "help":
        await help_command(update, context)
    elif data == "help_clear":
        await query.answer()
        await help_command(update, context)
    elif data == "about":
        await about_command(update, context)
    elif data == "stats":
        await stats_command(update, context)
    elif data.startswith("stats_"):
        await stats_callback(update, context)
    elif data == "add_medicine":
        await add_medicine_start(update, context)
    elif data == "add_analysis":
        await add_analysis_start(update, context)
    elif data == "list_medicines":
        await list_medicines(update, context)
    elif data == "list_analyses":
        await list_analyses(update, context)
    elif data.startswith("delete_medicine_"):
        await delete_medicine(update, context)
    elif data.startswith("delete_analysis_"):
        await delete_analysis(update, context)
    elif data == "mood":
        await mood_command(update, context)
    elif data.startswith("mood_"):
        await mood_callback(update, context)
    elif data == "symptoms":
        await symptoms_command(update, context)
    elif data.startswith("severity_"):
        await symptom_severity(update, context)
    elif data.startswith("take_"):
        await medicine_take(update, context)
    elif data.startswith("skip_"):
        await medicine_skip(update, context)
    elif data.startswith("analysis_take_"):
        await analysis_take(update, context)
    elif data.startswith("analysis_skip_"):
        await analysis_skip(update, context)
    elif data == "set_timezone":
        await set_timezone_command(update, context)
    elif data.startswith("tz_"):
        await timezone_callback(update, context)
    elif data == "noop":
        await query.answer("Это информационная кнопка")
    elif data.startswith("time_"):
        if context.user_data.get('medicine_data'):
            await add_medicine_time_callback(update, context)
        else:
            await add_analysis_time(update, context)
    elif data.startswith("course_"):
        if data == "course_days" or data == "course_months" or data == "course_unlimited":
            await add_medicine_course_type(update, context)
        else:
            await add_medicine_course_days(update, context)
    elif data.startswith("repeat_"):
        if context.user_data.get('medicine_data'):
            await add_medicine_repeat(update, context)
        else:
            await add_analysis_repeat(update, context)
    elif data.startswith("start_"):
        await add_medicine_start_date(update, context)
    elif data == "confirm_medicine":
        await add_medicine_confirm(update, context)
    elif data.startswith("analysis_date_"):
        await add_analysis_date(update, context)
    elif data == "analysis_date_back":
        await add_analysis_start(update, context)
    elif data == "analysis_time_back":
        await add_analysis_date(update, context)
    elif data == "analysis_repeat_back":
        await add_analysis_time(update, context)
    elif data == "analysis_reminder_back":
        await add_analysis_repeat(update, context)
    elif data.startswith("remind_"):
        await add_analysis_reminder(update, context)
    elif data == "confirm_analysis":
        await add_analysis_confirm(update, context)
    elif data == "phone_kit":
        await query.answer()
        await context.bot.send_message(
            chat_id=update.effective_user.id,
            text=f"📞 Телефон КИТ-клиники: {KIT_CLINIC['phone_display']}\n\nНажмите на номер чтобы позвонить: {KIT_CLINIC['phone']}",
            reply_markup=get_navigation_keyboard("about")
        )
    elif data == "phone_family":
        await query.answer()
        await context.bot.send_message(
            chat_id=update.effective_user.id,
            text=f"📞 Телефон Семейной клиники: {FAMILY_CLINIC['phone_display']}\n\nНажмите на номер чтобы позвонить: {FAMILY_CLINIC['phone']}",
            reply_markup=get_navigation_keyboard("about")
        )
    else:
        await query.answer("Функция в разработке")

async def start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат на стартовую страницу."""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    welcome_text = f"""👋 *Здравствуйте, {user.first_name}!*

Я *ЛОР-Помощник* — персональный медицинский бот, созданный врачом-оториноларингологом Денисом Казариным.

👶 *Врач ведет прием детей с 0 лет и взрослых*

🤖 *Мои возможности:*
• 💊 Напоминания о приеме лекарств
• 🩺 Напоминания об анализах и исследованиях
• 📊 Отслеживание самочувствия
• 📈 Статистика и отчеты для врача

Начните с добавления лекарства или анализа/исследования!"""
    
    await query.edit_message_text(
        welcome_text,
        reply_markup=get_start_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )

# ============== ЕЖЕДНЕВНЫЙ ОПРОС ==============
async def daily_mood_check(context: ContextTypes.DEFAULT_TYPE):
    """Ежедневный опрос о самочувствии в 21:00."""
    db = get_db()
    try:
        users = db.query(UserTimezone).all()
        
        for user in users:
            try:
                user_tz = pytz.timezone(user.timezone)
                now_local = datetime.now(user_tz)
                
                if 20 <= now_local.hour <= 22:
                    text = "📊 *Как вы себя чувствуете сегодня?*\n\nОцените свое самочувствие по 5-балльной шкале:"
                    
                    await rate_limiter.acquire(user.user_id)
                    await context.bot.send_message(
                        chat_id=user.user_id,
                        text=text,
                        reply_markup=get_mood_keyboard(),
                        parse_mode=ParseMode.MARKDOWN
                    )
            except Exception as e:
                reminder_logger.error(f"DAILY MOOD ERROR for user {user.user_id}: {e}")
                
    finally:
        db.close()

# ============== ИНИЦИАЛИЗАЦИЯ ПРИЛОЖЕНИЯ ==============
def create_application():
    """Создание и настройка приложения."""
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.scheduler = scheduler.scheduler
    
    medicine_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("add_medicine", add_medicine_start),
            CallbackQueryHandler(add_medicine_start, pattern="^add_medicine$")
        ],
        states={
            MEDICINE_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_medicine_name)
            ],
            MEDICINE_TIME: [
                CallbackQueryHandler(add_medicine_time_callback, pattern="^time_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_medicine_time_callback)
            ],
            MEDICINE_COURSE_TYPE: [
                CallbackQueryHandler(add_medicine_course_type, pattern="^course_")
            ],
            MEDICINE_COURSE_DAYS: [
                CallbackQueryHandler(add_medicine_course_days, pattern="^course_days_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_medicine_course_days)
            ],
            MEDICINE_REPEAT: [
                CallbackQueryHandler(add_medicine_repeat, pattern="^repeat_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_medicine_repeat)
            ],
            MEDICINE_START_DATE: [
                CallbackQueryHandler(add_medicine_start_date, pattern="^start_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_medicine_start_date)
            ],
            MEDICINE_CONFIRM: [
                CallbackQueryHandler(add_medicine_confirm, pattern="^confirm_medicine$")
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel, pattern="^cancel$"),
            CallbackQueryHandler(start_callback, pattern="^start$")
        ],
        name="add_medicine",
        persistent=False
    )
    
    analysis_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("add_analysis", add_analysis_start),
            CallbackQueryHandler(add_analysis_start, pattern="^add_analysis$")
        ],
        states={
            ANALYSIS_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_analysis_name)
            ],
            ANALYSIS_DATE: [
                CallbackQueryHandler(add_analysis_date, pattern="^analysis_date_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_analysis_date)
            ],
            ANALYSIS_TIME: [
                CallbackQueryHandler(add_analysis_time, pattern="^time_"),
                CallbackQueryHandler(add_analysis_time, pattern="^analysis_time_back$"),
                CallbackQueryHandler(add_analysis_time, pattern="^time_minute_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_analysis_time)
            ],
            ANALYSIS_REPEAT: [
                CallbackQueryHandler(add_analysis_repeat, pattern="^repeat_"),
                CallbackQueryHandler(add_analysis_repeat, pattern="^analysis_repeat_back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_analysis_repeat)
            ],
            ANALYSIS_REMINDER: [
                CallbackQueryHandler(add_analysis_reminder, pattern="^remind_"),
                CallbackQueryHandler(add_analysis_reminder, pattern="^analysis_reminder_back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_analysis_reminder)
            ],
            ANALYSIS_NOTES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_analysis_notes),
                CommandHandler("skip", skip_notes)
            ],
            ANALYSIS_CONFIRM: [
                CallbackQueryHandler(add_analysis_confirm, pattern="^confirm_analysis$")
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel, pattern="^cancel$"),
            CallbackQueryHandler(start_callback, pattern="^start$")
        ],
        name="add_analysis",
        persistent=False
    )
    
    symptom_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("symptoms", symptoms_command),
            CallbackQueryHandler(symptoms_command, pattern="^symptoms$")
        ],
        states={
            SYMPTOM_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, symptom_text)
            ],
            SYMPTOM_SEVERITY: [
                CallbackQueryHandler(symptom_severity, pattern="^severity_")
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel, pattern="^cancel$"),
            CallbackQueryHandler(mood_command, pattern="^mood$")
        ],
        name="add_symptom",
        persistent=False
    )
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("about", about_command))
    app.add_handler(CommandHandler("settimezone", set_timezone_command))
    app.add_handler(CommandHandler("mood", mood_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("list", list_medicines))
    app.add_handler(CommandHandler("list_medicines", list_medicines))
    app.add_handler(CommandHandler("list_analyses", list_analyses))
    
    app.add_handler(medicine_conv_handler)
    app.add_handler(analysis_conv_handler)
    app.add_handler(symptom_conv_handler)
    
    app.add_handler(CallbackQueryHandler(button_callback))
    
    app.job_queue.run_repeating(
        integrity_check,
        interval=3600,
        first=10,
        name="integrity_check"
    )
    
    app.job_queue.run_daily(
        daily_mood_check,
        time=datetime.strptime("21:00", "%H:%M").time(),
        name="daily_mood_check"
    )
    
    return app

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена операции."""
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "❌ Операция отменена",
            reply_markup=get_start_keyboard()
        )
    else:
        await update.message.reply_text(
            "❌ Операция отменена",
            reply_markup=get_start_keyboard()
        )
    return ConversationHandler.END

# ============== ЗАПУСК БОТА ==============
async def main():
    """Главная функция запуска."""
    global application
    
    if BOT_TOKEN == "ВАШ_ТОКЕН_ЗДЕСЬ":
        print("\n" + "="*50)
        print("⚠️  ВНИМАНИЕ! Необходимо установить токен бота!")
        print("="*50)
        return
    
    print("🚀 Запуск ЛОР-Помощника...")
    print("📊 Версия: 5.0.2 (Финальная стабильная)")
    print("⏰ Часовой пояс: UTC (все времена в БД)")
    print("💾 Job store: SQLAlchemyJobStore (persistent)")
    print("🔄 Retry: 3 попытки")
    print("🚦 Rate limit: 30/сек глобально, 1/сек на пользователя")
    print("🛡️ Integrity check: каждый час")
    print("-" * 50)
    
    print("🔄 Отключаем webhook...")
    import requests
    try:
        response = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook")
        print(f"✅ Webhook отключен: {response.json()}")
    except Exception as e:
        print(f"⚠️ Ошибка при отключении webhook: {e}")
    
    application = create_application()
    
    scheduler.start()
    
    await scheduler.restore_reminders()
    
    print("✅ Бот запущен и готов к работе!")
    print("📝 Логи пишутся в reminders.log")
    print("📡 Режим: Long Polling")
    print("💡 Отправьте /start в Telegram: @LorPomoshnikBot")
    print("⏎ Нажмите Ctrl+C для остановки")
    
    # ИСПРАВЛЕНИЕ: Используем правильный метод запуска без run_polling
    await application.initialize()
    await application.start()
    
    # Запускаем polling
    await application.updater.start_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )
    
    # Бесконечное ожидание с обработкой сигналов
    stop_signal = asyncio.Future()
    
    # Обработка сигналов для graceful shutdown
    def signal_handler():
        if not stop_signal.done():
            stop_signal.set_result(None)
    
    # Регистрируем обработчики сигналов (только не в Windows)
    if os.name != 'nt':
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler)
    
    try:
        await stop_signal
    except KeyboardInterrupt:
        print("\n\n🛑 Получен сигнал остановки")
    finally:
        print("🛑 Завершаем работу...")
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        if scheduler:
            scheduler.shutdown()
        reminder_logger.info("SHUTDOWN - Бот остановлен корректно")

# ============== ТОЧКА ВХОДА ==============
if __name__ == "__main__":
    # Проверяем, не запущен ли уже цикл событий
    try:
        loop = asyncio.get_running_loop()
        print("⚠️ Цикл событий уже запущен, создаем задачу...")
        # Если цикл уже запущен, создаем задачу
        loop.create_task(main())
        # Держим цикл запущенным
        if not loop.is_running():
            loop.run_forever()
    except RuntimeError:
        # Цикла нет, запускаем новый
        print("🚀 Запускаем новый цикл событий...")
        asyncio.run(main())
