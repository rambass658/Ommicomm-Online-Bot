#!/usr/bin/env python3
import asyncio
import json
import logging
import re
import io
import csv
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional, List, Any, Union

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

from omnicomm.client import OmnicommClient
import config

# STATS: импортируем модуль статистики
import stats

# ===== НАСТРОЙКА ЛОГИРОВАНИЯ =====
from logging.handlers import RotatingFileHandler
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            "data/bot.log",
            maxBytes=10*1024*1024,   # 10 МБ
            backupCount=5,
            encoding="utf-8"
        )
    ]
)
logger = logging.getLogger(__name__)

bot = Bot(token=config.TG_BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ===== КОНСТАНТЫ =====
VEHICLES_DB_FILE = "data/vehicles_db.json"

# ===== СОСТОЯНИЯ ДЛЯ FSM =====
class VehicleSearch(StatesGroup):
    waiting_for_find_query = State()      # ожидание номера для поиска
    waiting_for_state_query = State()     # ожидание госномера или ID для состояния

# ===== ФУНКЦИЯ ПРОВЕРКИ ДОСТУПА =====
async def check_access(obj: Union[Message, CallbackQuery]) -> bool:
    """Проверяет, имеет ли пользователь доступ к боту. Админ всегда пропускается."""
    user_id = obj.from_user.id
    if user_id in config.ADMIN_IDS:
        return True
    if not stats.is_activated(user_id):
        if isinstance(obj, Message):
            await obj.answer(
                "⛔ <b>Доступ запрещён</b>\n\n"
                "Для использования бота необходим ключ активации.\n"
                "Введите /activate [ключ] для активации.\n"
                "Подробнее: /help",
                parse_mode=ParseMode.HTML
            )
        else:  # CallbackQuery
            await obj.message.answer(
                "⛔ <b>Доступ запрещён</b>\n\n"
                "Для использования бота необходим ключ активации.\n"
                "Введите /activate [ключ] для активации.\n"
                "Подробнее: /help",
                parse_mode=ParseMode.HTML
            )
            await obj.answer()
        return False
    return True

# ===== ЗАГРУЗКА БАЗЫ ДАННЫХ ТС =====
def load_vehicles_db() -> Tuple[Dict, Dict]:
    try:
        with open(VEHICLES_DB_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"✅ База ТС загружена: {len(data['index'])} записей, {len(data['details'])} ТС")
        return data.get('index', {}), data.get('details', {})
    except FileNotFoundError:
        logger.warning("⚠️ Файл vehicles_db.json не найден. Поиск по госномеру работать не будет.")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки базы: {e}")
    return {}, {}

VEHICLE_INDEX, VEHICLE_DETAILS = load_vehicles_db()

def normalize_query(query: str) -> str:
    if not query:
        return ""
    return re.sub(r'\s+', '', query).upper()

def find_terminal_id(identifier: str) -> Optional[str]:
    norm = normalize_query(identifier)
    return VEHICLE_INDEX.get(norm)

# ===== КЛАВИАТУРЫ =====
def main_menu_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Найти ТС", callback_data="menu_find")],
        [InlineKeyboardButton(text="📍 Состояние ТС", callback_data="menu_state")],
        [InlineKeyboardButton(text="📊 Отчёт по оборотам", callback_data="menu_rpm")],
        [InlineKeyboardButton(text="❓ Помощь", callback_data="menu_help")]
    ])
    return keyboard

def cancel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    return keyboard

def state_button_keyboard(terminal_id: str) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📍 Состояние", callback_data=f"state_{terminal_id}"),
            InlineKeyboardButton(text="📊 Отчёт (7д)", callback_data=f"rpm_{terminal_id}")
        ],
        [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="menu_find")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_menu")]
    ])
    return keyboard

def period_keyboard(callback_prefix: str) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🌙 1 день", callback_data=f"{callback_prefix}_1"),
            InlineKeyboardButton(text="📆 7 дней", callback_data=f"{callback_prefix}_7"),
            InlineKeyboardButton(text="🗓️ 30 дней", callback_data=f"{callback_prefix}_30")
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    return keyboard

# ===== КОМАНДА START =====
@router.message(CommandStart())
async def start(msg: Message):
    stats.log_command(msg.from_user.id, msg.from_user.username, "start", msg.text)
    if msg.from_user.id in config.ADMIN_IDS or stats.is_activated(msg.from_user.id):
        await msg.answer(
            "🚛 <b>Omnicomm Bot — Мониторинг транспорта</b>\n\n"
            "Выберите действие:",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard()
        )
    else:
        await msg.answer(
            "🚛 <b>Omnicomm Bot</b>\n\n"
            "Этот бот требует активации.\n"
            "Введите /activate [ключ] для получения доступа.\n"
            "Подробнее: /help",
            parse_mode=ParseMode.HTML
        )

# ===== КОМАНДА АКТИВАЦИИ =====
@router.message(Command("activate"))
async def activate_cmd(msg: Message):
    stats.log_command(msg.from_user.id, msg.from_user.username, "activate", msg.text)
    args = msg.text.split()
    if len(args) < 2:
        await msg.answer("❌ Использование: /activate [ключ]")
        return
    key = args[1].strip()
    if stats.activate_user(msg.from_user.id, key):
        await msg.answer(
            "✅ <b>Доступ активирован!</b>\n\n"
            "Теперь вы можете пользоваться ботом.\n"
            "/start — главное меню",
            parse_mode=ParseMode.HTML
        )
    else:
        await msg.answer("❌ Неверный или уже использованный ключ.")

# ===== АДМИНСКИЕ КОМАНДЫ =====
@router.message(Command("genkey"))
async def genkey_cmd(msg: Message):
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.answer("⛔ У вас нет прав на эту команду.")
        return
    key = stats.generate_key()
    stats.add_key(key)
    await msg.answer(f"✅ Сгенерирован новый ключ:\n<code>{key}</code>", parse_mode=ParseMode.HTML)

@router.message(Command("keys"))
async def keys_cmd(msg: Message):
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.answer("⛔ У вас нет прав на эту команду.")
        return
    keys = stats.get_all_keys()
    if not keys:
        await msg.answer("📭 Нет ключей в базе.")
        return
    lines = ["🔑 <b>Список ключей:</b>\n"]
    for k in keys:
        status = "✅ использован" if k['used'] else "🆕 свободен"
        used_by = f" (пользователь {k['used_by']})" if k['used_by'] else ""
        lines.append(f"<code>{k['key']}</code> — {status}{used_by}")
    await msg.answer("\n".join(lines), parse_mode=ParseMode.HTML)

@router.message(Command("delkey"))
async def delkey_cmd(msg: Message):
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.answer("⛔ У вас нет прав на эту команду.")
        return
    args = msg.text.split()
    if len(args) < 2:
        await msg.answer("❌ Использование: /delkey [ключ]")
        return
    key = args[1].strip()
    if stats.delete_key(key):
        await msg.answer(f"✅ Ключ {key} удалён.")
    else:
        await msg.answer(f"❌ Ключ {key} не найден.")

# ===== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК =====
@router.callback_query(F.data == "menu_find")
async def menu_find(callback: CallbackQuery, state: FSMContext):
    if not await check_access(callback):
        await callback.answer()
        return
    stats.log_command(callback.from_user.id, callback.from_user.username, "callback_menu_find")
    if not VEHICLE_INDEX:
        await callback.message.edit_text("⚠️ База ТС не загружена.", reply_markup=None)
        await callback.answer()
        return
    await callback.message.edit_text(
        "🔍 Введите госномер, гаражный номер, VIN или ID для поиска:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(VehicleSearch.waiting_for_find_query)
    await callback.answer()

@router.callback_query(F.data == "menu_state")
async def menu_state(callback: CallbackQuery, state: FSMContext):
    if not await check_access(callback):
        await callback.answer()
        return
    stats.log_command(callback.from_user.id, callback.from_user.username, "callback_menu_state")
    await callback.message.edit_text(
        "📍 Введите госномер или ID транспортного средства:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(VehicleSearch.waiting_for_state_query)
    await callback.answer()

@router.callback_query(F.data == "menu_rpm")
async def menu_rpm(callback: CallbackQuery):
    if not await check_access(callback):
        await callback.answer()
        return
    stats.log_command(callback.from_user.id, callback.from_user.username, "callback_menu_rpm")
    await callback.message.edit_text(
        "📊 Выберите период для отчёта по всем ТС:",
        reply_markup=period_keyboard("rpm_all")
    )
    await callback.answer()

@router.callback_query(F.data == "menu_help")
async def menu_help(callback: CallbackQuery):
    # Помощь доступна без активации
    stats.log_command(callback.from_user.id, callback.from_user.username, "callback_menu_help")
    await callback.message.edit_text(
        "📋 <b>Доступные команды:</b>\n\n"
        "/start — главное меню\n"
        "/state [госномер или ID] — состояние ТС\n"
        "/find [номер] — поиск ТС по номеру\n"
        "/rpm_report [дни] — отчёт по оборотам (по умолч. 30 дней)\n\n"
        "🔑 <b>Как получить доступ?</b>\n"
        "Отправьте /activate [ключ], если у вас есть ключ активации.\n"
        "Для получения ключа обратитесь к администратору.\n\n"
        "<b>Примеры:</b>\n"
        "/state 2700РВ78\n"
        "/find 10039\n"
        "/rpm_report 7",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
        ])
    )
    await callback.answer()

@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery):
    if not await check_access(callback):
        await callback.answer()
        return
    stats.log_command(callback.from_user.id, callback.from_user.username, "callback_back_to_menu")
    await callback.message.edit_text(
        "🚛 Главное меню. Выберите действие:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "cancel")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    if not await check_access(callback):
        await callback.answer()
        return
    stats.log_command(callback.from_user.id, callback.from_user.username, "callback_cancel")
    await state.clear()
    await callback.message.edit_text(
        "🚛 Действие отменено. Главное меню:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

# ===== ОБРАБОТЧИК ДЛЯ КНОПКИ СОСТОЯНИЯ =====
@router.callback_query(lambda c: c.data and c.data.startswith("state_"))
async def callback_state(callback: CallbackQuery):
    if not await check_access(callback):
        await callback.answer()
        return
    terminal_id = callback.data.replace("state_", "")
    stats.log_command(callback.from_user.id, callback.from_user.username, "callback_state", terminal_id)

    await callback.answer("🔍 Запрашиваю состояние...")
    processing_msg = await callback.message.answer(f"🔍 Запрашиваю состояние ТС ID: {terminal_id}...")

    try:
        client: OmnicommClient = callback.bot.omnicomm_client
        state_data = await client.get_vehicle_state(terminal_id)
        response = format_vehicle_state(state_data, terminal_id)
        await processing_msg.delete()
        await callback.message.answer(response, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
    except Exception as exc:
        stats.log_error(callback.from_user.id, "callback_state", exc)
        await processing_msg.delete()
        await callback.message.answer(f"❌ Ошибка: {str(exc)[:200]}", reply_markup=main_menu_keyboard())

# ===== ОБРАБОТЧИКИ ДЛЯ ОТЧЁТОВ =====
@router.callback_query(F.data.startswith("rpm_all_"))
async def rpm_all_period(callback: CallbackQuery):
    if not await check_access(callback):
        await callback.answer()
        return
    days = int(callback.data.split("_")[2])
    stats.log_command(callback.from_user.id, callback.from_user.username, f"rpm_all_{days}")
    await callback.answer(f"⏳ Формирую отчёт за {days} дн., это может занять время...")
    vehicle_ids = [int(tid) for tid in set(VEHICLE_INDEX.values())]
    client: OmnicommClient = callback.bot.omnicomm_client
    await generate_and_send_rpm_report(
        callback.message,
        client,
        vehicle_ids=vehicle_ids,
        days=days,
        period_name=f"{days} дн."
    )

@router.callback_query(F.data.startswith("rpm_") & ~F.data.startswith("rpm_all"))
async def rpm_single(callback: CallbackQuery):
    if not await check_access(callback):
        await callback.answer()
        return
    terminal_id = callback.data.replace("rpm_", "")
    stats.log_command(callback.from_user.id, callback.from_user.username, f"rpm_single_{terminal_id}")
    await callback.answer(f"⏳ Формирую отчёт для ТС {terminal_id} за 7 дней...")
    client: OmnicommClient = callback.bot.omnicomm_client
    await generate_and_send_rpm_report(
        callback.message,
        client,
        vehicle_ids=[int(terminal_id)],
        days=7,
        period_name="7 дн.",
        single=True
    )

# ===== ОБРАБОТЧИКИ ВВОДА ДЛЯ FSM =====
@router.message(VehicleSearch.waiting_for_find_query)
async def process_find_query(msg: Message, state: FSMContext):
    if not await check_access(msg):
        return
    query = msg.text.strip()
    await state.clear()
    stats.log_command(msg.from_user.id, msg.from_user.username, "fsm_find", query)

    if not query:
        await msg.answer("❌ Введите хотя бы один символ.", reply_markup=main_menu_keyboard())
        return

    norm_query = normalize_query(query)
    if len(norm_query) < 2:
        await msg.answer("❌ Слишком короткий запрос. Минимум 2 символа.", reply_markup=main_menu_keyboard())
        return

    terminal_id = VEHICLE_INDEX.get(norm_query)
    if terminal_id:
        details = VEHICLE_DETAILS.get(terminal_id, {})
        plate = details.get('plate', 'не указан')
        name = details.get('name', '')
        brand = details.get('brand', '')
        model = details.get('model', '')
        response = (
            f"✅ <b>ТС найдено!</b>\n\n"
            f"<b>ID терминала:</b> <code>{terminal_id}</code>\n"
            f"<b>Госномер:</b> {plate}\n"
            f"<b>Название:</b> {name}\n"
            f"<b>Марка/модель:</b> {brand} {model}"
        )
        await msg.answer(response, parse_mode=ParseMode.HTML, reply_markup=state_button_keyboard(terminal_id))
        return

    matches = []
    seen_ids = set()
    for key, tid in VEHICLE_INDEX.items():
        if norm_query in key:
            if tid not in seen_ids:
                seen_ids.add(tid)
                matches.append({'id': tid, 'key': key})
        if len(matches) >= 10:
            break

    if not matches:
        await msg.answer(f"❌ По запросу '{query}' ничего не найдено.", reply_markup=main_menu_keyboard())
        return

    lines = [f"🔍 <b>Найдено по запросу '{query}':</b>", ""]
    for i, m in enumerate(matches, 1):
        details = VEHICLE_DETAILS.get(m['id'], {})
        plate = details.get('plate', '')
        name = details.get('name', '')
        if plate:
            lines.append(f"{i}. <b>{plate}</b>")
        else:
            lines.append(f"{i}. <b>{m['key']}</b>")
        lines.append(f"   ID: <code>{m['id']}</code>")
        if name:
            lines.append(f"   {name[:50]}")
        lines.append("")

    if len(matches) == 10:
        lines.append("<i>Показаны первые 10 результатов. Уточните запрос.</i>")

    keyboard_rows = []
    for m in matches:
        details = VEHICLE_DETAILS.get(m['id'], {})
        plate = details.get('plate', '')
        if plate:
            button_text = f"📍 {plate}"
        else:
            short_key = m['key'][:8]
            button_text = f"📍 {short_key}..."
        keyboard_rows.append([InlineKeyboardButton(text=button_text, callback_data=f"state_{m['id']}")])

    keyboard_rows.append([InlineKeyboardButton(text="🔍 Новый поиск", callback_data="menu_find")])
    keyboard_rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_menu")])
    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)

    await msg.answer("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=reply_markup)

@router.message(VehicleSearch.waiting_for_state_query)
async def process_state_query(msg: Message, state: FSMContext):
    if not await check_access(msg):
        return
    identifier = msg.text.strip()
    await state.clear()
    stats.log_command(msg.from_user.id, msg.from_user.username, "fsm_state", identifier)

    if not identifier:
        await msg.answer("❌ Введите ID или госномер.", reply_markup=main_menu_keyboard())
        return

    terminal_id = None
    if identifier.isdigit():
        terminal_id = identifier
    else:
        terminal_id = find_terminal_id(identifier)
        if not terminal_id:
            await msg.answer(
                f"❌ ТС с номером '{identifier}' не найдено в базе.\nПопробуйте /find для поиска.",
                reply_markup=main_menu_keyboard()
            )
            return

    processing_msg = await msg.answer(f"🔍 Запрашиваю состояние ТС ID: {terminal_id}...")
    try:
        client: OmnicommClient = msg.bot.omnicomm_client
        state_data = await client.get_vehicle_state(terminal_id)
        response = format_vehicle_state(state_data, terminal_id)
        await processing_msg.delete()
        await msg.answer(response, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
    except Exception as exc:
        stats.log_error(msg.from_user.id, "fsm_state", exc)
        await processing_msg.delete()
        await msg.answer(f"❌ Ошибка: {str(exc)[:200]}", reply_markup=main_menu_keyboard())

# ===== ФОРМАТИРОВАНИЕ СОСТОЯНИЯ =====
def format_vehicle_state(data: dict, vehicle_id: str) -> str:
    if not isinstance(data, dict):
        return f"⚠️ Неожиданный формат данных: {str(data)[:500]}"
    lines = [f"🚚 <b>Состояние ТС (ID: {vehicle_id})</b>", ""]
    status = data.get('status')
    if status is True:
        status_text = "✅ <b>Статус:</b> Активно"
    elif status is False:
        status_text = "❌ <b>Статус:</b> Неактивно"
    else:
        if data.get('lastDataDate') and data.get('currentSpeed') is not None:
            status_text = "✅ <b>Статус:</b> Активно (есть данные)"
        else:
            status_text = "❓ <b>Статус:</b> Нет данных"
    lines.append(status_text)
    address = data.get('address')
    if address:
        lines.append(f"🏠 <b>Адрес:</b> {address}")
    fuel = data.get('currentFuel')
    if fuel is not None:
        lines.append(f"⛽ <b>Топливо:</b> {fuel} л")
    ignition = data.get('currentIgn')
    if ignition is not None:
        lines.append(f"🔑 <b>Зажигание:</b> {'ВКЛ' if ignition else 'ВЫКЛ'}")
    speed = data.get('currentSpeed')
    if speed is not None:
        lines.append(f"🚗 <b>Скорость:</b> {speed} км/ч")
    last_date = data.get('lastDataDate')
    if last_date:
        try:
            if last_date > 10000000000:
                last_date = last_date / 1000
            dt = datetime.fromtimestamp(last_date, tz=timezone.utc)
            lines.append(f"🕒 <b>Последние данные:</b> {dt.strftime('%d.%m.%Y %H:%M:%S')} UTC")
            delta = datetime.now(timezone.utc) - dt
            if delta.days > 0:
                lines.append(f"   <i>({delta.days} дн. {delta.seconds//3600} ч. назад)</i>")
            elif delta.seconds > 3600:
                lines.append(f"   <i>({delta.seconds//3600} ч. {delta.seconds%3600//60} мин. назад)</i>")
            elif delta.seconds > 60:
                lines.append(f"   <i>({delta.seconds//60} мин. назад)</i>")
            else:
                lines.append(f"   <i>(только что)</i>")
        except Exception:
            lines.append(f"🕒 <b>Последние данные:</b> {last_date}")
    else:
        lines.append(f"🕒 <b>Последние данные:</b> нет данных")
    last_gps = data.get('lastGPS')
    if last_gps and isinstance(last_gps, dict):
        lat = last_gps.get('latitude')
        lon = last_gps.get('longitude')
        if lat is not None and lon is not None:
            lines.append(f"📍 <b>Координаты:</b> {lat:.6f}, {lon:.6f}")
            maps_link = f"https://maps.google.com/?q={lat},{lon}"
            lines.append(f"🗺️ <a href='{maps_link}'>Открыть на карте</a>")
    direction = data.get('lastGPSDir')
    if direction is not None:
        directions = ['С', 'СВ', 'В', 'ЮВ', 'Ю', 'ЮЗ', 'З', 'СЗ']
        idx = round(direction / 45) % 8
        lines.append(f"🧭 <b>Направление:</b> {direction}° ({directions[idx]})")
    satellites = data.get('lastGPSSat')
    if satellites is not None:
        lines.append(f"📡 <b>Спутники:</b> {satellites if satellites>0 else 'нет сигнала'}")
    speed_exceed = data.get('speedExceed')
    if speed_exceed is not None:
        lines.append(f"⚠️ <b>Превышение скорости:</b> {'ДА' if speed_exceed else 'нет'}")
    voltage = data.get('voltage')
    if voltage is not None:
        lines.append(f"🔋 <b>Напряжение:</b> {voltage} В")
    return "\n".join(lines)

# ===== УНИВЕРСАЛЬНАЯ ФУНКЦИЯ ДЛЯ ОТЧЁТА (С ЗАЩИТОЙ СЧЁТЧИКА) =====
async def generate_and_send_rpm_report(
    message: Message,
    client: OmnicommClient,
    vehicle_ids: List[int],
    days: int,
    period_name: str,
    single: bool = False
):
    status_msg = await message.answer(
        f"🔄 Сбор данных об оборотах за {period_name}...\n"
        f"ТС: {'все' if not single else 'одно'}. Это может занять несколько минут."
    )
    total = len(vehicle_ids)
    to_datetime = int(datetime.now().timestamp())
    from_datetime = int((datetime.now() - timedelta(days=days)).timestamp())
    semaphore = asyncio.Semaphore(5)
    processed = 0
    results = []
    counter_lock = asyncio.Lock()

    async def process_one(tid: int) -> Dict:
        nonlocal processed
        async with semaphore:
            try:
                report = await client.get_rpm_report([tid], from_datetime, to_datetime)
                data_points = []
                if report and 'data' in report:
                    if isinstance(report['data'], list):
                        for item in report['data']:
                            if item.get('vehicleId') == tid:
                                data_points = item.get('rpms', [])
                                break
                    elif isinstance(report['data'], dict) and report['data'].get('vehicleId') == tid:
                        data_points = report['data'].get('rpms', [])
                if data_points and isinstance(data_points, list):
                    values = [p['value'] for p in data_points if 'value' in p and isinstance(p['value'], (int, float))]
                    if values:
                        max_rpm = max(values)
                        avg_rpm = sum(values) / len(values)
                        engine_on = max_rpm > 100
                        return {
                            'id': str(tid),
                            'success': True,
                            'has_data': True,
                            'points': len(data_points),
                            'max_rpm': round(max_rpm, 1),
                            'avg_rpm': round(avg_rpm, 1),
                            'engine_on': engine_on,
                            'error': ''
                        }
                return {
                    'id': str(tid),
                    'success': True,
                    'has_data': False,
                    'points': 0,
                    'max_rpm': 0,
                    'avg_rpm': 0,
                    'engine_on': False,
                    'error': ''
                }
            except Exception as e:
                # ИСПРАВЛЕНО: используем logger.exception, чтобы добавить traceback
                logger.exception(f"Ошибка ТС {tid}")
                return {
                    'id': str(tid),
                    'success': False,
                    'has_data': False,
                    'points': 0,
                    'max_rpm': 0,
                    'avg_rpm': 0,
                    'engine_on': False,
                    'error': str(e)[:200]
                }
            finally:
                async with counter_lock:
                    processed += 1
                    if processed % 10 == 0:
                        await status_msg.edit_text(f"🔄 Обработано {processed}/{total} ТС...")

    tasks = [process_one(tid) for tid in vehicle_ids]
    results = await asyncio.gather(*tasks)
    output = io.StringIO()
    writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['ID', 'Госномер', 'Название', 'Кол-во точек', 'Макс. обороты', 'Ср. обороты', 'Двигатель работал', 'Статус'])
    for r in results:
        plate = VEHICLE_DETAILS.get(r['id'], {}).get('plate', '')
        name = VEHICLE_DETAILS.get(r['id'], {}).get('name', '')[:50]
        if r['success'] and r['has_data']:
            engine = 'Да' if r['engine_on'] else 'Нет'
            writer.writerow([r['id'], plate, name, r['points'], r['max_rpm'], r['avg_rpm'], engine, 'OK'])
        elif r['success']:
            writer.writerow([r['id'], plate, name, 0, '—', '—', '—', 'Нет данных'])
        else:
            writer.writerow([r['id'], plate, name, 0, '—', '—', '—', f"Ошибка: {r['error']}"])
    csv_data = output.getvalue().encode('utf-8-sig')
    file = BufferedInputFile(csv_data, filename=f"rpm_report_{datetime.now().strftime('%Y%m%d_%H%M')}_{period_name}.csv")
    await status_msg.delete()
    await message.answer_document(document=file, caption=f"📊 Отчёт по оборотам за {period_name}. ТС: {total}.")

# ===== ТЕКСТОВЫЕ КОМАНДЫ =====
@router.message(Command("find"))
async def find_command(msg: Message):
    if not await check_access(msg):
        return
    stats.log_command(msg.from_user.id, msg.from_user.username, "find", msg.text)
    if not VEHICLE_INDEX:
        await msg.answer("⚠️ База ТС не загружена.")
        return
    args = msg.text.split()
    if len(args) < 2:
        await msg.answer("🔍 Использование: /find [номер]\n\nПримеры:\n/find 2700РВ78\n/find 10039", reply_markup=main_menu_keyboard())
        return
    query = args[1].strip()
    norm_query = normalize_query(query)
    if len(norm_query) < 2:
        await msg.answer("❌ Слишком короткий запрос.", reply_markup=main_menu_keyboard())
        return
    terminal_id = VEHICLE_INDEX.get(norm_query)
    if terminal_id:
        details = VEHICLE_DETAILS.get(terminal_id, {})
        plate = details.get('plate', 'не указан')
        name = details.get('name', '')
        brand = details.get('brand', '')
        model = details.get('model', '')
        response = f"✅ <b>ТС найдено!</b>\n\n<b>ID терминала:</b> <code>{terminal_id}</code>\n<b>Госномер:</b> {plate}\n<b>Название:</b> {name}\n<b>Марка/модель:</b> {brand} {model}"
        await msg.answer(response, parse_mode=ParseMode.HTML, reply_markup=state_button_keyboard(terminal_id))
        return
    matches = []
    seen_ids = set()
    for key, tid in VEHICLE_INDEX.items():
        if norm_query in key:
            if tid not in seen_ids:
                seen_ids.add(tid)
                matches.append({'id': tid, 'key': key})
        if len(matches) >= 10:
            break
    if not matches:
        await msg.answer(f"❌ По запросу '{query}' ничего не найдено.", reply_markup=main_menu_keyboard())
        return
    lines = [f"🔍 <b>Найдено по запросу '{query}':</b>", ""]
    for i, m in enumerate(matches, 1):
        details = VEHICLE_DETAILS.get(m['id'], {})
        plate = details.get('plate', '')
        name = details.get('name', '')
        if plate:
            lines.append(f"{i}. <b>{plate}</b>")
        else:
            lines.append(f"{i}. <b>{m['key']}</b>")
        lines.append(f"   ID: <code>{m['id']}</code>")
        if name:
            lines.append(f"   {name[:50]}")
        lines.append("")
    if len(matches) == 10:
        lines.append("<i>Показаны первые 10 результатов. Уточните запрос.</i>")
    keyboard_rows = []
    for m in matches:
        details = VEHICLE_DETAILS.get(m['id'], {})
        plate = details.get('plate', '')
        if plate:
            button_text = f"📍 {plate}"
        else:
            short_key = m['key'][:8]
            button_text = f"📍 {short_key}..."
        keyboard_rows.append([InlineKeyboardButton(text=button_text, callback_data=f"state_{m['id']}")])
    keyboard_rows.append([InlineKeyboardButton(text="🔍 Новый поиск", callback_data="menu_find")])
    keyboard_rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_menu")])
    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    await msg.answer("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=reply_markup)

@router.message(Command("state"))
async def state_command(msg: Message):
    if not await check_access(msg):
        return
    stats.log_command(msg.from_user.id, msg.from_user.username, "state", msg.text)
    args = msg.text.split()
    if len(args) < 2:
        await msg.answer("⚠️ Использование: /state [госномер или ID]\n\nПример: /state 2700РВ78", reply_markup=main_menu_keyboard())
        return
    identifier = args[1].strip()
    terminal_id = None
    if identifier.isdigit():
        terminal_id = identifier
    else:
        terminal_id = find_terminal_id(identifier)
        if not terminal_id:
            await msg.answer(f"❌ ТС с номером '{identifier}' не найдено.", reply_markup=main_menu_keyboard())
            return
    processing_msg = await msg.answer(f"🔍 Запрашиваю состояние ТС ID: {terminal_id}...")
    try:
        client: OmnicommClient = msg.bot.omnicomm_client
        state_data = await client.get_vehicle_state(terminal_id)
        response = format_vehicle_state(state_data, terminal_id)
        await processing_msg.delete()
        await msg.answer(response, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
    except Exception as exc:
        stats.log_error(msg.from_user.id, "state", exc)
        await processing_msg.delete()
        await msg.answer(f"❌ Ошибка: {str(exc)[:200]}", reply_markup=main_menu_keyboard())

@router.message(Command("rpm_report"))
async def rpm_report_cmd(msg: Message):
    if not await check_access(msg):
        return
    stats.log_command(msg.from_user.id, msg.from_user.username, "rpm_report", msg.text)
    if not VEHICLE_INDEX:
        await msg.answer("⚠️ База ТС не загружена.")
        return
    args = msg.text.split()
    days = 30
    if len(args) >= 2:
        try:
            days = int(args[1])
            if days < 1:
                await msg.answer("❌ Число дней должно быть положительным.")
                return
        except ValueError:
            await msg.answer("❌ Неверный формат. Пример: /rpm_report 7")
            return
    vehicle_ids = [int(tid) for tid in set(VEHICLE_INDEX.values())]
    client: OmnicommClient = msg.bot.omnicomm_client
    await generate_and_send_rpm_report(msg, client, vehicle_ids=vehicle_ids, days=days, period_name=f"{days} дн.")

# ===== КОМАНДА СТАТИСТИКИ (ТОЛЬКО ДЛЯ АДМИНА) =====
@router.message(Command("stats"))
async def stats_command(msg: Message):
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.answer("⛔ У вас нет прав на просмотр статистики.")
        return
    stats_data = stats.get_stats()
    response = f"""📊 <b>Статистика бота</b>

👥 <b>Пользователи:</b> {stats_data['unique_users']}
📝 <b>Всего команд:</b> {stats_data['total_commands']}
❌ <b>Ошибок:</b> {stats_data['total_errors']}
📅 <b>Команд сегодня:</b> {stats_data['today_commands']}

🔥 <b>Топ-5 команд:</b>\n"""
    for cmd, count in stats_data['top_commands']:
        response += f"  • /{cmd}: {count}\n"
    response += "\n⏰ <b>Активность по часам (последние 24ч):</b>\n"
    for hour, count in stats_data['hourly']:
        response += f"  {hour}:00 — {count}\n"
    await msg.answer(response, parse_mode=ParseMode.HTML)

# ===== ЗАПУСК БОТА (С ГЛОБАЛЬНЫМ КЛИЕНТОМ) =====
async def main():
    stats.init_db()
    client = OmnicommClient()
    bot.omnicomm_client = client
    try:
        await dp.start_polling(bot)
    finally:
        await client.aclose()

if __name__ == "__main__":
    asyncio.run(main())