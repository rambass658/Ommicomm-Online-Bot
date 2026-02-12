#!/usr/bin/env python3
import asyncio
import json
import logging
import re
from datetime import datetime
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.enums import ParseMode
from omnicomm.client import OmnicommClient

import config

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=config.TG_BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ===== ЗАГРУЗКА БАЗЫ ДАННЫХ ТС =====
VEHICLES_DB_FILE = "vehicles_db.json"

def load_vehicles_db():
    """Загружает базу соответствий из JSON."""
    try:
        with open(VEHICLES_DB_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"✅ База ТС загружена: {len(data['index'])} записей")
        return data.get('index', {}), data.get('details', {})
    except FileNotFoundError:
        logger.warning("⚠️ Файл vehicles_db.json не найден. Поиск по госномеру работать не будет.")
        return {}, {}
    except Exception as e:
        logger.error(f"Ошибка загрузки базы: {e}")
        return {}, {}

# Глобальные переменные с базой
VEHICLE_INDEX, VEHICLE_DETAILS = load_vehicles_db()

def normalize_query(query: str) -> str:
    """Приводит поисковый запрос к нормализованному виду (без пробелов, заглавные)."""
    if not query:
        return ""
    return re.sub(r'\s+', '', query).upper()

def find_terminal_id(identifier: str) -> str | None:
    """Ищет ID терминала по госномеру, гаражному номеру, VIN или ID."""
    norm = normalize_query(identifier)
    return VEHICLE_INDEX.get(norm)

# ===== КОМАНДА START =====
@router.message(Command("start"))
async def start(msg: Message):
    await msg.answer(
        "🚛 Omnicomm Bot - Мониторинг транспорта\n\n"
        "Доступные команды:\n"
        "/state <госномер или ID> — состояние ТС\n"
        "/find <номер> — поиск ТС по госномеру, гаражному номеру или VIN\n\n"
        "Примеры:\n"
        "/state 2700РВ78\n"
        "/state 326026157\n"
        "/find 10039\n"
        "/find 2700РВ78",
        parse_mode=ParseMode.HTML
    )

# ===== КОМАНДА STATE (с поддержкой госномера) =====
@router.message(Command("state"))
async def vehicle_state(msg: Message):
    try:
        args = msg.text.split()
        if len(args) < 2:
            await msg.answer(
                "⚠️ Укажите ID или госномер транспортного средства\n\n"
                "Пример: /state 2700РВ78 или /state 326026157"
            )
            return

        identifier = args[1].strip()
        terminal_id = None

        # Если это число — возможно, сразу ID терминала
        if identifier.isdigit():
            terminal_id = identifier
        else:
            # Ищем в базе
            terminal_id = find_terminal_id(identifier)
            if not terminal_id:
                # Пробуем как частичный поиск?
                # Можно добавить поиск по части, но пока просто ошибка
                await msg.answer(f"❌ ТС с номером '{identifier}' не найдено в базе.\n"
                                 f"Попробуйте /find для поиска.")
                return

        # Запрашиваем состояние
        processing_msg = await msg.answer(f"🔍 Запрашиваю состояние ТС ID: {terminal_id}...")

        client = OmnicommClient()
        state_data = await client.get_vehicle_state(terminal_id)
        await client.aclose()

        # Красиво форматируем ответ
        response = format_vehicle_state(state_data, terminal_id)
        await processing_msg.delete()
        await msg.answer(response, parse_mode=ParseMode.HTML)

    except Exception as exc:
        error_msg = f"❌ Ошибка при получении состояния ТС: {str(exc)}"
        if len(error_msg) > 4000:
            error_msg = error_msg[:4000] + "..."
        await msg.answer(error_msg)

# ===== ФОРМАТИРОВАНИЕ СОСТОЯНИЯ (ваша прежняя функция) =====
def format_vehicle_state(data: dict, vehicle_id: str) -> str:
    """Форматирует данные о состоянии ТС в читаемый вид (ваша версия)."""
    if not isinstance(data, dict):
        return f"⚠️ Неожиданный формат данных: {str(data)[:500]}"

    lines = [
        f"🚚 <b>Состояние ТС (ID: {vehicle_id})</b>",
        ""
    ]

    # Статус
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

    # Адрес
    address = data.get('address')
    if address:
        lines.append(f"🏠 <b>Адрес:</b> {address}")

    # Топливо
    fuel = data.get('currentFuel')
    if fuel is not None:
        lines.append(f"⛽ <b>Топливо:</b> {fuel} л")

    # Зажигание
    ignition = data.get('currentIgn')
    if ignition is not None:
        lines.append(f"🔑 <b>Зажигание:</b> {'ВКЛ' if ignition else 'ВЫКЛ'}")

    # Скорость
    speed = data.get('currentSpeed')
    if speed is not None:
        lines.append(f"🚗 <b>Скорость:</b> {speed} км/ч")

    # Дата последних данных
    last_date = data.get('lastDataDate')
    if last_date:
        try:
            if last_date > 10000000000:
                last_date = last_date / 1000
            dt = datetime.fromtimestamp(last_date)
            if dt.year < 2000:
                dt = datetime.fromtimestamp(last_date * 1000)
            lines.append(f"🕒 <b>Последние данные:</b> {dt.strftime('%d.%m.%Y %H:%M:%S')}")
            # возраст данных
            delta = datetime.now() - dt
            if delta.days > 0:
                lines.append(f"   <i>({delta.days} дн. {delta.seconds//3600} ч. назад)</i>")
            elif delta.seconds > 3600:
                lines.append(f"   <i>({delta.seconds//3600} ч. {delta.seconds%3600//60} мин. назад)</i>")
            elif delta.seconds > 60:
                lines.append(f"   <i>({delta.seconds//60} мин. назад)</i>")
            else:
                lines.append(f"   <i>(только что)</i>")
        except:
            lines.append(f"🕒 <b>Последние данные:</b> {last_date}")
    else:
        lines.append(f"🕒 <b>Последние данные:</b> нет данных")

    # Координаты GPS
    last_gps = data.get('lastGPS')
    if last_gps and isinstance(last_gps, dict):
        lat = last_gps.get('latitude')
        lon = last_gps.get('longitude')
        if lat is not None and lon is not None:
            lines.append(f"📍 <b>Координаты:</b> {lat:.6f}, {lon:.6f}")
            maps_link = f"https://maps.google.com/?q={lat},{lon}"
            lines.append(f"🗺️ <a href='{maps_link}'>Открыть на карте</a>")

    # Направление
    direction = data.get('lastGPSDir')
    if direction is not None:
        directions = ['С', 'СВ', 'В', 'ЮВ', 'Ю', 'ЮЗ', 'З', 'СЗ']
        idx = round(direction / 45) % 8
        lines.append(f"🧭 <b>Направление:</b> {direction}° ({directions[idx]})")

    # Спутники
    satellites = data.get('lastGPSSat')
    if satellites is not None:
        if satellites > 0:
            lines.append(f"📡 <b>Спутники:</b> {satellites}")
        else:
            lines.append(f"📡 <b>Спутники:</b> нет сигнала")

    # Превышение скорости
    speed_exceed = data.get('speedExceed')
    if speed_exceed is not None:
        lines.append(f"⚠️ <b>Превышение скорости:</b> {'ДА' if speed_exceed else 'нет'}")

    # Напряжение
    voltage = data.get('voltage')
    if voltage is not None:
        lines.append(f"🔋 <b>Напряжение:</b> {voltage} В")

    return "\n".join(lines)

# ===== КОМАНДА FIND (поиск по базе) =====
@router.message(Command("find"))
async def find_vehicle(msg: Message):
    """Поиск ТС по госномеру, гаражному номеру, VIN или части номера."""
    if not VEHICLE_INDEX:
        await msg.answer("⚠️ База ТС не загружена. Сначала создайте файл vehicles_db.json.")
        return

    args = msg.text.split()
    if len(args) < 2:
        await msg.answer(
            "🔍 <b>Поиск транспортного средства</b>\n\n"
            "Использование:\n"
            "<code>/find номер</code>\n\n"
            "Примеры:\n"
            "<code>/find 2700РВ78</code> — точный поиск\n"
            "<code>/find 10039</code> — поиск по гаражному номеру\n"
            "<code>/find HCMADC90C00051205</code> — поиск по VIN",
            parse_mode=ParseMode.HTML
        )
        return

    query = args[1].strip()
    norm_query = normalize_query(query)

    if len(norm_query) < 2:
        await msg.answer("Введите минимум 2 символа для поиска.")
        return

    # Сначала точное совпадение
    terminal_id = VEHICLE_INDEX.get(norm_query)
    if terminal_id:
        # Покажем подробную информацию
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
            f"<b>Марка/модель:</b> {brand} {model}\n\n"
            f"<b>Команды:</b>\n"
            f"/state {terminal_id} — состояние ТС"
        )
        await msg.answer(response, parse_mode=ParseMode.HTML)
        return

    # Если точного нет — ищем частичные совпадения (все ключи, где содержится запрос)
    # Ограничим 10 результатами
    matches = []
    for key, tid in VEHICLE_INDEX.items():
        if norm_query in key:
            if tid not in [m['id'] for m in matches]:  # уникальные ТС
                matches.append({'id': tid, 'key': key})
        if len(matches) >= 10:
            break

    if not matches:
        await msg.answer(f"❌ По запросу '{query}' ничего не найдено.")
        return

    response = f"🔍 <b>Найдено по запросу '{query}':</b>\n\n"
    for i, m in enumerate(matches, 1):
        details = VEHICLE_DETAILS.get(m['id'], {})
        plate = details.get('plate', '')
        name = details.get('name', '')
        response += f"{i}. <b>{plate or m['key']}</b>\n"
        response += f"   ID: <code>{m['id']}</code>\n"
        response += f"   {name[:50]}\n"
        response += f"   /state {m['id']}\n\n"

    if len(matches) == 10:
        response += "<i>Показаны первые 10 результатов. Уточните запрос.</i>"

    await msg.answer(response, parse_mode=ParseMode.HTML)

# ===== ЗАПУСК БОТА =====
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())