import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

import config
from omnicomm.client import OmnicommClient
from omnicomm.exceptions import OmnicommAPIError, OmnicommAuthError

if not config.TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN is not set. Please set it in the environment or .env file.")

bot = Bot(token=config.TG_BOT_TOKEN)
dp = Dispatcher()
client = OmnicommClient()


@dp.message(Command("start"))
async def start(msg: types.Message):
    await msg.answer(
        "Omnicomm Bot\n"
        "/terminals — список терминалов\n"
        "/terminal <id> — профиль терминала\n"
        "/vehicles — список ТС\n"
        "/vehicle <id> — профиль ТС"
    )


@dp.message(Command("terminals"))
async def terminals(msg: types.Message):
    try:
        data = await client.get_terminals()
    except (OmnicommAPIError, OmnicommAuthError) as exc:
        await msg.answer(f"Ошибка при получении терминалов: {exc}")
        return
    except Exception as exc:
        await msg.answer(f"Неожиданная ошибка: {exc}")
        return

    if not data:
        return await msg.answer("Терминалы не найдены.")

    lines = []
    for t in data:
        tid = t.get("id")
        name = t.get("name", "")
        lines.append(f"{tid} — {name}")
    await msg.answer("\n".join(lines))


@dp.message(Command("terminal"))
async def terminal(msg: types.Message):
    args = (msg.text or "").split()
    if len(args) < 2:
        return await msg.answer("Использование: /terminal <id>")

    terminal_id = args[1]
    try:
        profile = await client.get_terminal_profile(terminal_id)
    except (OmnicommAPIError, OmnicommAuthError) as exc:
        await msg.answer(f"Ошибка при получении профиля терминала: {exc}")
        return
    except Exception as exc:
        await msg.answer(f"Неожиданная ошибка: {exc}")
        return

    await msg.answer(str(profile))


@dp.message(Command("vehicles"))
async def vehicles(msg: types.Message):
    try:
        data = await client.get_vehicles()
    except (OmnicommAPIError, OmnicommAuthError) as exc:
        await msg.answer(f"Ошибка при получении ТС: {exc}")
        return
    except Exception as exc:
        await msg.answer(f"Неожиданная ошибка: {exc}")
        return

    if not data:
        return await msg.answer("ТС не найдены.")

    lines = []
    for v in data:
        vid = v.get("id")
        name = v.get("name", "")
        lines.append(f"{vid} — {name}")
    await msg.answer("\n".join(lines))


@dp.message(Command("vehicle"))
async def vehicle(msg: types.Message):
    args = (msg.text or "").split()
    if len(args) < 2:
        return await msg.answer("Использование: /vehicle <id>")

    vehicle_id = args[1]
    try:
        profile = await client.get_vehicle_profile(vehicle_id)
    except (OmnicommAPIError, OmnicommAuthError) as exc:
        await msg.answer(f"Ошибка при получении профиля ТС: {exc}")
        return
    except Exception as exc:
        await msg.answer(f"Неожиданная ошибка: {exc}")
        return

    await msg.answer(str(profile))


async def main():
    try:
        await dp.start_polling(bot)
    finally:
        try:
            await client.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
