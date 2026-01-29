# Omnicomm Telegram Bot

Небольшой Telegram-бот для работы с API Omnicomm: выводит список терминалов и транспортных средств, а также профили по id.

Особенности
- Асинхронный клиент на httpx.AsyncClient с безопасным обновлением JWT (asyncio.Lock)
- Aiogram 3.x для обработки сообщений
- Конфигурация через .env

Переменные окружения
- TG_BOT_TOKEN — токен Telegram-бота
- OMNICOMM_BASE_URL — базовый URL Omnicomm API (например, `https://rc.gsprom.ru`)
- OMNICOMM_USERNAME — логин Omnicomm
- OMNICOMM_PASSWORD — пароль Omnicomm

Пример (см. `.env.example`):
```
TG_BOT_TOKEN=telegram_token
OMNICOMM_BASE_URL=https://rc.gsprom.ru
OMNICOMM_USERNAME=login
OMNICOMM_PASSWORD=password
```

Установка и запуск (локально)
1. Создайте виртуальное окружение:
   python -m venv .venv
   source .venv/bin/activate  # linux / macOS
   .venv\Scripts\activate     # Windows
2. Установите зависимости:
   pip install -r requirements.txt
3. Создайте `.env` с переменными (или экспортируйте в окружение).
4. Запустите:
   python bot.py

Замечание: НЕ загружайте `.env` в репозиторий. Храните секреты в CI/CD или в настройках инфраструктуры.
