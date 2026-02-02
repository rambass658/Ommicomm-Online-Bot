# Omnicomm Online Telegram Bot

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

Замечание: НЕ загружайте `.env` в репозиторий. Храните секреты в CI/CD или в настройках инфраструктуры.


