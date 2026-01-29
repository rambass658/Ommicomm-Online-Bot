class OmnicommAuthError(Exception):
    """Ошибка аутентификации с Omnicomm (неверные учетные данные, неверный ответ при логине и т.п.)."""
    pass


class OmnicommAPIError(Exception):
    """Ошибка при обращении к API Omnicomm (сетевые ошибки, HTTP ошибки, неверный JSON и т.п.)."""
    pass
