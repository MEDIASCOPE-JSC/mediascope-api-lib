"""
Error module for Mediascope API.
"""
import json

class MediascopeApiError(Exception):
    """
    Базовый класс для всех ошибок, связанных с Mediascope API.
    """
    def __init__(self, message: str, code: int = None):
        self.message = message
        self.code = code
        super().__init__(message)

    def __str__(self):
        if self.code:
            return f"[Error code {self.code}] {self.message}"
        return self.message


class AuthorizationError(MediascopeApiError):
    """
    Ошибка авторизации, возникающая при неправильных данных для авторизации или недостатке прав.
    """
    def __init__(self, message: str = "Ошибка авторизации", code: int = 401):
        super().__init__(message, code)


class AccessForbiddenError(MediascopeApiError):
    """
    Ошибка доступа, возникающая при недостаточности прав для выполнения операции.
    """
    def __init__(self, message: str = "Доступ запрещен", code: int = 403):
        super().__init__(message, code)


class NotFoundError(MediascopeApiError):
    """
    Ошибка, возникающая, когда запрашиваемый ресурс не найден.
    """
    def __init__(self, message: str = "Ресурс не найден", code: int = 404):
        super().__init__(message, code)


class NoDataError(MediascopeApiError):
    """
    Ошибка, возникающая, когда данные не найдены.
    """
    def __init__(self, message: str = "Нет данных", code: int = 404):
        super().__init__(message, code)


class TooManyRequestsError(MediascopeApiError):
    """
    Ошибка, возникающая при слишком большом количестве запросов за короткий
    промежуток времени (ошибка 429).
    """
    def __init__(self, message: str = "Слишком много запросов", code: int = 429):
        super().__init__(message, code)


class BadRequestError(MediascopeApiError):
    """
    Ошибка, возникающая при неверном запросе (ошибка 400).
    """
    def __init__(self, message: str = "Неверный запрос", code: int = 400):
        self.message = message
        self.code = code
        if self.message.startswith("{"):
            try:
                err_data = json.loads(self.message)
                if 'errorCode' in err_data and 'errorMessage' in err_data:
                    self.message = str(err_data['errorCode']) + ' Сообщение: ' + str(err_data['errorMessage'])
            except json.JSONDecodeError:
                pass
        super().__init__(message, code)

class ServerError(MediascopeApiError):
    """
    Ошибка, возникающая при внутренней ошибке сервера (ошибки 5xx).
    """
    def __init__(self, message: str = "Ошибка сервера", code: int = 500):
        super().__init__(message, code)
