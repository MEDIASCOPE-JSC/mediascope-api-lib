import json


class HTTP404Error(Exception):
    def __init__(self, message="Адрес или задача не найдена"):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.message}'


class HTTP400Error(Exception):
    def __init__(self, status_code=400, message="Не верный запрос"):
        self.message = message
        self.status_code = status_code
        if self.message.startswith("{"):
            try:
                err_data = json.loads(self.message)
                if 'errorCode' in err_data and 'errorMessage' in err_data:
                    self.message = str(err_data['errorCode']) + ' Сообщение: ' + str(err_data['errorMessage'])
            except:
                pass

        super().__init__(self.status_code, self.message)

    def __str__(self):
        return f'{self.message}'
