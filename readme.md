# Mediascope API Lib

Python библиотека для работы с Mediascope Delivery API (https://api.mediascope.net)

Библиотека позволяет упростить работу пользователей с Mediascope Delivery API (API) при работе с данными Web-Index.
В первую очередь библиотека предназначена для использования совместно с Mediascope Jupyter 
ноутбуками (https://github.com/MEDIASCOPE-JSC/mediascope-jupyter).


## Основные возможности

- авторизация, получение и обновление токена доступа
- формирование заданий с использованием SQL выражений
- отправка заданий на расчет и ожидание результата
- преобразование результата в pandas DataFrame
- расчет дополнителных статистик на базе полученных результатов


## Использование

Для работы с Mediascope API вам потребуются данные для доступа к API:

- username - имя пользователя для доступа к Mediascope API
- passw - пароль для доступа к Mediascope API
- client_id - идентификатор клиента
- client_secret - ключ для доступа к API
- auth_server - адрес сервера аутентификации
- root_url - адрес Mediascope API
    
Данную информацию вы можете получить у менеджеров Mediascope.

## Конфигурация

Создайте файл в корне проекта:
```shell
settings.json
```
со следующими параметрами:

```json
{
   "username": "you username",
   "passw": "you password",
	"client_id": "client_id",
	"client_secret": "00000000-0000-0000-0000-000000000000",
    "auth_server": "https://auth.mediascope.net/.....",
    "root_url": "https://api.mediascope.net/...."
}
```
и укажите актуальную информацию для доступа.

## Использование
Примеры работы с данными через Mediascope API приведены в проекте: https://github.com/MEDIASCOPE-JSC/mediascope-jupyter

