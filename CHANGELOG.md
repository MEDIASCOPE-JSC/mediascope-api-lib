# Changelog

All notable changes to this project will be documented in this file.

## [1.1.18] - 2024-10-14

1. добавление дополнительных полей статуса задач для TV Index API, Crossweb API, Metrix API
2. добавление поддержки работы с несколькими API в одном скрипте/ноутбуке
3. добавление проверки наличия обновления библиотеки при инициализации объектов API
4. добавление метода проверки статуса списка задач в TV Index API
5. исправление ошибок

## [1.1.17] - 2024-07-17

1. добавление автоматического повторного вызова API (до 5 раз) при возникновении ошибок на сервере
2. добавление тестов библиотеки
3. добавление асинхронного расчета задач и возможности работы с профилем для Metrix API (API Counter)
4. добавление настройки задержек при опросе статусов расчетных задач Crossweb API
5. расширение логирования ошибок
6. исправление ошибок

## [1.1.16] - 2024-04-17

### Added

1. добавление настроек задержек при опросе статуса расчетных задач
2. добавление учета поставки данных при проверке валидности расчетных задач
3. добавление параметра вывода заголовка с количеством записей при загрузке справочников

## [1.1.15] - 2024-01-15

### Added

1. добавление возможности перезапуска и отмены расчетных задач в Crossweb API
2. добавление отчета media-profile в Crossweb API
3. добавление отчета profile-duplication в Crossweb API
4. добавление справочников медиа мониторинга и профиля в Crossweb API

## [1.1.14] - 2023-12-20

### Added

1. добавление справочника рекламодатель-продукт

### Updated

1. исправление ошибок обработки фильтра по регионам

## [1.1.13] - 2023-11-13

### Added

1. автодобавление базового и/или целевого демо фильтра на основании фильтра по регионам для всех типов отчетов

### Updated

1. изменение способа загрузки доступных атрибутов отчетов
2. исправление ошибки получения доступных атрибутов для отчета, закрытого ограничениями

## [1.1.12] - 2023-09-13

### Added

1. Получить списки доступных атрибутов отчета Анализ отдельных респондентов
2. Получить коллекцию связей регион-город
3. Включение режима автоматического добавления демо фильтра по городам на основании фильтра по регионам.

## [1.1.11] - 2023-08-23


1. catalogs:
    1. Удалены методы неактивных каталогов
    2. скорректирован текст документации (docstring)
    3. в методе get_tv_demo_attribute поле colName заменено на entityName. В entityName содержатся названия в стиле lowerCamelCase, в этом формате АПИ принимает на вход параметры задачи.
    4. добавлены дефолтные значения сортировки получаемых каталогов
    5. во всех методах для параметра use_cache установлено дефолтное значение False
    6. в ряде каталогов сделан более удобный для восприятия порядок полей
    7. добавлен метод get_availability_period (доступный для расчета период)
    8. добавлен метод get_tv_age_restriction (каталог возрастных ограничений)
2. tasks:
    1. в метод build_task добавлен параметр sortings, позволяющий задать условия сортировки результата расчета. Дефолтное значение None
    2. в метод result2table добавлен параметр to_lists, позволяющий объединить атрибуты многотоварной рекламы в списки. Дефолтное значение False


## [1.1.10] - 2023-06-28

### Updated

- In the TV-Index fixed tv-break-distribution dict

## [1.1.9] - 2023-06-26

### Added

#### CrossWeb
- Added ability to use English names and notes for get items from catalogs in the CrossWeb API

#### TV-Index (Mediavortex API)

- Added 18 catalogs
- Added reports:
  - consumption-target
  - duplication-timeband
- Added methods for restart and cancel tasks

## [1.1.8] - 2023-03-28

### Added

- Added module for work with TV-Index data (Mediavortex API)

### Updated

- Updated Network module: added ability to use proxy for requests
- Updated CrossWeb catalog module: added method for getting available date ranges for data
- Updated Counter module: added methods:
  - for getting available tmsecs
  - for getting available partners
  - for getting report by tmsecs
  - for getting report by partners

## [1.1.7] - 2022-11-11

### Added
- Added methods for calculate Media duplication reports into Crossweb API
- Added methods for calculate Monitoring reports into Crossweb API
- Added methods getting trees (monitoring-link-tree and procuct-category-tree)
- Added method get_product_advertiser in the Monitoring Crossweb API

### Updated
- Fixed transform to int columns with uni ("-" symbol appears)

## [1.1.6] - 2022-09-06

### Added
- Added module for work with Counter API (Calculate hits and uniques statistics on Counter data)

### Updated
- Duplicated functionality moved from specific modules to common module

## [1.1.5] - 2022-06-21

### Added

Added attribute: Thematics of resources.
- Added method for work with dictionary Thematics of resources.
- Added ability use thematics of resources in filters and slices.

## [1.1.3] - [1.1.4] - 2022-01-24

### Updated
- Updated urls for CrossWeb API: advertisement -> profile, because some browsers block urls with "advertisement" word.
- Added "task_type" in the CrossWeb task and updated send_tasks methods, now they use "task_type" for select API endpoints
- Added checks for available "units" in the Cross Web task


## [1.1.2] - 2022-01-18

### Updated
- Fixed bug with demographic filter in the CrossWeb. (Demographic filter was skipped in the tasks) 


## [1.1.0] - 2021-12-30

### Added
- Added methods for work with CrossWeb API
- Added module for error handling

### Updated
- In the Responsum modules, updated text in comments and docstrings

## [1.0.5] - 2021-06-29

### Updated
- Fixed the facility name for prefact (desktop-pre -> desktop_pre)


## [1.0.4] - 2021-06-23

### Updated
- Updated the wait_task method - added ability to wait for many running tasks

## [1.0.3] - 2021-05-27

### Updated
- Fixed bug with task state in the wait_task method - The method could finish waiting before the task calculated.
- Fixed bug in the result2hierarchy method - added search for names for branches: Agency and Network 

## [1.0.1] - [1.0.2] - 2021-04-27

### Added
- Changelog (this file)
- Added ability to set path to a settings file
- Added ability to set path to a cache folder 
- In a cache filename added user login for ensuring unique cache files for different users

### Updated
- In the Responsum Catalogs module, added parameter "use_cache=True/False" in the get_holdings method.

### Deleted
- Removed setting 'scope': 'offline_access' from token request body, because the auth server don't allowed this option 
  for users
 
