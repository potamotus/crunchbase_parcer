# Crunchbase Investor Count Scraper

Скрипт для получения количества бизнес-ангелов (Individual/Angel) по городам из Crunchbase Advanced Search.

## Как работает

1. Python читает список городов из CSV
2. Генерирует JavaScript-скрипт для выполнения в Chrome Console
3. JS делает запросы к внутреннему API Crunchbase (нужна авторизация через браузер)
4. Результаты сохраняются в JSON
5. Python обновляет исходный CSV с полученными данными

## Использование

### 1. Генерация JS-скрипта

```bash
# Просмотр списка городов (без запросов)
python3 crunchbase_scraper.py generate --dry-run

# Генерация для первых 5 городов (тест)
python3 crunchbase_scraper.py generate --limit 5

# Генерация для всех 266 городов
python3 crunchbase_scraper.py generate
```

### 2. Выполнение в Chrome

1. Открой https://www.crunchbase.com и залогинься
2. Открой DevTools → Console (Cmd+Option+J)
3. Вставь содержимое `output/crunchbase_query.generated.js`
4. Дождись завершения (~13 минут для всех городов)
5. Скопируй JSON-вывод в `output/results.json`

### 3. Обновление CSV

```bash
python3 crunchbase_scraper.py update output/results.json
```

Результат: `output/updated_source.csv`

## Структура

```
├── crunchbase_scraper.py   # Основной скрипт
├── data/
│   └── source.csv          # Исходные данные (города)
├── output/                  # Результаты (в .gitignore)
│   ├── crunchbase_query.generated.js
│   ├── results.json
│   └── updated_source.csv
├── .env.example
└── .gitignore
```

## Безопасность

- Никакие API-ключи не требуются — авторизация через сессию браузера
- Файлы `.env` и `output/` исключены из git
- Скрипт использует только чтение данных, не модифицирует ничего в Crunchbase
