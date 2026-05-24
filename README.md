# BigDataFlink

Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink.

## Архитектура

1. **Producer** - читает CSV файлы и отправляет сообщения в Kafka.
2. **Kafka** - хранит поток сообщений в topic `mock_data`.
3. **Flink** - читает поток из Kafka и загружает dimension-таблицы Star Schema.
4. **PostgreSQL** - хранит результат обработки.

## Сервисы

| Сервис | Назначение | URL / порт |
| --- | --- | --- |
| `postgres` | PostgreSQL с БД `bigdata` | `localhost:5435` |
| `zookeeper` | Zookeeper для Kafka | `localhost:2181` |
| `kafka` | Kafka broker | `localhost:9092`, внутри Docker: `kafka:29092` |
| `kafka-ui` | Web UI для Kafka | http://localhost:8080 |
| `jobmanager` | Flink JobManager и Web UI | http://localhost:8081 |
| `taskmanager` | Flink TaskManager | внутри Docker |
| `kafka_producer` | CSV -> Kafka producer | контейнер `kafka_producer` |
| `flink_job` | Submit Flink job | запускает `flink run --detached` |

## Структура проекта

```text
.
├── docker-compose.yml       # Docker Compose конфигурация
├── data/                    # CSV файлы
├── producer/                # Kafka Producer
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
├── flink_job/               # PyFlink приложение
│   ├── flink_job.py
│   └── Dockerfile
├── sql/                     # Инициализация БД
│   └── 01-init.sql
└── README.md
```

## Запуск

### 1. Собрать и запустить стек

```bash
docker compose up -d --build
```

Первый запуск может занять несколько минут: образ Flink скачивает PyFlink и JDBC/Kafka connector jar-файлы.

После запуска `docker compose` сам:

1. поднимает PostgreSQL, Kafka, Kafka UI и Flink;
2. ждет готовности Kafka и PostgreSQL;
3. запускает `kafka_producer`;
4. сабмитит `flink_job.py` во Flink через сервис `flink_job`.



### 2. Проверить контейнеры

```bash
docker compose ps
```

Ожидаемо должны быть запущены `postgres`, `zookeeper`, `kafka`, `kafka-ui`, `jobmanager` и `taskmanager`.

Сервис `flink_job` может завершиться после успешного submit. Это нормально: сама streaming job продолжает работать в Flink.

### 3. Проверить Producer

```bash
docker logs kafka_producer
```

В конце должно быть примерно:

```text
Successfully sent 10000 messages
```

Также можно открыть Kafka UI: http://localhost:8080

### 4. Проверить Flink job

```bash
docker compose exec jobmanager /opt/flink/bin/flink list --running
```

Ожидаемо появится job в статусе `RUNNING`, например:

```text
insert-into_default_catalog.default_database.dim_customer,... (RUNNING)
```

Flink Web UI доступен здесь: http://localhost:8081

## Проверка

### Flink

- Открыть http://localhost:8081
- В разделе Jobs убедиться, что streaming job находится в статусе `RUNNING`

Если UI открывается, но jobs нет, проверьте лог submit-контейнера:

```bash
docker compose logs flink_job
```

### Kafka

Проверить topic:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic mock_data
```

Посмотреть сообщения удобнее через Kafka UI: http://localhost:8080

### PostgreSQL

- Хост: `localhost`
- Порт: `5435`
- БД: `bigdata`
- Пользователь: `admin`
- Пароль: `admin123`

Текущая Flink job загружает dimension-таблицы: `dim_customer`, `dim_product`, `dim_seller`, `dim_store`, `dim_supplier`.

Проверить данные через контейнер:

```bash
docker compose exec postgres psql -U admin -d bigdata -c "SELECT 'dim_customer' AS t, COUNT(*) FROM star.dim_customer UNION ALL SELECT 'dim_product', COUNT(*) FROM star.dim_product UNION ALL SELECT 'dim_seller', COUNT(*) FROM star.dim_seller UNION ALL SELECT 'dim_store', COUNT(*) FROM star.dim_store UNION ALL SELECT 'dim_supplier', COUNT(*) FROM star.dim_supplier;"
```

Или SQL-запросом:

```sql
SELECT 'dim_customer' as t, COUNT(*) FROM star.dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM star.dim_product
UNION ALL SELECT 'dim_seller', COUNT(*) FROM star.dim_seller
UNION ALL SELECT 'dim_store', COUNT(*) FROM star.dim_store
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM star.dim_supplier;
```

Пример успешного результата после загрузки тестовых CSV:

```text
dim_customer | 10000
dim_product  | 3233
dim_seller   | 10000
dim_store    | 9998
dim_supplier | 10000
```

## Ручной перезапуск Flink job

Если нужно заново отправить job без полного пересоздания стека:

```bash
docker compose up -d --force-recreate flink_job
```

Если нужно запустить вручную из JobManager:

```bash
docker compose exec jobmanager /opt/flink/bin/flink run --detached --jobmanager jobmanager:8081 --python /opt/flink/flink_job.py
```

Перед повторным запуском проверьте, что старая job не продолжает работать:

```bash
docker compose exec jobmanager /opt/flink/bin/flink list --running
```

## Полезные логи

```bash
docker compose logs jobmanager
docker compose logs taskmanager
docker compose logs flink_job
docker logs kafka_producer
```

## Остановка

```bash
docker compose down -v
```
# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

Одним из самых популярных фреймворков для работы со streaming processing является Apache Flink. Apache Flink - мощный фреймворк, который предлагает широкий набор функциональности для простого написания streaming processing.

Что необходимо сделать? 

Необходимо реализовать потоковую обработку данных с помощью Flink, который читает топик Kafka, трансформирует данные в режиме streaming в модель звезда и пишет результат в PostgreSQL. Данные в Kafka-топиках хранятся в формате json. Данные в топик kafka нужно отправлять самостоятельно, эмулируя источник данных.

Какие данные отправляются в Kafka?
 - Каждое сообщение в Kafka-топике - это строчка из csv файлов, преобразованная в формат json.

Какие данные отправляются в PostgreSQL?
 - Трансформированные данные в модель данных звезда.

![Лабораторная работа №3](https://github.com/user-attachments/assets/d3c1544d-3fe6-4c15-b673-9aa5d27dbd76)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Flink (рекомендую установку через Docker).
5. Устанавливаете Apache Kafka (рекомендую установку через Docker).
6. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
7. Реализуете приложение, которое каждую строчку из исходных csv-файлов преобразует в json и отправляет в виде сообщения в Kafka-топик.
8. Реализуете приложение на Flink, которое читает Kafka-топик, преобразует данные в модель звезда и сохраняет в PostgreSQL в режиме streaming.
9. Проверяете конечные данные в PostgreSQL.
10. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Flink, Kafka и запуском приложения, которое из файлов mock_data(*).csv создает сообщения json в Kafka.
3. Инструкция, как запускать Flink-джобу и приложение для отправки данных в Kafka для проверки лабораторной работы.
4. Код Apache Flink для трансформации данных в режиме streaming.
