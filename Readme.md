# Система сбора неструктурированных логов

## Компоненты

Данная система использует Fluentd для сбора, Kafka для доставки логов, улучшенный алгоритм Drain3 для парсинга, Clickhouse для хранения и Apache Superset для просмотра логов.

## Конфигурация
Перед запуском пользователю требуется выставить настройки работы алгоритма в файле [drain3.ini](test_app/Drain3/examples/drain3.ini).
Drain3 использует [configparser](https://docs.python.org/3.4/library/configparser.html). 

Парметры конфигурации:

- `[DRAIN]/sim_th` - порог сходства, если процент схожих токенов для сообщения журнала ниже этого числа, будет создан новый кластер (дефолтное значение 0.4)
- `[DRAIN]/depth` - максимальная глубина уровней кластеров. Минимум 3. (дефолтное значение 4)
- `[DRAIN]/max_children` - максимальное количество детей у узла (дефолтное значение 100)
- `[DRAIN]/max_clusters` - максимальное количество отслеживаемых кластеров(по умолчанию неограниченно)
- `[DRAIN]/extra_delimiters` - разделители, которые следует применять при разбиении сообщения журнала на слова
- `[MASKING]/masking` - параметры масок (дефолтное значение "")
- `[MASKING]/mask_prefix` & `[MASKING]/mask_suffix` - обертка идентифицированных параметров в шаблонах(< и > при замене параметра на <*>)
- `[SNAPSHOT]/snapshot_interval_minutes` - временной интервал для сохранения состояния системы (дефолтное значение 1)
- `[SNAPSHOT]/compress_state` - сжимать ли состояние перед сохранением

## Маски

Маски в Drain3 позволяют заменять ключевыми словами определенные переменные в логах перед передачей в Drain. Хорошо определенная маска может повысить точность поис- ка шаблонов. Параметры шаблона, которые не соответствуют какой-либо пользовательской маске на этапе предварительного маскирования, заменяются ядром Drain на <*>. Нами бы- ли улучшены существующие и добавлены новые маски. На данный момент выделяются id, параметры с "=" (например, user=name), ip, версии приложений, время(например, 26 ms), дробные числа, шестнадцатеричные числа, числа, команды cmd.

Пример масок для IP адресов и чисел:

```
[MASKING]
masking = [
          {"regex_pattern":"((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})((?=[^A-Za-z0-9])|$)", "mask_with": "IP"},
          {"regex_pattern":"((?<=[^A-Za-z0-9])|^)([\\-\\+]?\\d+)((?=[^A-Za-z0-9])|$)", "mask_with": "NUM"},
          ]
```

## Персистентность

Drain3 поддерживает в своей реализации персистентность. Это означает, что алго- ритм устойчив к перезапускам, позволяя продолжать деятельность и сохранять полученные данные. Состояние Drain3 включает дерево поиска и все кластеры, которые были определены до момента создания снимка(snapshot, снапшот). Снимки сохраняются после создания нового шаблона, обновления шаблона, а так же каждые snapshot_interval_minutes минут. На данный момент алгоритм поддерживает несколько режимов персистентности: snapshot сохраняется в **файл**, в **kafka** в специальной теме, используемой только для снапшотов - последнее сообщение в этой теме является последним снапшотом, в виде ключа в базе данных **Redis**, в виде **объекта в памяти**, либо **не сохраняется** вовсе.


Пример снапшота:

```json
{
  "clusters": [
    {
      "cluster_id": 1,
      "log_template_tokens": [
        "aa",
        "aa",
        "<*>"
      ],
      "py/object": "drain3_core.LogCluster",
      "size": 2
    },
    {
      "cluster_id": 2,
      "log_template_tokens": [
        "My",
        "IP",
        "is",
        "<IP>"
      ],
      "py/object": "drain3_core.LogCluster",
      "size": 1
    }
  ]
}
```

## Запуск

Для запуска системы пользователю требуется изменить настройки запуска алгоритма Drain3 в файле drain3.ini, если это требуется. В файле [drain_kafka_demo.py](test_app/Drain3/examples/drain_kafka_demo.py) нужно обязательно указать формат логов в переменной **log_format**. Для сопоставления названия полей в формате логов, содержащих дату/время, уровень логирования и контент, и полей в таблице структурированных логов требуется указать их соответствие в словаре **headers_mapping**. Так же нужно обязательно указать формат в котором записывается дата и время в переменной **time_format**, для записи в базу. Для выбора режима персистентности в **persistence_type** нужно выбрать **FILE**, **KAFKA**, **REDIS** или **NONE**.

Система запускается при помощи докер контейнера. В файле docker-compose.yaml нужно указать для fluentd и server(контейнер с кодом алгоритма) в разделе volumes путь до директории, в которую будут записываться файлы с логами, например, ../../../spring-petclinic:/Drain3/spring-petclinic. Далее код запускается стандартно при помощи команд `docker-compose build` и `docker-compose up`.

## Просмотр логов

Для просмотра структурированных логов и взаимодействия с ними пользователю предлагается воспользоваться Apache Superset. Это открытое программное обеспечение для исследования и визуализации данных, ориентированное на большие объемы данных. Superset позволяет создавать интерактивные дашборды, графики и таблицы для анализа данных из различных источников, в том числе и из таблиц csv.

Для запуска Apache Superset требуется выполнить команды:

```
git clone https://github.com/apache/superset.git
cd superset

echo "clickhouse-connect" >> ./docker/requirements-local.txt
echo "clickhouse-driver==0.2.6" >> ./docker/requirements-local.txt
echo "clickhouse-sqlalchemy==0.2.4" >> ./docker/requirements-local.txt
echo"MAPBOX_API_KEY=<INSERT>" >> docker/.env-non-dev

docker-compose -f docker-compose-non-dev.yml pull
docker-compose -f docker-compose-non-dev.yml up
```

После этого Superset будет доступен по адресу `http://localhost:8088`. После использования стандартных логина и пароля admin/admin можно подключиться к базе Clickhouse с логами. Для подключения можно использовать url:
```
clickhousedb://host.docker.internal/default
```