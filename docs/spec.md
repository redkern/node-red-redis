<!-- Copy of the spec "ТЗ: @redkern/node-red-redis" (Claude Docs). Source of requirement IDs for scripts/check-req-ids.js. -->

# ТЗ: @redkern/node-red-redis

2026-10-05 · @Yehor

## 1. Мета

Production-палітра Redis для Node-RED: довільні команди, Pub/Sub, scan, транзакції і Redis Streams з consumer groups, гарантією at-least-once, DLQ, backpressure і керуванням для Kubernetes rollout. Замінює `@yroshcha/node-red-contrib-redis-full`. At-least-once означає, що після падіння пода або надто довгої обробки повідомлення може прийти повторно (RDS-STR-15), тож обробка downstream має бути ідемпотентною; без дублів гарантується лише pause/resume і штатний redeploy.

## 2. Дефекти старого пакета, які ТЗ закриває

| № | Дефект | Вимога |
| --- | --- | --- |
| D1 | Recovery-цикл читає PEL з `0` на кожній ітерації: дублі і хибні DLQ | RDS-STR-3, RDS-STR-4 |
| D2 | `allowMsgCommand` увімкнений за замовчуванням, довільні команди з `msg` | RDS-CMD-2, RDS-CMD-3 |
| D3 | Cluster: `db` і `retryStrategy` у `redisOptions` | kit KIT-RDS-4 |
| D4 | XPENDING на кожен запис для DLQ-перевірки | RDS-STR-6 |
| D5 | Control API на `httpNode`, токен опціональний, `err.message` у відповіді | RDS-CTL-* |
| D6 | Backpressure по PEL усієї групи | RDS-STR-8 |
| D7 | Моноліт 1435 рядків, коментарі двома мовами | розділ 8 |
| D8 | Backoff-очікування не переривається при close | kit KIT-LIFE-2 |

## 3. Неймінг

| Що | Значення |
| --- | --- |
| npm / GitHub | `@redkern/node-red-redis` / `redkern/node-red-redis` |
| Префікс типів | `redkern-redis-` |
| Категорія в палітрі | `redkern redis` |
| HTTP | `/redkern/redis/streams/status` і `/redkern/redis/streams/control` на `httpAdmin`; `/redkern/redis/streams/drain` і `/metrics` на внутрішньому порту 9551 |
| Права | `redkern.redis.read`, `redkern.redis.write` |
| settings.js | `redkernRedisConfigEnabled`, `redkernRedisConfigInternalPort`, `redkernRedisConfigMetricsToken`, `redkernRedisConfigDrainToken`: kit читає налаштування з префіксом типу конфіг-ноди (KIT-CFG-6) |
| Env | `REDKERN_REDIS_ENABLED`, `REDKERN_REDIS_INTERNAL_PORT` (дефолт 9551), `REDKERN_REDIS_METRICS_TOKEN`, `REDKERN_REDIS_DRAIN_TOKEN`; поля конфіг-нод з env лише через `${VAR}` у флоу (KIT-CFG-5). Без `REDKERN_REDIS_METRICS_TOKEN` роуту /metrics на порту 9551 немає: kit робить токен обов'язковим для токенного роуту, і без цієї умови вимикалася б уся палітра. З ним листенер стає токенним (KIT-HTTP-6, kit 1.1.0), тож drain вимагає `REDKERN_REDIS_DRAIN_TOKEN`. |
| Логи | `[redkern:redis]` |
| CSS / іконки | `redkern-redis-*`, `redkern-redis.svg` |

### Типи нод і мапінг

| Старий | Новий |
| --- | --- |
| `yroshcha-redis-config` | `redkern-redis-config` |
| `yroshcha-redis-command` | `redkern-redis-command` |
| `yroshcha-redis-subscribe` | `redkern-redis-subscribe` |
| `yroshcha-redis-multi` | `redkern-redis-multi` |
| `yroshcha-redis-scan` | `redkern-redis-scan` |
| `yroshcha-redis-stream-out` | `redkern-redis-stream-out` |
| `yroshcha-redis-stream-in` | `redkern-redis-stream-in` |
| `yroshcha-redis-stream-ack` | `redkern-redis-stream-ack` |
| `yroshcha-redis-stream-claim` | `redkern-redis-stream-claim` |
| `yroshcha-redis-stream-gc` | `redkern-redis-stream-gc` |
| `yroshcha-redis-stream-metrics` | `redkern-redis-stream-metrics` |
| `yroshcha-redis-stream-control` | `redkern-redis-stream-control` |
| `yroshcha-redis-stream-api` | видаляється, API налаштовується в `redkern-redis-config` |

Виняток: `yroshcha-redis-subscribe` у режимі BLPOP/BRPOP мігрує в `redkern-redis-list-in`, у режимі SUBSCRIBE/PSUBSCRIBE у `redkern-redis-subscribe` (вибір типу за режимом ноди, див. RDS-MIG-1).

### Дані, які НЕ змінюються

- Consumer name за замовчуванням: `${instanceId()}-${node.id}`, де `instanceId()` з kit повертає `os.hostname()`, тобто те саме ім'я пода, що раніше читалося з `HOSTNAME`. Імена консюмерів після міграції не змінюються.
- Дефолтний DLQ: `${streamKey}:dlq`, поля DLQ-запису (`sourceStream`, `sourceGroup`, `sourceId`, `consumer`, `deliveries`, `payload`).
- Формат полів стріму, який пише stream-out і читає stream-in.

* RDS-MIG-1. **Ручна міграція.** Автоматичного мігратора немає: ноди переносяться руками, флоу за флоу, нова палітра стоїть поруч зі старою. README палітри містить таблицю типів вище і чекліст ручних дій для кожного типу. [рев'ю]
  - config: нова `redkern-redis-config` поруч зі старою; `cluster: true` стає `mode: cluster` з host:port як єдиним seed; `retryMaxDelay` прибраний; поки працюють обидві палітри, з'єднань удвічі більше, тож перед хвилею перевірити `maxclients`;
  - command: опції `block` немає, блокуючі команди відхиляються (замість них list-in або stream-in); якщо флоу передає `msg.command`, увімкнути `allowMsgCommand` і перевірити, що команди не в denylist, а скрипти задаються командою ноди з текстом у `msg.args`;
  - subscribe у режимі BLPOP/BRPOP: `redkern-redis-list-in` з `prefetch: 0`, щоб темп забору не змінився (RDS-CMD-7); `topic` стає `key`, `mode` стає `direction`;
  - subscribe у режимі SUBSCRIBE/PSUBSCRIBE: `redkern-redis-subscribe` з тими самими каналами; динамічного `msg.subscribe` немає;
  - stream-out: увімкнути `allowMsgStream`, якщо стрім береться з `msg.stream`; порожній `maxlen` означає 0;
  - stream-in: `autoAck` стає `ackMode` (`auto`/`manual`); поставити `backpressureScope: consumer`, бо стара палітра рахувала PEL групи (D6), і тепер `maxPending` діє на под; PEL-алерти замінені нодою stream-metrics і метрикою `redkern_redis_stream_pending`;
  - stream-ack: увімкнути "Pass the message on", якщо флоу йде далі після ack;
  - для command, scan, stream-out, stream-in, stream-ack, stream-claim, stream-gc увімкнути `legacyMsgFields`, якщо далі у флоу читаються старі поля msg;
  - `yroshcha-redis-stream-api` не переноситься, HTTP-керування тепер через admin API і loopback drain-роут.

## 4. Функціональні вимоги: з'єднання

- RDS-CON-1. `redkern-redis-config` створює клієнти лише через `createRedisClient` з kit. [лінт]
- RDS-CON-2. Клієнти створюються з ролями kit (KIT-RDS-3): один `shared` для неблокуючих команд з auto-pipelining, окремі `blocking` для `XREADGROUP BLOCK` і `BLPOP` (по одному на ноду stream-in і list-in) і `subscriber` для Pub/Sub. Offline queue вимкнена в усіх ролях; `ioredis` палітра передає в kit сама і закріплює його версію.
- RDS-CON-3. Пароль та ACL-секрети лише в `credentials`.
- RDS-CON-4. Стан спільного клієнта і кількість відкритих з'єднань конфіг-ноди видно в її діалозі (через admin-роут /redkern/redis/connections з правом redkern.redis.read) і в метриці redkern_redis_connections{role}: статус конфіг-нод Node-RED на полотні не показує.
- RDS-CON-5. Бюджет з'єднань на под: один `shared` на конфіг-ноду, один `subscriber` на конфіг-ноду з Pub/Sub і по одному `blocking` на кожну ноду stream-in і list-in. README містить формулу, а статус конфіг-ноди і метрика `redkern_redis_connections{role}` показують фактичну кількість. Під час міграції конфіг-ноди дублюються, тож кількість подвоюється; перед хвилею міграції перевіряється `maxclients`.
- RDS-CON-6. Параметри з'єднання (host, port, TLS, таймаути) задаються в конфіг-ноді у флоу; якщо вони мають приходити з оточення, поле містить `${VAR}` (KIT-CFG-5), тож з флоу в Git видно, звідки значення, а дві конфіг-ноди можуть посилатися на різні Redis.

## 5. Функціональні вимоги: команди

- RDS-CMD-1. `redkern-redis-command` приймає команду з конфігу або з `msg.command` (рядок або масив токенів), аргументи з `msg.args`.
- RDS-CMD-2. `allowMsgCommand` за замовчуванням `false`.
- RDS-CMD-3. Denylist за замовчуванням: `FLUSHALL`, `FLUSHDB`, `CONFIG`, `SHUTDOWN`, `DEBUG`, `MODULE`, `ACL`, `REPLICAOF`, `SLAVEOF`, `MIGRATE`, `SCRIPT FLUSH`, а також команди з тим самим радіусом ураження: `SWAPDB`, `FAILOVER`, `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `FUNCTION FLUSH/DELETE/RESTORE`, `CLIENT KILL/PAUSE/UNBLOCK`, адмінські підкоманди `CLUSTER`. Скрипти (`EVAL`, `EVALSHA`, `FCALL`, `FUNCTION`, `SCRIPT`) з `msg.command` і `msg.commands` відхиляються, бо скрипт з повідомлення обходить denylist; скрипт задається командою ноди, а текст іде в `msg.args`. Команди, що змінюють стан спільного з'єднання (`CLIENT REPLY/TRACKING/SETNAME`, `HELLO`, `AUTH`, `READONLY`), відхиляються завжди. Denylist вимикається лише явним чекбоксом з попередженням у UI; межа безпеки це Redis ACL, рекомендований мінімальний ACL наведено в README.
- RDS-CMD-4. `redkern-redis-multi` виконує MULTI/EXEC і повертає результат кожної команди; denylist діє і тут.
- RDS-CMD-5. `redkern-redis-scan` має ліміт результатів і таймаут; у cluster сканує всі мастери.
- RDS-CMD-6. `redkern-redis-subscribe` підтримує лише Pub/Sub: SUBSCRIBE і PSUBSCRIBE; після reconnect перепідписується автоматично.
- RDS-CMD-7. `redkern-redis-list-in`: перехідна нода для черг на списках, BLPOP/BRPOP з тією самою семантикою, що й у старій `subscribe` (елемент видаляється зі списку при читанні), на окремому `blocking`-з'єднанні. Поле `prefetch` визначає, скільки елементів нода забирає наперед: `1` (дефолт для нових нод) означає наступний BLPOP лише після `done` попереднього, тож нода не забирає з черги більше, ніж встигає флоу. Для мігрованих нод `prefetch` відтворює фактичну поведінку старої `subscribe`, яку фіксує тест на старому пакеті в alpha.1. Надійного режиму на BLMOVE немає: гарантію доставки дають Streams. Нода позначена як перехідна з плановою депрекацією після переходу зі списків на Streams.
- RDS-CMD-8. Ноди command, multi і stream-out обробляють повідомлення через `k.onInput` з явними `concurrency` (дефолт палітри 64) і `maxQueue` (дефолт 10 000), бо з `concurrency: 1` кожна команда чекала б на попередню. startQueueTimeoutMs 30 с: команда, що чекала старту конфіг-ноди довше, завершується done(err) з кодом NODE_NOT_READY. При `concurrency > 1` порядок виконання не гарантується; кому порядок потрібен, ставить 1. Ці ж ноди мають опцію `errorOutput` (KIT-LOG-4), дефолт `false`: помилки йдуть у catch, а для високочастотних флоу, де аутейдж Redis інакше залив би Loki рядками, її вмикають, і помилки йдуть на окремий вихід.

## 6. Функціональні вимоги: Redis Streams

- RDS-STR-1. `redkern-redis-stream-in` створює групу з `MKSTREAM` якщо її немає; при NOGROUP під час роботи відновлює її.
- RDS-STR-2. Режими ACK: `manual` (дефолт, at-least-once) і `auto` (в UI підписаний як at-most-once).
- RDS-STR-3. **Recovery через курсор.** Відновлення проходить власний PEL сторінками `XPENDING <stream> <group> (<курсор> + <count> <consumer>`, що не змінює лічильники доставок. Записи з in-flight реєстру пропускаються, решта забирається назад `XCLAIM <consumer> 0 <id…>`: він повертає поля, додає одну доставку лише реально повторно відправленим записам і пропускає записи, заакані після XPENDING. Після порожньої сторінки перехід на `>`. Читання історії через `XREADGROUP … 0` заборонене: воно збільшує лічильник доставок усім записам PEL, включно з тими, що ще в флоу, і після кількох redeploy веде до хибного DLQ (D1).
- RDS-STR-4. **In-flight реєстр.** ID, відправлені в флоу і ще не заакані, повторно не відправляються. Запис знімається з реєстру після ACK, переміщення в DLQ або через `reclaimIdleMs`.
- RDS-STR-5. Передача стану від stream-ack до stream-in йде через реєстр, створений у замиканні палітри (вимога KIT-ARCH-2).
- RDS-STR-6. DLQ-перевірка робить один range-запит XPENDING на батч, а не на запис. При recovery лічильники беруться зі сторінки XPENDING з RDS-STR-3, окремого запиту немає.
- RDS-STR-7. Переміщення в DLQ атомарне (XADD + XACK в MULTI). У cluster-режимі нода перевіряє на старті, що джерело і DLQ лежать в одному слоті, інакше червоний статус з підказкою про hash tag.
- RDS-STR-8. Backpressure: поле `backpressureScope` зі значеннями `consumer` (дефолт, власний PEL) і `group`; ліміт `maxPending`.
- RDS-STR-9. XAUTOCLAIM мертвих консюмерів після завершення власного recovery, з курсором між викликами.
- RDS-STR-10. Batching (`batchWindowMs`, `count`), rate limit, `readIntervalMs` зберігаються з поточною семантикою.
- RDS-STR-11. `startPaused` для rollout: група створена, але робота не резервується до `resume`.
- RDS-STR-12. `redkern-redis-stream-ack` читає ID з `msg.redkern.redis.id` (рядок або масив для батчу) і з legacy-полів (`msg.streamId`, `msg._streamKey`, `msg._streamGroup`); опціональний XDEL після ACK.
- RDS-STR-13. `redkern-redis-stream-gc` видаляє неактивні consumers лише з порожнім PEL.

* RDS-STR-14. Метрики палітри живуть у її власному реєстрі, оголошеному один раз через `kit.init({ metrics })` у форматі `text/plain; version=0.0.4` (KIT-MET-1, KIT-MET-2); ноди `redkern-redis-stream-metrics` беруть джерело через `k.metrics.acquire()`. З prometheus-палітрою метрики потрапляють у її `/metrics`, без неї віддаються на порту 9551 з окремим токеном `metrics`.
* RDS-STR-15. At-least-once допускає повторну доставку: після падіння пода (XAUTOCLAIM) і коли обробка триває довше за `reclaimIdleMs`, бо тоді запис може забрати інший консюмер. Тому `reclaimIdleMs` (дефолт 60 с) має бути більшим за максимальний час обробки, README вимагає ідемпотентної обробки downstream, а повторні доставки рахує метрика `redkern_redis_redeliveries_total`.

### Контракт msg

- Новий: `msg.redkern.redis = { stream, group, id, consumer, deliveries }`; для батчу `id` це масив. Палітра пише лише у свій ключ `msg.redkern.redis` і не чіпає ключів інших палітр (стандарт kit), тож у флоу kafka → redis метадані обох лишаються.
- Опція `legacyMsgFields`: дублює `msg.streamId`, `msg.stream`, `msg.group`, `msg._streamKey`, `msg._streamGroup`, `msg.consumer`. Вимкнена за замовчуванням; при ручній міграції вмикається, якщо флоу далі читає ці поля (RDS-MIG-1). Та сама опція є в command, scan, stream-out, stream-ack, stream-claim і stream-gc для полів, які виставляли їхні старі аналоги: msg.command, msg.scanComplete, msg.scanTruncated, msg.streamId, msg.stream, msg.group, msg.consumer, msg.acked, msg.deleted.

## 7. Функціональні вимоги: керування

- RDS-CTL-1. Команди `pause`, `resume`, `drain`, `status` доступні через ноду `redkern-redis-stream-control` і через HTTP.
- RDS-CTL-2. HTTP-роути реєструються через `createRouteRegistry` з kit на `httpAdmin` з правами `redkern.redis.read` (status) і `redkern.redis.write` (решта).
- RDS-CTL-3. Для preStop-хука Kubernetes: внутрішній сервер kit на порту 9551, роут `drain` з `auth: 'local'` (лише loopback, без токена). Kit 1.1.0 не дозволяє змішувати loopback і токенні роути на одному листенері (KIT-HTTP-6), тож коли на порту ввімкнено `/metrics` з токеном, `drain` вимагає власного токена `REDKERN_REDIS_DRAIN_TOKEN` (токен `metrics` його не відкриває); без нього роут `drain` вимкнено і палітра пише попередження. preStop оформлюється через `exec` з самим Node.js за готовим фрагментом з README kit (KIT-HTTP-9), який передає drain-токен, якщо він заданий: фрагмент виходить з 0, якщо порт закритий за задумом (`ENABLED=false` чи немає нод stream-in), і з ненульовим кодом на таймауті чи відмові `drain`, щоб це було видно. README палітри містить готові Helm values з цим фрагментом.
- RDS-CTL-4. `drain` зупиняє читання нових записів, відправляє вже зарезервовані (утримані паузою, у батчі, повернуті читанням, що було заблоковане на момент drain), проходить recovery власного PEL і чекає на порожній власний PEL з таймаутом `drainTimeoutMs`, після чого віддає `drained: true/false`; нода лишається на паузі. preStop-хук блокується, доки `drain` не відповість, тож README вимагає `terminationGracePeriodSeconds` більшого за `drainTimeoutMs` плюс 10 с бюджету закриття kit (KIT-LIFE-1); інакше Kubernetes вб'є под посеред drain.
- RDS-CTL-5. Помилки HTTP-роутів за KIT-HTTP-4: невалідний запит дає `PublicError` 400 з кодом, решта дає 500 лише з `{ error: 'Internal error', id }`, а повний текст з тим самим `id` іде в лог (функції `safeError` у kit 1.0.1 немає). Помилки для флоу маскуються kit у `done(err)` і `errorOutput` (KIT-SEC-3, KIT-LOG-4).

### Стан при старті процесу і dev-режим (1.1.0)

RDS-CTL-6..14 не входять у 1.0.0: в 1.0.0 стан задає лише `startPaused` на stream-in (RDS-STR-11) і команди RDS-CTL-1 для stream-in. Вимоги нижче реалізуються в 1.1.0.

Команди `pause` і `resume` (RDS-CTL-1) працюють під час роботи, але при старті процесу стан консюмерів завжди визначає env. Це потрібно для персональних dev-інстансів Node-RED: там консюмери за замовчуванням на паузі, розробник вмикає лише те, що тестує, а забуте вмикання гасне само.

- RDS-CTL-6. **Стан при старті процесу** для `redkern-redis-stream-in`, `redkern-redis-list-in` і `redkern-redis-stream-claim` береться з `REDKERN_REDIS_CONSUMERS_AUTOSTART` (`true` / `false`). Без змінної ноди стартують, щоб забута змінна в prod не зупинила обробку. Env застосовується лише тоді, коли нода з цим id створюється в процесі вперше.
- RDS-CTL-7. **Стан pause / resume живе в Map у замиканні палітри** (KIT-ARCH-2) за id ноди: переживає redeploy, не переживає рестарт процесу і ніколи не пишеться у `flows.json`, credentials чи context. Пауза після `drain` (RDS-CTL-4) зберігається так само.
- RDS-CTL-8. **Охоплення команд**: `pause` / `resume` з RDS-CTL-1 поширюються на `redkern-redis-list-in` і `redkern-redis-stream-claim`; на нодах `stream-in` і `list-in` у редакторі є кнопка, що викликає роут з правом `redkern.redis.write`.
- RDS-CTL-9. **Пауза `stream-in`**: без нових `XREADGROUP`, pending entries лишаються в групі. **Пауза `stream-claim`**: без `XAUTOCLAIM`, щоб пауза одного пода не перехоплювала чужі повідомлення.
- RDS-CTL-10. **Пауза `list-in`**: без нових `BLPOP` / `BRPOP`; заблокований виклик переривається через `CLIENT UNBLOCK` для `blocking`-з'єднання ноди (RDS-CON-2). Елемент, який повернув перерваний чи паралельний виклик, завжди віддається у флоу: pop деструктивний, повернути елемент нікуди.
- RDS-CTL-11. **Автопауза**: `REDKERN_REDIS_AUTOPAUSE_MIN` (без змінної вимкнена, задається лише на dev). Таймер стартує при `resume`; коли він спливає, виконується `pause`.
- RDS-CTL-12. **Без наздоганяння пропущеного**: при `REDKERN_REDIS_SKIP_BACKLOG_ON_START=true` перший `resume` після паузи для `stream-in` робить `XGROUP SETID <group> $`. Лише для приватних груп (dev-префікс), на спільних групах не вмикати. Для `list-in` не застосовується: накопичене в приватному списку це власні тестові дані.
- RDS-CTL-13. **Статус і аудит**: сірий `paused (env)`, сірий `paused`, сірий `auto-paused`, зелений `running` з часом до автопаузи; рядок у лозі на кожну зміну з джерелом (`env`, `control`, `button`, `autopause`); метрики `redkern_redis_consumer_enabled{node}` і `redkern_redis_consumer_state_changes_total{node,source}`.
- RDS-CTL-14. Стан діє лише на процес, що отримав команду. README прямо зазначає, що на multi-pod розгортанні це не kill switch для всього сервісу.

### Повернення з DLQ (1.2.0)

- RDS-DLQ-1. Нода `redkern-redis-stream-redrive` і роут `POST /redkern/redis/redrive` на `httpAdmin` з правом `redkern.redis.data` (KIT-AUTH-5) повертають записи з DLQ у `sourceStream`. Діапазон задається ID або часом (`XRANGE`), фільтр за `sourceGroup` і `consumer`, швидкість обмежує `ratePerSec` (дефолт 100). `dryRun` повертає кількість і метадані перших 20 записів без `payload`.
- RDS-DLQ-2. Повернення атомарне: `XADD` у джерело і `XDEL` з DLQ в одному `MULTI` (джерело і DLQ уже в одному слоті, RDS-STR-7). Новий запис несе оригінальні поля і лічильник `redrives`; запис понад `maxRedrives` (дефолт 3) лишається в DLQ і рахується як пропущений.
- RDS-DLQ-3. Кожен запуск пише аудиторський рядок у лог (хто, коли, діапазон, фільтр, скільки повернуто і пропущено) і метрику `redkern_redis_redriven_total{stream, result}`. Повернений запис це ще одна доставка at-least-once (RDS-STR-15).
- RDS-DLQ-4. Репо містить у `observability/` Grafana-дашборд і `PrometheusRule` з алертами: ріст DLQ, PEL біля `maxPending`, консюмер на паузі після rollout, ріст `redkern_redis_redeliveries_total`.

### Кореляція (1.3.0)

- RDS-TRC-1. `stream-out` робить `k.trace.inject` у поля запису (`traceparent`, `redkern-started-at`), `stream-in` робить `k.trace.extract` (KIT-TRC-2, KIT-TRC-3); DLQ і redrive зберігають ці поля.
- RDS-TRC-2. На `stream-out` inject за замовчуванням вимкнений (`traceFields: false`), бо формат полів стріму не змінюється під час міграції; вмикається окремою зміною разом із переходом зі списків на Streams.

## 8. Структура репо

```
lib/stream-consumer.js   цикл як клас з DI клієнта і годинника
lib/inflight.js  lib/dlq.js  lib/control.js  lib/denylist.js  lib/palette.js
nodes/redkern-redis.js   єдиний node set: замикання палітри (KIT-ARCH-2)
nodes/<одна нода>.js  editor/<одна нода>.html
nodes/redkern-redis.html, nodes/locales/en-US/redkern-redis.*   збираються з джерел
observability/   Grafana-дашборд і PrometheusRule (з 1.2.0, RDS-DLQ-4)
examples/  test/unit/  test/integration/  docs/spec.md
```

- RDS-NR-1. Пакет відповідає всім правилам [Node-RED Creating Nodes](https://nodered.org/docs/creating-nodes/) за переліком KIT-NR-1..17 стандарту kit (розділ 8.2); `npx node-red-dev validate` у CI без помилок (CI перевіряє, що scorecard повний: усі перевірки P01, P03-P08, N01, N02, D01-D03 є і пройдені) до публікації 1.0.0 в npm, після 1.0.0 пакет додано у Flow Library. Відступи фіксуються в цьому ТЗ з причиною (KIT-NR-17). Відступ: D03 (залежність не найновіша) допускається лише для ioredis, бо його версія закріплена (RDS-CON-2) і оновлюється тільки окремим протестованим релізом. Тека `observability/` з дашбордом і алертами (з 1.2.0) лежить поруч із `examples/`, а не в ній (KIT-NR-13). [CI]

* Вся логіка в `lib/`, тестується без Node-RED. Ноди лише маплять config і msg.
* Одна нода на файл (nodes/<тип>.js і editor/<тип>.html). Пакет реєструє один node set redkern-redis, бо стан палітри живе в замиканні module.exports = function (RED) (KIT-ARCH-2), а Node-RED дає кожному node set окремий об'єкт RED; nodes/redkern-redis.html, каталог і довідка locales/en-US/redkern-redis.* збираються з джерел, і CI падає, якщо вони застаріли. Код редактора у `.html` загорнутий в IIFE.
* Коментарі та повідомлення англійською.
* Node.js >= 22, Node-RED >= 4.1.0 < 6.0.0, `files`-whitelist, CI і release як у kit.
* RDS-UI-1. **Вигляд у редакторі** за стандартом kit (розділ 8.1): кораловий колір палітри, мотив "стопка дисків" з окремим знаком дії для кожної з 12 нод, `paletteLabel` і підписи за замовчуванням з каталогу. Підписи портів кожної ноди (зокрема виходів `scan`, `stream-in` і `stream-claim`) задаються в message catalog за кількістю портів з цього ТЗ; порт помилок з `errorOutput` останній і підписаний `error`. Діалоги `redkern-redis-config` і `stream-in` мають вкладки General, Reliability, Advanced. [CI]

- RDS-UI-2. Порти нод за таблицею нижче; порт `error` з’являється лише з `errorOutput` і завжди останній. check-editor звіряє кількість підписів із портами. [CI]

| Нода | Входи | Виходи |
| --- | --- | --- |
| command, multi, stream-out | 1: message | 1: result / results / added; + error з `errorOutput` |
| scan | 1: message | 1: items |
| subscribe | 0 | 1: message |
| list-in | 0 | 1: element |
| stream-in | 0 | 1: entries |
| stream-ack | 1: message | 0; 1 (acknowledged) з опцією "Pass the message on", при міграції її вмикають вручну |
| stream-claim | 1: message | 1: claimed |
| stream-gc | 1: message | 1: report |
| stream-metrics | 1: message | 1: sample |
| stream-control | 1: message | 1: result |

Відмінності портів від старого пакета: subscribe більше не має входу (динамічна підписка msg.subscribe прибрана), stream-ack за замовчуванням без виходу і вирівняна праворуч (стандарт kit 8.1).

## 9. Критерії приймання

- pause/resume при downstream 500 мс і навантаженні 1000 msg/s: 0 дублів, 0 записів у DLQ.
- Kill пода посеред батчу: усі записи доставлені після XAUTOCLAIM.
- Отруйне повідомлення потрапляє в DLQ рівно після `maxDeliveries`.
- Cluster: DLQ з hash tag працює, без hash tag червоний статус на старті.
- Заборонена команда з `msg` відхиляється; HTTP без прав повертає 401.
- Redeploy під навантаженням завершується менше ніж за 2 с.
- Стара і нова палітри встановлені одночасно без конфліктів типів і роутів.
- Кількість з'єднань на под відповідає формулі RDS-CON-5 і видна в `redkern_redis_connections{role}`.
- command з `concurrency: 64` обробляє 10 000 msg/s без зростання черги на тестовому Redis; переповнення `maxQueue` завершується `done(err)` з кодом `INPUT_QUEUE_FULL`.
- stream-ack з масивом ID у батчі ACK-ає всі записи одним викликом.
- У флоу kafka → redis stream-out → stream-in обидва ключі `msg.redkern.kafka` і `msg.redkern.redis` присутні.

Для 1.1.0 (RDS-CTL-6..14):

- RDS-CTL-6..14: при `REDKERN_REDIS_CONSUMERS_AUTOSTART=false` `stream-in`, `list-in` і `stream-claim` після рестарту процесу на паузі незалежно від стану до рестарту, а без змінної стартують; стан після `resume` переживає redeploy; автопауза спрацьовує за `REDKERN_REDIS_AUTOPAUSE_MIN`.
- `pause` під час заблокованого `BLPOP` не губить знятий елемент; на паузі немає ні `XREADGROUP`, ні `XAUTOCLAIM`, ні `BLPOP`.
- Зі `REDKERN_REDIS_SKIP_BACKLOG_ON_START=true` `stream-in` після паузи не віддає записів, доданих під час паузи.

## 10. Поза обсягом

- Кешування, Lua-бібліотеки, RedisJSON/Search.
- Автоматичний XTRIM: лишається в stream-gc з захистом від обрізання нижче PEL.

## 11. Етапи

| Версія | Обсяг |
| --- | --- |
| 1.0.0-alpha.1 | config, command, multi, scan, subscribe, list-in на kit |
| 1.0.0-alpha.2 | stream-in/out/ack з курсором і in-flight |
| 1.0.0-beta.1 | DLQ, claim, gc, metrics, control |
| 1.0.0-rc.1 | load-тест пройдений, README містить чекліст ручної міграції для всіх типів (RDS-MIG-1) |
| 1.0.0 | перший продакшн-реліз; перед публікацією в npm CI запускає npx node-red-dev validate (RDS-NR-1) |
| 1.1.0 | стан консюмерів при старті і dev-режим RDS-CTL-6..14: CONSUMERS_AUTOSTART, pause/resume для list-in і stream-claim, автопауза, пропуск backlog, метрики стану |
| 1.2.0 | redrive з DLQ і observability/ з дашбордом та алертами RDS-DLQ-1..4 |
| 1.3.0 | кореляція RDS-TRC-1..2 (kit 1.1.0) |

У npm публікується лише 1.0.0 і вище; pre-release виходять як GitHub Release з tarball (стандарт kit, розділ 8).

## 12. Відкриті питання

Відкритих питань немає. Ухвалено:

- HTTP control API лишається; preStop через `exec` на `127.0.0.1` (RDS-CTL-3), роути на `httpAdmin` з правами для ручного керування;
- черги на списках винесені в перехідну ноду `redkern-redis-list-in` з незмінною семантикою BLPOP/BRPOP (RDS-CMD-7); `subscribe` лише для Pub/Sub. Перехід зі списків на Streams виконується після переїзду флоу на redkern, окремими змінами.
- міграція ручна, без migrate і дескриптора (RDS-MIG-1); RDS-CTL-6..14, RDS-DLQ-1..4 і RDS-TRC-1..2 винесені з 1.0.0 у версії 1.1.0, 1.2.0 і 1.3.0.
