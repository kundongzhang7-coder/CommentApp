# hmdp · 黑马点评（骨架来源 + 工程化加固版）

> **骨架来源**：本项目基于 B 站[黑马程序员 Redis 教程](https://www.bilibili.com/video/BV1cr4y1671t)实战篇的 hmdp 项目（[原仓库](https://github.com/cs001020/hmdp)）二次开发。
> 保留的是原项目的业务模型、表结构与秒杀核心链路（Redis Lua 预扣库存 + 异步落库 + Redisson 用户级锁 + 优惠券一人一单）；
> 在此基础上补的是教程没有覆盖、但真实系统必须有的一层：**接口限流、消息队列与死信补偿、缓存雪崩防护、可观测性**，并把依赖升到当前主流版本。
> 表中所有数字都是本机单节点实测（MySQL + Redis 7.4 + RabbitMQ 4.4 + Spring Boot 3.5.16），不是估算值。

## 一、相对原项目改了什么

| # | 原实现的问题 | 现在的做法 | 代码位置 |
|---|---|---|---|
| 1 | 秒杀、登录、验证码接口都没有频率限制，脚本可无限刷 | 通用 Redis 滑动窗口限流器，一次 Lua 完成"清理 → 计数 → 判定 → 记账 → 续期"；秒杀按「用户+券」10 次/10 秒，登录按 IP 10 次/10 分钟，验证码按手机号 60 秒 1 次 | `utils/RedisRateLimiter.java`、`resources/lua/rate_limit*.lua` |
| 2 | IP 直接取 `X-Forwarded-For` 第一段，请求头可伪造，限流形同虚设 | `ClientIpResolver`：只有直连地址命中可信代理白名单才采信转发头，且从右往左取第一个非可信地址；白名单为空时一律用 `remoteAddr` | `utils/ClientIpResolver.java`、`config/SecurityProperties.java` |
| 3 | 秒杀消息用 Redis Stream 传递，队列的可靠性（持久化、投递确认、失败路由）全靠应用自己维护 pending-list 与重试计数 | 改用 RabbitMQ：持久化队列 + 发布确认 + 手动 ACK + 死信队列，消息不丢的责任交给 broker | `config/RabbitMqConfig.java`、`mq/` 包 |
| 4 | 落库失败只 `log.error` 后 return，而 Lua 已经扣了库存、记了一人一单 → 库存永久少一件、该用户永远买不了 | 事务内失败抛异常触发回滚；补偿统一由 `SeckillCompensator` 执行：`INCRBY seckill:stock:{id}` + `SREM seckill:order:{id}`，SETNX 标记保证只补一次，并写 `order:fail:{orderId}` 快照（7 天） | `mq/SeckillCompensator.java` |
| 5 | 消费失败没有次数上限，一条坏消息能反复重投 | 消费端本地重试 3 次（退避 500ms / 1000ms），仍失败则 `nack(requeue=false)` 进死信队列，由 DLQ 消费者补偿并 ACK | `mq/SeckillOrderConsumer.java`、`mq/SeckillDeadLetterConsumer.java` |
| 6 | 重复投递会产生重复订单 | 按订单号做幂等（`getById` 命中直接跳过）；DB 一人一单复查命中改为抛异常而不是静默返回 | `VoucherOrderServiceImpl#createVoucherOder` |
| 7 | "接口返回成功"与"消息真的入队"不同步，可能给前端一个假订单号 | 生产者开启 `publisher-confirm-type=correlated` + `mandatory` + `returns`，逐条等 broker 确认；失败就地回滚预占并返回失败 | `mq/SeckillOrderProducer.java` |
| 8 | 缓存物理 TTL 固定，批量写入的 key 会在同一秒集体失效 | `CacheClient.set` 的 TTL 加 0~60s 随机抖动 | `utils/CacheClient.java` |
| 9 | 教程演示用的 2、3 号 Redisson 客户端（6380/6381）是常驻 Bean，只跑单节点 Redis 的环境直接启动失败 | 改为 `hmdp.redis.extra-nodes-enabled` 开关，默认关闭 | `config/RedissonConfig.java`、`application.yaml` |
| 10 | 没有任何健康检查 | 引入 actuator，只暴露 `health` 与 `info` | `pom.xml`、`application.yaml` |

## 二、消息队列设计（RabbitMQ）

```
生产者（秒杀接口线程）
   │ convertAndSend + CorrelationData（等确认）
   ▼
seckill.order.exchange ──routingKey: seckill.order──> seckill.order.queue（durable，2 消费者，prefetch=1）
                                                          │ 本地重试 3 次仍失败 → basicNack(requeue=false)
                                                          ▼
                                            seckill.order.dlx ──> seckill.order.dlq（1 消费者）
                                                                   补偿 Redis 预占 + 写失败快照 + ACK
```

四个可靠性措施，缺一条就会出现资损或死循环：

1. **持久化**：交换机、队列、消息全部 durable / PERSISTENT，broker 重启后未消费任务仍在。
2. **发布确认**：`correlated` + `mandatory` + `returns`，逐条拿 confirm；ack 为 false、3 秒超时、或消息被退回（没有队列匹配路由键）都视为投递失败，就地回滚 Redis 预占。
3. **手动 ACK + prefetch=1**：落库成功才 ACK；宕机时 broker 最多重投 1 条未确认消息，配合订单号幂等不会重复下单。
4. **死信兜底**：重试 3 次仍失败 → nack → 死信消费者统一补偿（库存回加、一人一单移除、失败快照），处理完 ACK，不让死信无限堆积。

**为什么不再用 Redis Stream**：一条链路只应有一个队列。Stream 方案要做的事（未确认消息扫描、重试计数、ACK 语义）与 RabbitMQ 完全重叠，两套并存只会带来重复下单和两份状态；而且 Stream 的可靠性要靠应用自己写扫描线程维持，RabbitMQ 由 broker 保证。Stream 版本保留在历史提交 `5b9d3e1` 里，需要对比时可以回看。

`seckill.lua` 相应调整：只做「库存判断 + 一人一单判断 + 扣减 + 标记」四步原子操作，不再 `XADD`——Lua 只能写 Redis，没法参与发布确认，消息投递必须交给 Java 侧的 `SeckillOrderProducer`。

## 三、实测记录（2026-11-22，RabbitMQ 4.4.0）

| 验证项 | 操作 | 结果 |
|---|---|---|
| 正常链路 | 登录后秒杀券 13 一次 | 返回订单号 `561774837484224513`；Redis 与 DB 库存同步 198→197；订单落库（user 1019）；`seckill.order.queue` 与 `seckill.order.dlq` 积压均为 0 |
| 队列声明 | 应用启动后 `rabbitmqctl list_queues` | `seckill.order.queue` 2 个消费者、`seckill.order.dlq` 1 个消费者；`seckill.order.exchange`、`seckill.order.dlx` 均为 direct |
| **死信补偿** | DB 库存置 0、Redis 留 5，触发一次秒杀 | 消费端第 1/2/3 次连续失败（间隔 500ms、1000ms），nack 后进死信；死信消费者补偿：Redis 库存回到 **5**、一人一单移除该用户、写入 `order:fail:561774906203701250`、订单数不变（2）、两队列积压归 0 |
| **broker 宕机** | 停掉 RabbitMQ 后再秒杀 | 接口在 3 秒内返回「下单繁忙，请稍后再试」；Redis 库存保持 5（预占已回滚）、一人一单无残留、写入失败快照 |
| **broker 恢复** | 重启 RabbitMQ | 消费者自动重连（队列 2 / 死信 1），再次下单成功，Redis 与 DB 库存 5→4 一致，队列积压 0 |
| 验证码限流 | 同一手机号连续发 2 次 | 第 1 次成功；第 2 次「验证码发送过于频繁，请稍后再试」 |
| 登录 IP 限流 | 同一 IP 连续 11 次错误验证码 | 前 10 次「验证码错误」，第 11 次「操作过于频繁」；`limit:login:ip:*` 的 ZCARD=10 |
| 秒杀用户限流 | 同一用户对同一张券连打 12 次 | 第 1 次返回订单号，第 2~10 次「请勿重复下单」，第 11~12 次「手速太快了」 |
| 缓存抖动 | 连续查询 4 个不同店铺 | 4 个 key 的 TTL 各不相同（基准 1800 + 0~60 抖动，实测落在 1813~1847 区间） |
| 健康检查 | `GET /actuator/health` | `status=UP`，db / redis / ping 均 UP |

## 四、依赖版本（2026-11 升级）

| 组件 | 升级前 | 升级后 |
|---|---|---|
| Spring Boot | 2.7.4 | **3.5.16** |
| JDK | properties 写 source/target 8、插件里又写 16 | **release 17** |
| MyBatis-Plus | mybatis-plus-boot-starter 3.5.2 | **mybatis-plus-spring-boot3-starter 3.5.12** |
| Redisson | 3.16.3 | **3.52.0**（不用 starter，避免自动配置再多出一个默认客户端） |
| 消息队列 | 无（用 Redis Stream 代偿） | **spring-boot-starter-amqp**（BOM 托管，broker 为 RabbitMQ 4.4.0） |
| Hutool | 5.8.8 | **5.8.48** |
| MySQL 驱动 | mysql:mysql-connector-java | **com.mysql:mysql-connector-j**（版本由 Boot BOM 管理） |
| Lombok | 隐式 1.18.x | **1.18.48**，显式挂 `annotationProcessorPaths` |

只改 pom 是起不来的，版本升级必须同步处理这四点：

1. **`javax.*` → `jakarta.*`**：`javax.servlet.http.HttpServletRequest/HttpSession/HttpServletResponse` 与 `javax.annotation.Resource/PostConstruct`，涉及 14 个文件。
2. **配置前缀 `spring.redis.*` → `spring.data.redis.*`**：不改密码不生效，启动即 `NOAUTH Authentication required`。
3. **MyBatis-Plus 换 boot3 专用 starter**；3.5.9 之后 `PaginationInnerInterceptor` 依赖的 jsqlparser 被拆包，需额外引入 `mybatis-plus-jsqlparser`。
4. **Lombok 显式声明注解处理器**：JDK 23 起 javac 不再自动发现 classpath 上的 processor，不配 `annotationProcessorPaths` 时所有 `@Data` 实体会报「找不到 getXxx()」。

为什么不升 Spring Boot 4.x：MyBatis-Plus 目前只有 boot3 适配，Boot 4 对应 Spring Framework 7，升上去会卡在持久层。
为什么 MyBatis-Plus 停在 3.5.12：3.5.18 把 `IService/ServiceImpl` 迁到了 `com.baomidou.mybatisplus.spring.service`，会迫使重写十几个 service 的 import。

## 五、本地启动

1. MySQL 建库 `hmdp` 并导入 `hmdp.sql`；Redis 起在 6379（密码见 `application.yaml`）。
2. RabbitMQ 起在 5672（管理台 15672，`guest/guest`）。本机脚本：`D:\software\start-rabbitmq.bat`。交换机与队列由应用启动时自动声明，无需手工创建。
3. 秒杀前需要预热库存（原项目由 `VoucherServiceImpl#addSeckillVoucher` 写入，手工测试可执行）：
   ```text
   SET seckill:stock:{voucherId} {stock}
   ```
4. 打包与运行：
   ```text
   mvn clean package -DskipTests
   java -jar target/hmdp-1.0-SNAPSHOT.jar
   ```
5. 健康检查：`http://localhost:8081/actuator/health`；前端资源在 `src/main/resources/nginx-1.18.0` 下（Nginx 8080 托管，`/api` 反代到 8081）。

## 六、可调参数

`hmdp.security.*`

| 参数 | 默认 | 含义 |
|---|---|---|
| `trusted-proxies` | 空 | 可信反向代理地址，支持精确 IP 与 CIDR；为空表示没有代理，完全忽略 `X-Forwarded-For` |
| `login-ip-max` | 10 | 登录接口同一 IP 在窗口内的最大尝试次数 |
| `login-window-minutes` | 10 | 登录限流窗口长度（分钟） |
| `login-user-alarm-threshold` | 20 | 同一手机号失败次数告警阈值（只告警不锁定） |
| `seckill-user-max` | 10 | 秒杀接口同一用户对同一张券的窗口内最大请求数 |
| `seckill-window-seconds` | 10 | 秒杀限流窗口长度（秒） |

`hmdp.mq.*`

| 参数 | 默认 | 含义 |
|---|---|---|
| `retry-max` | 3 | 消费端本地重试次数，超过后 nack 进死信队列 |
| `retry-backoff-millis` | 500 | 重试退避基数，第 n 次重试等待 n * 该值 |

`spring.rabbitmq.*` 支持环境变量覆盖（`RABBITMQ_HOST` / `RABBITMQ_PORT` / `RABBITMQ_USER` / `RABBITMQ_PASSWORD`）。

---

## 七、原始教程说明（保留）

项目代码包含 2 个分支：
- master : 主分支，包含完整版代码，作为大家的编码参考使用
- init : 初始化分支，实战篇的初始代码，建议大家以这个分支作为自己开发的基础代码
- 前端资源在 src/main/resources/nginx-1.18.0 下

视频地址：
- [黑马程序员 Redis 入门到实战教程，深度透析 redis 底层原理 + redis 分布式锁 + 企业解决方案 + redis 实战](https://www.bilibili.com/video/BV1cr4y1671t)
- P24 起 实战篇

### 1. 下载
```git
git clone https://github.com/cs001020/hmdp.git
git checkout init
```
