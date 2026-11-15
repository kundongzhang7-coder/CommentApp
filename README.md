# hmdp · 黑马点评（骨架来源 + 工程化加固版）

> **骨架来源**：本项目基于 B 站[黑马程序员 Redis 教程](https://www.bilibili.com/video/BV1cr4y1671t)实战篇的 hmdp 项目（[原仓库](https://github.com/cs001020/hmdp)）二次开发。
> 保留的是原项目的业务模型、表结构与秒杀主链路（Redis Lua 预扣库存 + Redis Stream 异步落库 + Redisson 用户级锁）；
> 在此基础上补的是教程没有覆盖、但真实系统必须有的一层：**接口限流、落库失败补偿、消费重试边界、缓存雪崩防护、可观测性**。
> 下表所有数字都是本机单节点实测（MySQL + Redis 7.4 + Spring Boot 2.7.4），不是估算值。

## 一、相对原项目改了什么

| # | 原实现的问题 | 现在的做法 | 代码位置 |
|---|---|---|---|
| 1 | 秒杀、登录、验证码接口都没有频率限制，脚本可无限刷 | 通用 Redis 滑动窗口限流器，一次 Lua 完成"清理 → 计数 → 判定 → 记账 → 续期"；秒杀按「用户+券」10 次/10 秒，登录按 IP 10 次/10 分钟，验证码按手机号 60 秒 1 次 | `utils/RedisRateLimiter.java`、`resources/lua/rate_limit*.lua` |
| 2 | IP 直接取 `X-Forwarded-For` 第一段，请求头可伪造，限流形同虚设 | `ClientIpResolver`：只有直连地址命中可信代理白名单才采信转发头，且从右往左取第一个非可信地址；白名单为空时一律用 `remoteAddr` | `utils/ClientIpResolver.java`、`config/SecurityProperties.java` |
| 3 | 落库失败（一人一单复查命中、库存扣减失败、DB 异常）只 `log.error` 后 return，而 Lua 已经扣了库存、记了一人一单 → 库存永久少一件、该用户永远买不了 | 事务内失败一律抛异常触发回滚；消费者捕获后补偿 Redis 预占：`INCRBY seckill:stock:{voucherId}` + `SREM seckill:order:{voucherId}`，用 SETNX 标记保证只补一次；同时写 `order:fail:{orderId}` 快照（保留 7 天）便于人工核对 | `VoucherOrderServiceImpl#compensate` / `#markFailed` |
| 4 | pending-list 是 `while(true)` + 失败只 `sleep(20ms)`，一条坏消息能把消费线程永久卡死 | 每条消息累计重试次数（`order:retry:{recordId}`），超过 3 次则补偿 + 记录快照 + ACK，消费者不会被坏消息拖死 | `VoucherOrderServiceImpl#process` |
| 5 | 获取用户级锁失败只打一行日志，消息仍被 ACK，订单静默丢失 | 锁竞争失败返回 `false`、不 ACK，消息留在 pending-list 等下一轮；只有"处理完毕"（成功或已补偿）才 ACK | `VoucherOrderServiceImpl#handleVoucherOder` |
| 6 | Stream 至少一次投递 + pending 重试可能重复处理，产生重复订单 | 按订单号做幂等（`getById` 命中直接跳过）；DB 复查命中改为抛异常而不是静默返回 | `VoucherOrderServiceImpl#createVoucherOder` |
| 7 | 缓存物理 TTL 固定，批量写入的 key 会在同一秒集体失效 | `CacheClient.set` 的 TTL 加 0~60s 随机抖动 | `utils/CacheClient.java` |
| 8 | 教程演示用的 2、3 号 Redisson 客户端（6380/6381）是常驻 Bean，只跑单节点 Redis 的环境直接启动失败 | 改为 `hmdp.redis.extra-nodes-enabled` 开关，默认关闭 | `config/RedissonConfig.java`、`application.yaml` |
| 9 | 没有任何健康检查 | 引入 actuator，只暴露 `health` 与 `info` | `pom.xml`、`application.yaml` |

## 二、实测记录（本机单节点，2026-11-15）

| 验证项 | 操作 | 结果 |
|---|---|---|
| 验证码限流 | 同一手机号连续发 2 次 | 第 1 次成功；第 2 次「验证码发送过于频繁，请稍后再试」 |
| 登录 IP 限流 | 同一 IP 连续 11 次错误验证码 | 前 10 次「验证码错误」，第 11 次「操作过于频繁，请稍后再试」；`limit:login:ip:*` 的 ZCARD=10 |
| 秒杀用户限流 | 同一用户对同一张券连打 12 次 | 第 1 次返回订单号；第 2~10 次「请勿重复下单」；第 11~12 次「手速太快了，请稍后再试」 |
| 正常链路一致性 | 1 次成功秒杀 | Redis 库存 199→198、DB 库存 198、订单落库、`XPENDING=0` |
| 失败补偿 | 把 DB 库存置 0 而 Redis 保留 5，触发一次秒杀 | 落库失败 → Redis 库存回到 5、一人一单集合清空、写入 `order:fail:{orderId}`、DB 订单数不变 |
| 缓存抖动 | 连续查询 4 个不同店铺 | TTL 分别为 1825 / 1828 / 1813 / 1830 秒（基准 1800 + 0~60 抖动） |
| 健康检查 | `GET /actuator/health` | `status=UP`，db / redis / ping 均为 UP |

## 三、本地启动

1. MySQL 建库 `hmdp` 并导入 `hmdp.sql`；Redis 起在 6379（密码见 `application.yaml`）。
2. 秒杀前需要预热库存（原项目由 `VoucherServiceImpl#addSeckillVoucher` 写入，手工测试可执行）：
   ```text
   SET seckill:stock:{voucherId} {stock}
   XGROUP CREATE stream.orders g1 $ MKSTREAM
   ```
3. 打包与运行：
   ```text
   mvn clean package -DskipTests
   java -jar target/hmdp-1.0-SNAPSHOT.jar
   ```
4. 健康检查：`http://localhost:8081/actuator/health`；前端资源在 `src/main/resources/nginx-1.18.0` 下。

## 四、可调参数（`application.yaml` → `hmdp.security`）

| 参数 | 默认 | 含义 |
|---|---|---|
| `trusted-proxies` | 空 | 可信反向代理地址，支持精确 IP 与 CIDR；为空表示没有代理，完全忽略 `X-Forwarded-For` |
| `login-ip-max` | 10 | 登录接口同一 IP 在窗口内的最大尝试次数 |
| `login-window-minutes` | 10 | 登录限流窗口长度（分钟） |
| `login-user-alarm-threshold` | 20 | 同一手机号失败次数告警阈值（只告警不锁定，避免被恶意刷数锁死正常用户） |
| `seckill-user-max` | 10 | 秒杀接口同一用户对同一张券的窗口内最大请求数 |
| `seckill-window-seconds` | 10 | 秒杀限流窗口长度（秒） |

---

## 五、原始教程说明（保留）

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

### 2. 常见问题
直接使用 master 分支启动时，控制台会一直报错：
```text
NOGROUP No such key 'stream.orders' or consumer group 'g1' in XREADGROUP with GROUP option
```
这是因为完整版代码会尝试访问 Redis 的 Stream。切到 init 分支，或先执行：
```text
XGROUP CREATE stream.orders g1 $ MKSTREAM
```
