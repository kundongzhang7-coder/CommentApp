package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import jodd.util.StringUtil;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private CacheClient cacheClient;

    /**
     * 根据id查询商铺信息
     * @param id 商铺id
     * @return 商铺信息
     */
    @Override
    public Result queryById(Long id) {

        //缓存穿透
//        Shop shop = cacheClient
//                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //利用互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        //利用缓存穿透(旁路)方案：缓存未命中则查数据库并回填，商铺即可正常显示
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (shop == null)
            return Result.fail("商铺不存在");

        // 8.再返回商铺信息
        return Result.ok(shop);
    }

    /**
     * 利用逻辑过期解决缓存击穿，查询商铺信息
     * @param id
     * @return
     */
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//    public Shop queryWithLogicExpire(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //1.从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //2.判断缓存是否命中
//        if (StrUtil.isBlank(shopJson)) {
//            //3.不存在则直接返回
//            return null;
//        }
//
//        //4 先把JSON反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        //5.判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            //5.1未过期,则返回商铺信息
//            return shop;
//        }
//
//        //5.2过期,则进行缓存重建
//        //6.1获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//
//        //6.2判断是否获取成功
//        if (!isLock) {
//            //6.3成功,开启独立线程,实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    //重建缓存
//                    this.saveShop2Redis(id, 20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    //释放锁
//                    unlock(lockKey);
//                }
//            });
//        }
//
//        //6.4失败,返回过期商铺信息
//        return shop;
//    }

    /**
     * 利用互斥锁解决缓存击穿，查询商铺信息
     * @param id
     * @return
     */
//    public Shop queryWithMutex(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //1.从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //2.判断缓存是否命中
//        if (StrUtil.isNotBlank(shopJson)) {
//            //3.存在则直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//
//        //判断命中的是否是空值
//        if (shopJson != null) {
//            //命中空值则返回错误信息
//            return null;
//        }
//
//        //4. 实现缓存重建
//        //4.1 获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            boolean isLock = tryLock(lockKey);
//
//            //4.2判断是否获取成功
//            if (!isLock) {
//                //4.3 获取失败,休眠并重试
//                Thread.sleep(100);
//                return queryWithMutex(id);
//            }
//
//            //4.4获取成功，则根据ID查询数据库
//            shop = getById(id);
//
//            //5.判断商铺是否在数据库中存在
//            if (shop == null) {
//                //6.不存在则返回错误信息，并将null写入缓存
//                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//
//            //7.存在则将商铺数据写入Redis
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            //8. 释放互斥锁
//            unlock(lockKey);
//        }
//
//        //9.再返回商铺信息
//        return shop;
//    }

    /**
     * 利用缓存穿透查询商铺信息
     * @return
     */
//    public Shop queryWithPassThrough(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //1.从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //2.判断缓存是否命中
//        if (StrUtil.isNotBlank(shopJson)) {
//            //3.存在则直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//
//        //判断命中的是否是空值
//        if (shopJson != null) {
//            //命中空值则返回错误信息
//            return null;
//        }
//
//        //4.不存在则根据id查询数据库
//        Shop shop = getById(id);
//
//        //5.判断商铺是否在数据库中存在
//        if (shop == null) {
//            //6.不存在则返回错误信息，并将null写入缓存
//            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//
//        //7.存在则将商铺数据写入Redis
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        // 8.再返回商铺信息
//        return shop;
//    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) return Result.fail("商铺id不能为空");
        //1.更新数据库
        updateById(shop);
        //2.删除Redis缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        //3.返回更新结果
        return Result.ok();
    }

    /**
     * 排序方式：按评论数（人气）
     */
    private static final String SORT_BY_COMMENTS = "comments";
    /**
     * 排序方式：按评分
     */
    private static final String SORT_BY_SCORE = "score";
    /**
     * 附近商铺的检索半径，单位：米
     */
    private static final int GEO_SEARCH_RADIUS_METERS = 5000;

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y, String sortBy) {
        //1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            //不需要坐标：直接数据库分页；指定了排序方式就按人气/评分倒序
            String orderColumn = null;
            if (SORT_BY_COMMENTS.equals(sortBy)) {
                orderColumn = "comments";
            } else if (SORT_BY_SCORE.equals(sortBy)) {
                orderColumn = "score";
            }
            Page<Shop> page;
            if (orderColumn == null) {
                page = query()
                        .eq("type_id", typeId)
                        .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            } else {
                page = query()
                        .eq("type_id", typeId)
                        .orderByDesc(orderColumn)
                        .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            }
            // 返回数据
            return Result.ok(page.getRecords());
        }

        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        //3.查询Redis，按距离取出半径内的商铺
        String key = SHOP_GEO_KEY + typeId;
        //按距离排序时只要前 end 条即可；按人气/评分排时必须把半径内全部取出来，才能在内存里排序
        //只有明确要求按人气/评分时才走内存排序，其余情况（空串、未知值）一律按距离
        boolean sortByDistance = !SORT_BY_COMMENTS.equals(sortBy) && !SORT_BY_SCORE.equals(sortBy);
        RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs
                .newGeoRadiusArgs()
                .includeDistance()
                .sortAscending();
        if (sortByDistance) {
            args.limit(end);
        }
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key, GeoReference.fromCoordinate(x, y), new Distance(GEO_SEARCH_RADIUS_METERS), args);

        //4.解析出ID和距离
        if (results == null) return Result.ok(Collections.emptyList());
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) return Result.ok(Collections.emptyList());
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        for (GeoResult<RedisGeoCommands.GeoLocation<String>> result : list) {
            //4.1 获取shopId
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.2 获取distance
            distanceMap.put(shopIdStr, result.getDistance());
        }

        //5.按距离排序：距离序就在 Redis 结果上按下标切片，再按 id 顺序回查
        if (sortByDistance) {
            List<Long> pageIds = ids.subList(from, Math.min(end, ids.size()));
            return Result.ok(queryShopsByIdOrder(pageIds, distanceMap));
        }

        //6.按人气/评分排序：半径内的商铺全部查回来，在内存里排好序再切片
        List<Shop> shops = query().in("id", ids).list();
        Comparator<Shop> comparator;
        if (SORT_BY_COMMENTS.equals(sortBy)) {
            comparator = Comparator.comparing(Shop::getComments,
                    Comparator.nullsLast(Comparator.<Integer>reverseOrder()));
        } else {
            comparator = Comparator.comparing(Shop::getScore,
                    Comparator.nullsLast(Comparator.<Integer>reverseOrder()));
        }
        //同分时再按 id 升序，保证翻页稳定，不会重复或漏掉商铺
        shops.sort(comparator.thenComparing(Shop::getId));
        if (shops.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        List<Shop> pageShops = new ArrayList<>(shops.subList(from, Math.min(end, shops.size())));
        fillDistance(pageShops, distanceMap);
        return Result.ok(pageShops);
    }

    /**
     * 按给定的 id 顺序查回商铺（ORDER BY FIELD 保证顺序），并回填距离
     */
    private List<Shop> queryShopsByIdOrder(List<Long> ids, Map<String, Distance> distanceMap) {
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id, " + idStr + ")").list();
        fillDistance(shops, distanceMap);
        return shops;
    }

    /**
     * 把 Redis 里的距离值塞回商铺对象，前端直接拿来展示
     */
    private void fillDistance(List<Shop> shops, Map<String, Distance> distanceMap) {
        for (Shop shop : shops) {
            Distance distance = distanceMap.get(shop.getId().toString());
            if (distance != null) {
                shop.setDistance(distance.getValue());
            }
        }
    }

    /**
     * 尝试获取锁
     * @param key
     * @return
     */
//    private boolean tryLock(String key) {
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }

    /**
     * 释放互斥锁
     */
//    private void unlock(String key) {
//        stringRedisTemplate.delete(key);
//    }

//    public void saveShop2Redis(Long id, Long expireSeconds) {
//        //1.查询店铺数据
//        Shop shop = getById(id);
//        //2.封装逻辑过期时间
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//        //3.写入Redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
//    }
}
