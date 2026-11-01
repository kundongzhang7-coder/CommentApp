package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
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

        //利用逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

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
