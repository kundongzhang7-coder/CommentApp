package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> queryTypeList() {
        // 1. 先从redis缓存查询（list结构，每个元素是一条店铺类型的JSON字符串）
        List<String> shopTypeStrList = stringRedisTemplate
                .opsForList().range(RedisConstants.CACHE_SHOP_TYPE_KEY, 0, -1);

        // 2. 缓存命中，把JSON字符串反序列化为对象返回
        if (shopTypeStrList != null && !shopTypeStrList.isEmpty()) {
            return shopTypeStrList.stream()
                    .map(str -> JSONUtil.toBean(str, ShopType.class))
                    .collect(Collectors.toList());
        }

        // 3. 缓存未命中，查数据库并按sort排序
        List<ShopType> typeList = query().orderByAsc("sort").list();

        // 4. 数据库也没有数据，直接返回空列表
        if (typeList == null || typeList.isEmpty()) {
            return typeList;
        }

        // 5. 查到数据，序列化为JSON字符串写入redis缓存（从list尾部依次放入）
        List<String> shopTypeJsonList = typeList.stream()
                .map(JSONUtil::toJsonStr)
                .collect(Collectors.toList());
        stringRedisTemplate.opsForList().rightPushAll(RedisConstants.CACHE_SHOP_TYPE_KEY, shopTypeJsonList);

        // 6. 返回结果
        return typeList;
    }
}