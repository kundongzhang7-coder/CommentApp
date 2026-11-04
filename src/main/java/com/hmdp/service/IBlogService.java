package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

    /**
     * 根据id查询探店博文
     */
    Result queryBlogById(Long id);

    /**
     * 根据id查询探店博文
     */
    Result queryHotBlog(Integer current);

    /**
     * 更新探店博文是否点赞状态
     */
    Result likeBlog(Long id);

    Result queryBlogLikes(Long id);
}
