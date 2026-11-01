package com.hmdp.utils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.springframework.web.servlet.HandlerInterceptor;

public class LoginInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.判断是否需要拦截(ThreadLocal当中是否有用户 )
        if (UserHolder.getUser() == null) {
            //2.如果不存在，拦截
            response.setStatus(401);
            return false;
        }

        //存在，放行
        return true;
    }
}
