package com.hmdp.utils;

import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * 客户端真实 IP 解析。
 * <p>
 * X-Forwarded-For 是客户端可任意伪造的请求头，直接取第一段等于把限流开关交给攻击者。
 * 这里的口径是：
 * <ol>
 *   <li>只有当直连地址本身命中可信代理白名单时，才采信转发头；</li>
 *   <li>采信时从右往左取第一个"非可信"地址——XFF 是逐跳追加的链，
 *       最左边可被客户端伪造，最右边才是离应用最近的代理写入的；</li>
 *   <li>可信代理为空时，一律使用 remoteAddr，杜绝伪造绕过。</li>
 * </ol>
 */
public final class ClientIpResolver {

    private static final String UNKNOWN = "unknown";

    private ClientIpResolver() {
    }

    public static String resolve(HttpServletRequest request, List<String> trustedProxies) {
        if (request == null) {
            return UNKNOWN;
        }
        String remoteAddr = normalize(request.getRemoteAddr());
        if (!isTrusted(remoteAddr, trustedProxies)) {
            return remoteAddr;
        }

        List<String> chain = splitChain(request.getHeader("X-Forwarded-For"));
        for (int i = chain.size() - 1; i >= 0; i--) {
            String candidate = normalize(chain.get(i));
            if (StringUtils.hasText(candidate)
                    && !UNKNOWN.equalsIgnoreCase(candidate)
                    && !isTrusted(candidate, trustedProxies)) {
                return candidate;
            }
        }

        String realIp = normalize(request.getHeader("X-Real-IP"));
        if (StringUtils.hasText(realIp)
                && !UNKNOWN.equalsIgnoreCase(realIp)
                && !isTrusted(realIp, trustedProxies)) {
            return realIp;
        }
        return remoteAddr;
    }

    private static List<String> splitChain(String header) {
        List<String> result = new ArrayList<>();
        if (!StringUtils.hasText(header)) {
            return result;
        }
        for (String part : header.split(",")) {
            if (StringUtils.hasText(part)) {
                result.add(part.trim());
            }
        }
        return result;
    }

    private static String normalize(String ip) {
        if (!StringUtils.hasText(ip)) {
            return UNKNOWN;
        }
        String value = ip.trim();
        if (value.startsWith("[") && value.contains("]")) {
            value = value.substring(1, value.indexOf(']'));
        }
        if (value.startsWith("::ffff:")) {
            value = value.substring("::ffff:".length());
        }
        if ("::1".equals(value)) {
            return "127.0.0.1";
        }
        return value;
    }

    private static boolean isTrusted(String ip, List<String> trustedProxies) {
        if (!StringUtils.hasText(ip) || trustedProxies == null || trustedProxies.isEmpty()) {
            return false;
        }
        for (String rule : trustedProxies) {
            if (!StringUtils.hasText(rule)) {
                continue;
            }
            String trimmed = rule.trim();
            if (trimmed.contains("/")) {
                if (inCidr(ip, trimmed)) {
                    return true;
                }
            } else if (trimmed.equalsIgnoreCase(ip)) {
                return true;
            }
        }
        return false;
    }

    private static boolean inCidr(String ip, String cidr) {
        String base = cidr.substring(0, cidr.indexOf('/'));
        String bitsText = cidr.substring(cidr.indexOf('/') + 1);
        Long ipValue = toIpv4(ip);
        Long baseValue = toIpv4(base);
        if (ipValue == null || baseValue == null) {
            return false;
        }
        int bits;
        try {
            bits = Integer.parseInt(bitsText.trim());
        } catch (NumberFormatException e) {
            return false;
        }
        if (bits < 0 || bits > 32) {
            return false;
        }
        long mask = bits == 0 ? 0L : (0xFFFFFFFFL << (32 - bits)) & 0xFFFFFFFFL;
        return (ipValue & mask) == (baseValue & mask);
    }

    private static Long toIpv4(String ip) {
        if (!StringUtils.hasText(ip) || ip.indexOf(':') >= 0) {
            return null;
        }
        String[] parts = ip.split("\\.");
        if (parts.length != 4) {
            return null;
        }
        long value = 0L;
        for (String part : parts) {
            try {
                int octet = Integer.parseInt(part);
                if (octet < 0 || octet > 255) {
                    return null;
                }
                value = (value << 8) | octet;
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return value;
    }
}
