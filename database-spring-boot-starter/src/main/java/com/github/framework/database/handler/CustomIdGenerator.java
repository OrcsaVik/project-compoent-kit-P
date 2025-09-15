package com.github.framework.database.handler;

import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;
import com.github.framework.distributedid.utils.SnowflakeIdUtil;

/**
 * @author Vik
 * @date 2025-09-04
 * @description
 */
public class CustomIdGenerator implements IdentifierGenerator {

    @Override
    public Number nextId(Object entity) {
        return SnowflakeIdUtil.nextId();
    }
}
