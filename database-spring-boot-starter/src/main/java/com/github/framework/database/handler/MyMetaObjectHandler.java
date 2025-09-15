package com.github.framework.database.handler;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import com.github.framework.common.enums.DelEnum;
import org.apache.ibatis.reflection.MetaObject;

import java.util.Date;

/**
 * @author Vik
 * @date 2025-09-04
 * @description
 */
public class MyMetaObjectHandler implements MetaObjectHandler {

    /**
     * 数据新增时填充
     *
     * @param metaObject
     */
    @Override
    public void insertFill(MetaObject metaObject) {
        this.strictInsertFill(metaObject, "createTime", Date.class, new Date());
        this.strictInsertFill(metaObject, "updateTime", Date.class, new Date());
        this.strictInsertFill(metaObject, "delFlag", Integer.class, DelEnum.NORMAL.code());
    }

    /**
     * 数据修改时填充
     *
     * @param metaObject
     */
    @Override
    public void updateFill(MetaObject metaObject) {
        this.strictInsertFill(metaObject, "updateTime", Date.class, new Date());
    }
}
