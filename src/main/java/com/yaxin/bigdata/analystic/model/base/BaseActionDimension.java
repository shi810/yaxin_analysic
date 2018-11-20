package com.yaxin.bigdata.analystic.model.base;

import java.sql.PreparedStatement;

/**
 * @ClassName BaseActionDimension
 * @Description TODO
 * @Author Chenfg
 * @Date 2018/11/5 0005 10:09
 * @Version 1.0
 */
public abstract class BaseActionDimension {
    protected static BaseDimension dimension;

    public abstract  String buildCacheKey();

    public  abstract String buildSqls();

    public abstract  void setArgs(PreparedStatement ps);
}
