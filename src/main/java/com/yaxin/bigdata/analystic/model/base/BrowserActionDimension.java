package com.yaxin.bigdata.analystic.model.base;

import java.sql.PreparedStatement;

/**
 * @ClassName BrowserActionDimension
 * @Description TODO
 * @Author Chenfg
 * @Date 2018/11/5 0005 10:09
 * @Version 1.0
 */
public class BrowserActionDimension extends BaseActionDimension{
    @Override
    public String buildCacheKey() {
        return null;
    }

    @Override
    public String buildSqls() {
        return null;
    }

    @Override
    public void setArgs(PreparedStatement ps) {

    }
}
