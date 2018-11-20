package com.yaxin.bigdata.analystic.mr.service;

import com.yaxin.bigdata.analystic.model.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 根据维度获取对应的Id的接口
 */
public interface IDimension {
    int getDimensionIdByObject(BaseDimension dimension) throws IOException, SQLException;
}
