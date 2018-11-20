package com.yaxin.bigdata.analystic.mr;

import com.yaxin.bigdata.analystic.model.StatsBaseDimension;
import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

/**
 * 操作结果表的接口
 */
public interface IOutputWritter {

    /**
     * 为每一个kpi的最终结果赋值的接口
     * @param conf
     * @param key
     * @param value
     * @param ps
     * @param iDimension
     */
    void output(Configuration conf,
                StatsBaseDimension key,
                StatsOutputValue value,
                PreparedStatement ps,
                IDimension iDimension);
}
