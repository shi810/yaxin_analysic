package com.yaxin.bigdata.analystic.mr.local;

import com.yaxin.bigdata.analystic.model.StatsBaseDimension;
import com.yaxin.bigdata.analystic.model.StatsLocationDimension;
import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.LocationReduceOutput;
import com.yaxin.bigdata.analystic.mr.IOutputWritter;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName NewUserWriter
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description local的ps的赋值
 **/
public class LocalWriter implements IOutputWritter {
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        try {
            StatsLocationDimension statsLocationDimension  = (StatsLocationDimension) key;
            LocationReduceOutput locationReduceOutput = (LocationReduceOutput) value;
            //为ps赋值
            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(statsLocationDimension.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(statsLocationDimension.getStatsCommonDimension().getPlatformDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(statsLocationDimension.getLocationDimension()));
            ps.setInt(++i,locationReduceOutput.getAus());
            ps.setInt(++i,locationReduceOutput.getSessions());
            ps.setInt(++i,locationReduceOutput.getBounce_sessions());
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,locationReduceOutput.getAus());
            ps.setInt(++i,locationReduceOutput.getSessions());
            ps.setInt(++i,locationReduceOutput.getBounce_sessions());

            //添加到批处理中
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}