package com.yaxin.bigdata.analystic.mr.seesion;

import com.yaxin.bigdata.analystic.model.StatsBaseDimension;
import com.yaxin.bigdata.analystic.model.StatsUserDimension;
import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.analystic.mr.IOutputWritter;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.common.GlobalConstants;
import com.yaxin.bigdata.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/7/30 11:07
 * @Description: sessions的ps赋值
 */
public class SessionOutputWritter implements IOutputWritter {

    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        try {
            StatsUserDimension statsUserDimension = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            int sessions = ((IntWritable) ((OutputWritable) value).
                    getValue().get(new IntWritable(-1))).get();
            int sessionsLength = ((IntWritable) ((OutputWritable) value).
                    getValue().get(new IntWritable(-2))).get();

            //为ps赋值
            int i = 0;
            ps.setInt(++i, iDimension.getDimensionIdByObject(statsUserDimension.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i, iDimension.getDimensionIdByObject(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
            if (v.getKpi().equals(KpiType.BROWSER_SESSION)) {
                ps.setInt(++i, iDimension.getDimensionIdByObject(statsUserDimension.getBrowserDimension()));
            }
            ps.setInt(++i, sessions);
            ps.setInt(++i, sessionsLength);
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i, sessions);
            ps.setInt(++i, sessionsLength);

            //添加到批处理中
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
