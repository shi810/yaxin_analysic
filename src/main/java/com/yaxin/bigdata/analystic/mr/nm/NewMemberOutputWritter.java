/**
 *
 */
package com.yaxin.bigdata.analystic.mr.nm;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;

/**
 * 活跃用户的sql赋值
 */
public class NewMemberOutputWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(NewMemberOutputWritter.class);
    @Override
    //这里通过key和value给ps语句赋值
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {

        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            int i = 0;
            switch (v.getKpi()){
                case NEW_MEMBER:
                case BROWSER_NEW_MEMBER:
                    //获取活跃用户的值
                    int newUser = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();
                    ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
                    ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
                    //修改1
                    if(v.getKpi().equals(KpiType.BROWSER_NEW_MEMBER)){
                        ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
                    }
                    ps.setInt(++i,newUser);
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
                    ps.setInt(++i,newUser);
                    break;
                case MEMBER_INFO:
                    String memberId = ((Text)(v.getValue().get(new IntWritable(-2)))).toString();
                    long minTime = ((LongWritable)(v.getValue().get(new IntWritable(-3)))).get();
                    ps.setString(++i,memberId);
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setLong(++i,minTime);//
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    break;

                    default:
                        break;
            }
            ps.addBatch();//添加到批处理中，批量执行SQL语句
//            ps.executeBatch();
        } catch (Exception e) {
            logger.warn("给ps赋值失败！！！",e);
        }
    }
}