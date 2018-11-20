package com.yaxin.bigdata.analystic.hive;

import com.yaxin.bigdata.Util.TimeUtil;
import com.yaxin.bigdata.analystic.model.base.DateDimension;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.analystic.mr.service.impl.IDimensionImpl;
import com.yaxin.bigdata.common.DateEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ClassName DateDimensionUdf
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 获取时间维度id
 **/
public class DateDimensionUdf extends UDF {

    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String date){
        if(StringUtils.isEmpty(date)){
            date = TimeUtil.getYesterday();
        }
        DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parseString2Long(date), DateEnum.DAY);
        int id = 0;
        try {
            id = iDimension.getDimensionIdByObject(dateDimension);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new DateDimensionUdf().evaluate("2018-09-19"));
    }
}