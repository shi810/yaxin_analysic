package com.yaxin.bigdata.analystic.hive;

import com.yaxin.bigdata.analystic.model.base.PlatformDimension;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.analystic.mr.service.impl.IDimensionImpl;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ClassName EventDimensionUdf
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 获取平台维度的Id
 **/
public class PlatformDimensionUdf extends UDF {

    IDimension iDimension = new IDimensionImpl();

    /**
     * @return  事件维度的id
     */
    public int evaluate(String platform){

        if(StringUtils.isEmpty(platform)){
            platform = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;

        try {
            PlatformDimension pl = new PlatformDimension(platform);
            id = iDimension.getDimensionIdByObject(pl);
        } catch (Exception e) {
           e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new PlatformDimensionUdf().evaluate("website"));
    }

}