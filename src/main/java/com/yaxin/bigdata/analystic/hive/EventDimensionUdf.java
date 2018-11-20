package com.yaxin.bigdata.analystic.hive;

import com.yaxin.bigdata.analystic.model.base.EventDimension;
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
 * @Description 获取事件维度的Id
 **/
public class EventDimensionUdf extends UDF {

    IDimension iDimension = new IDimensionImpl();

    /**
     *
     * @param category
     * @param action
     * @return  事件维度的id
     */
    public int evaluate(String category,String action){
        if(StringUtils.isEmpty(category)){
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(action)){
            action = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;

        try {
            EventDimension ed = new EventDimension(category,action);
            id = iDimension.getDimensionIdByObject(ed);
        } catch (Exception e) {
           e.printStackTrace();
        }
        return id;
    }


}