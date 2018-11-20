package com.yaxin.bigdata.analystic.mr.local;

import com.yaxin.bigdata.analystic.model.StatsLocationDimension;
import com.yaxin.bigdata.analystic.model.value.map.LocationOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.LocationReduceOutput;
import com.yaxin.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName NewUserReducer
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description local的reducer类
 **/
public class LocalReducer extends Reducer<StatsLocationDimension, LocationOutputValue,
        StatsLocationDimension, LocationReduceOutput> {

    private static final Logger logger = Logger.getLogger(LocalReducer.class);
    private StatsLocationDimension k = new StatsLocationDimension();
    private LocationReduceOutput v = new LocationReduceOutput();
    private Set<String> unique = new HashSet<String>();  //用于uuid的去重统计
    private Map<String,Integer> map = new HashMap<String,Integer>();

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<LocationOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空set
        this.unique.clear();
        //循环
        for (LocationOutputValue lv: values){
            if(StringUtils.isNotEmpty(lv.getUid().trim())){
                this.unique.add(lv.getUid());//循环将uuid添加set中
            }

            if(StringUtils.isNotEmpty(lv.getSid().trim())){
                if(map.containsKey(lv.getSid())){
                    this.map.put(lv.getSid(),2); //不是跳出会话个数
                } else {
                    this.map.put(lv.getSid(),1);   //跳出会话个数
                }
            }
        }

        //构造输出value
        this.v.setAus(this.unique.size());
        this.v.setSessions(this.map.size());
        int bounceSessions = 0;
        for (Map.Entry<String,Integer> en:map.entrySet()){
            if(en.getValue() == 1){
                bounceSessions ++;
            }
        }
        this.v.setBounce_sessions(bounceSessions);
        //设置kpi
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //输出
        context.write(key,this.v);
    }
}