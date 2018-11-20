/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: NewUserReducer
 * Author:   14751
 * Date:     2018/9/20 17:38
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间         版本号            描述
 */
package com.yaxin.bigdata.analystic.mr.au;

import com.yaxin.bigdata.Util.TimeUtil;
import com.yaxin.bigdata.analystic.model.StatsUserDimension;
import com.yaxin.bigdata.analystic.model.value.map.TimeOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.common.DateEnum;
import com.yaxin.bigdata.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 活跃的用户的reducer类
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension,TimeOutputValue,StatsUserDimension,OutputWritable> {
    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set unique = new HashSet();//用于去重，利用HashSet
    private MapWritable map = new MapWritable();

    //========================小时统计
    private Map<Integer,Set<String>> hourlyMap = new HashMap<Integer, Set<String>>();
    private MapWritable houlyWritable = new MapWritable();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化按小时的容器
        for(int i = 0; i < 24 ; i++){
            this.hourlyMap.put(i,new HashSet<String>());
            this.houlyWritable.put(new IntWritable(i),new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            for(TimeOutputValue tv : values){//循环
                this.unique.add(tv.getId());//将uuid取出添加到set中进行去重操作

                //统计小时的活跃用户
                if(key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USER.kpiName)) {
                    //按小时的
                    int hour = TimeUtil.getDateInfo(tv.getTime(), DateEnum.HOUR);
                    this.hourlyMap.get(hour).add(tv.getId());
                }
            }

            //按小时统计
            if(key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USER.kpiName)){
                for (Map.Entry<Integer,Set<String>> en : hourlyMap.entrySet()){
                    //构造输出的value
                    this.houlyWritable.put(new IntWritable(en.getKey()),new IntWritable(en.getValue().size()));
                }
                this.v.setKpi(KpiType.HOURLY_ACTIVE_USER);
                this.v.setValue(this.houlyWritable);
                context.write(key,this.v);
            }


            //构造输出的value
            //根据kpi别名获取kpi类型（比较灵活） --- 第一种方法
            this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
            System.out.println(this.v);

            //通过集合的size统计新增用户uuid的个数，前面的key可以随便设置，就是用来标识新增用户个数的（比较难理解）
            this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
            this.v.setValue(this.map);
            //输出
            context.write(key,this.v);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            this.unique.clear();
            this.hourlyMap.clear();
            this.houlyWritable.clear();
            for(int i = 0; i < 24 ; i++){
                this.hourlyMap.put(i,new HashSet<String>());
                this.houlyWritable.put(new IntWritable(i),new IntWritable(0));
            }
        }

        /**
         * 注意点：
         * 如果只是输出到文件系统中，则不需要kpi，不需要声明集合map
         * value只需要uuid的个数，这就不要封装对象了
         */
    }
}