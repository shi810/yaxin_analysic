package com.yaxin.bigdata.analystic.mr;

import com.yaxin.bigdata.Util.JdbcUtil;
import com.yaxin.bigdata.analystic.model.StatsBaseDimension;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.analystic.mr.service.impl.IDimensionImpl;
import com.yaxin.bigdata.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName OutputToMySqlFormat
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 将结果输出到mysql的自定义类
 **/
public class OutputToMySqlFormat extends OutputFormat<StatsBaseDimension, OutputWritable> {

    //DBOutputFormat

    /**
     *
     * 获取输出记录
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<StatsBaseDimension, OutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
       Connection conn = JdbcUtil.getConn();
       Configuration conf = taskAttemptContext.getConfiguration();
       IDimension iDimension = new IDimensionImpl();
        return new OutputToMysqlRecordWritter(conf,conn,iDimension);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //检测输出空间
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(null,taskAttemptContext);
    }


    /**
     * 用于封装写出记录到mysql的信息
     */
    public static class OutputToMysqlRecordWritter extends RecordWriter<StatsBaseDimension,OutputWritable>{
        Configuration conf = null;
        Connection conn = null;
        IDimension iDimension = null;
        //存储kpi-ps
        private Map<KpiType,PreparedStatement> map = new HashMap<KpiType,PreparedStatement>();
        //存储kpi-对应的输出sql
        private Map<KpiType,Integer> batch = new HashMap<KpiType,Integer>();

        public OutputToMysqlRecordWritter(Configuration conf, Connection conn, IDimension iDimension) {
            this.conf = conf;
            this.conn = conn;
            this.iDimension = iDimension;
        }

        /**
         * 写
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(StatsBaseDimension key, OutputWritable value) throws IOException, InterruptedException {
            //获取kpi
            KpiType kpi = value.getKpi();
            PreparedStatement ps = null;
            try {
                //获取ps
                if(map.containsKey(kpi)){
                    ps = map.get(kpi);
                } else {
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi,ps);  //将新增加的ps存储到map中
                }
                int count = 1;
                this.batch.put(kpi,count);
                count++;

                //为ps赋值准备
                String calssName = conf.get("writter_"+kpi.kpiName);
                //com.yaxin.bigdata.analystic.mr.nu.NewUserOutputWritter
                Class<?> classz = Class.forName(calssName); //将报名+类名转换成类
                IOutputWritter writter = (IOutputWritter)classz.newInstance();
                //调用IOutputWritter中的output方法
                writter.output(conf,key,value,ps,iDimension);

                //对赋值好的ps进行执行t
                if(batch.size()%50 == 0 || batch.get(kpi)%50 == 0){  //有50个ps执行
                    ps.executeBatch();  //批量执行
                    //this.conn.commit(); //提交批处理执行
                    batch.remove(kpi); //将执行完的ps移除掉
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        /**
         *
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType,PreparedStatement> en:map.entrySet()){
                    en.getValue().executeBatch(); //将剩余的ps进行执行
//                    this.conn.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                for (Map.Entry<KpiType,PreparedStatement> en:map.entrySet()){
                    JdbcUtil.close(conn,en.getValue(),null); //关闭所有能关闭的资源
                }
            }
        }
    }
}