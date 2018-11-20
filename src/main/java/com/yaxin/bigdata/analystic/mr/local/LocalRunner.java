package com.yaxin.bigdata.analystic.mr.local;

import com.yaxin.bigdata.Util.TimeUtil;
import com.yaxin.bigdata.analystic.model.StatsLocationDimension;
import com.yaxin.bigdata.analystic.model.value.map.LocationOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.LocationReduceOutput;
import com.yaxin.bigdata.analystic.mr.OutputToMySqlFormat;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ClassName NewUserRunner
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description local的驱动类
 *
 **/
public class LocalRunner implements Tool {
   private static final Logger logger = Logger.getLogger(LocalRunner.class);
    private Configuration conf = new Configuration();

    //主函数---入口
    public static void main(String[] args){
        try {
            ToolRunner.run(new Configuration(),new LocalRunner(),args);
        } catch (Exception e) {
            logger.warn("NEW_USER TO MYSQL is failed !!!",e);
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writter.xml");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
         //处理参数
        this.setArgs(args,conf);
        Job job = Job.getInstance(conf,"active member");
        job.setJarByClass(LocalRunner.class);

        job.setMapperClass(LocalMapper.class);
        job.setMapOutputKeyClass(StatsLocationDimension.class);
        job.setMapOutputValueClass(LocationOutputValue.class);

        //设置reducer类
        job.setReducerClass(LocalReducer.class);
        job.setOutputKeyClass(StatsLocationDimension.class);
        job.setOutputValueClass(LocationReduceOutput.class);

        //设置输出的类
        job.setOutputFormatClass(OutputToMySqlFormat.class);

        this.handleInputOutput(job);

        return job.waitForCompletion(true)?0:1;
    }


    /**
     * 参数处理,将接收到的日期存储在conf中，以供后续使用
     * @param args  如果没有传递日期，则默认使用昨天的日期
     * @param conf
     */
    private void setArgs(String[] args, Configuration conf) {
        String date = null;
        for (int i = 0;i < args.length;i++){
            if(args[i].equals("-d")){
                if(i+1 < args.length){
                    date = args[i+1];
                    break;
                }
            }
        }
        //代码到这儿，date还是null，默认用昨天的时间
        if(date == null){
            date = TimeUtil.getYesterday();
        }
        //然后将date设置到时间conf中
        conf.set(GlobalConstants.RUNNING_DATE,date);
    }

    /**
     * 设置输入输出,_SUCCESS文件里面是空的，所以可以直接读取清洗后的数据存储目录
     * @param job
     */
    private void handleInputOutput(Job job) {
        String[] fields = job.getConfiguration().get(GlobalConstants.RUNNING_DATE).split("-");
        String month = fields[1];
        String day = fields[2];

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inpath = new Path("/ods/" + month + "/" + day);
            if(fs.exists(inpath)){
                FileInputFormat.addInputPath(job,inpath);
            }else{
                throw new RuntimeException("输入路径不存在inpath" + inpath.toString());
            }
        } catch (IOException e) {
            logger.warn("设置输入输出路径异常！！！",e);
        }
    }
}