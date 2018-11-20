package com.yaxin.bigdata.etl.mr;

import com.yaxin.bigdata.Util.TimeUtil;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 驱动类
 * yarn jar xxx.jar com.yaxin.bigdata.etl.mr.Etl2HdfsRunner -d 2018-11-02
 */
public class Etl2HdfsRunner implements Tool {
    private static Logger logger = Logger.getLogger(Etl2HdfsRunner.class);
    private static Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();


        conf.set("mapreduce.job.jar", "F:\\IdeaProjects\\yaxin_analysic\\target\\yaxin_analysic-1.0.jar");
        //System.out.println(conf.get("yarn.resourcemanager.hostname"));
        //处理传入的参数
        handleArgs(conf,args);

        Job job = Job.getInstance(conf, "Etl2Hdfs");

        job.setJarByClass(Etl2HdfsRunner.class);

        //设置map相关的参数
        job.setMapperClass(Etl2HdfsMapper.class);
        job.setOutputKeyClass(LogWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        //设置输入输出
        handleInputoutput(job);
        return job.waitForCompletion(true)?0:1;
    }

    /**
     * 处理输入参数，判断是否有日期
     * @param conf
     * @param args
     */
    private void handleArgs(Configuration conf, String[] args) {
        String date =null;
        if (args.length > 0){
            //循环参数
            for (int i=0;i<args.length;i++) {
                //判断参数是否为-d
                if (args[i].equals("-d")){
                   date = args[i+1];
                   break;
                }
            }
        }

        //判断date是否为空，如果为空则获取昨天的日期来做为待处理的日期
        if(StringUtils.isEmpty(date)){
            date = TimeUtil.getYesterday();
        }

        //将date存储到conf
        conf.set(GlobalConstants.RUNNING_DATE,date);
    }

    /**
     * 处理输入输出
     * @param job
     */
    private void handleInputoutput(Job job) {
        //获取日期
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String[] fields = date.split("-");
        String month = fields[1];
        String day = fields[2];

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inPath = new Path("/log/" +month + "/" + day);
            Path outPath = new Path("/ods/" +month + "/" + day);

            //判断输入路径是否存在
            if(fs.exists(inPath)){
                FileInputFormat.addInputPath(job,inPath);
            }else{
                throw new RuntimeException("输入路径不存在，inpath：" + inPath.toString());
            }


            //设置输出
            if(fs.exists(outPath)){
                fs.delete(outPath,true);
            }

            FileOutputFormat.setOutputPath(job,outPath);
        } catch (IOException e) {
            logger.warn("设置输入输出路径异常",e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        conf = this.conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new Etl2HdfsRunner(),args);
        } catch (Exception e) {
            logger.error("提交job异常",e);
        }
    }
}
