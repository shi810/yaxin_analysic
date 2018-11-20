package com.yaxin.bigdata.analystic.mr.seesion;

import com.yaxin.bigdata.analystic.model.StatsCommonDimension;
import com.yaxin.bigdata.analystic.model.StatsUserDimension;
import com.yaxin.bigdata.analystic.model.base.BrowserDimension;
import com.yaxin.bigdata.analystic.model.base.DateDimension;
import com.yaxin.bigdata.analystic.model.base.KpiDimension;
import com.yaxin.bigdata.analystic.model.base.PlatformDimension;
import com.yaxin.bigdata.analystic.model.value.map.TimeOutputValue;
import com.yaxin.bigdata.common.DateEnum;
import com.yaxin.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/27 10:27
 * @Description: session的个数和时长
 */
public class SessionMapper extends Mapper<LongWritable,Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(SessionMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension sessionKpi = new KpiDimension(KpiType.SESSION.kpiName);
    private KpiDimension browserSessionKpi = new KpiDimension(KpiType.BROWSER_SESSION.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取需要的字段
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return ;
        }

        //拆分
        String[] fields = line.split("\u0001");

        //获取想要的字段
        String serverTime = fields[1];
        String platform = fields[13];
        String sessionId = fields[5];
        String browserName = fields[24];
        String browserVersion = fields[25];

        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(sessionId) || sessionId.equals("null")) {
            logger.info("serverTime & memberId is null serverTime:" + serverTime + ".sessionId" + sessionId);
            return;
        }
        logger.info("inputData========:"+serverTime+" "+platform+" "+sessionId+" "+browserName+" "+browserVersion);


        //构建输出value
        long stime = Long.valueOf(serverTime);
        this.v.setId(sessionId);
        this.v.setTime(stime);  //一定要设置


        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);


        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //为statsCommonDimension赋值
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        BrowserDimension defaultBrowser = new BrowserDimension("", "");

        //用户模块新增用户
        //设置默认的浏览器对象(因为新增用户指标并不需要浏览器维度，所以赋值为空)
        BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
        statsCommonDimension.setKpiDimension(sessionKpi);
        this.k.setBrowserDimension(defaultBrowserDimension);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.v.setId(sessionId);
        context.write(this.k, this.v);//输出

        //浏览器模块新增用户
        statsCommonDimension.setKpiDimension(browserSessionKpi);
        BrowserDimension browserDimension = new BrowserDimension(browserName, browserVersion);
        this.k.setBrowserDimension(browserDimension);
        this.k.setStatsCommonDimension(statsCommonDimension);
        logger.info("mapper================"+this.k.toString());
        context.write(this.k, this.v);//输出
    }
}
