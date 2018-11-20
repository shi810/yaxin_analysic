package com.yaxin.bigdata.etl.mr;

import com.yaxin.bigdata.common.Constants;
import com.yaxin.bigdata.etl.ip.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class Etl2HdfsMapper extends Mapper<LongWritable, Text,LogWritable, NullWritable> {
    private static Logger logger = Logger.getLogger(Etl2HdfsMapper.class);
    private static LogWritable k = new LogWritable();
    private static int inputRecords,filterRecords,outputRecords;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        inputRecords++;
        if (StringUtils.isEmpty(line)){
            filterRecords++;
            return;
         }

         //调用LogUtil中的parserLog方法，返回map，然后循环map将数据分别输出
        Map<String, String> map = LogUtil.parserLog(line);

        //可以将数据根据事件分别输出
        String eventName = map.get(Constants.LOG_EVENT_NAME);
        //获取到事件对应的枚举值
        Constants.EventEnum event = Constants.EventEnum.valueOfAlias(eventName);

        switch (event){
            case LANUCH:
            case PAGEVIEW:
            case EVENT:
            case CHARGESUCCESS:
            case CHARGEREQUEST:
            case CHARGEREFUND:
                handleLog(map,context);  //处理输出
                break;
            default:
                break;
        }
    }

    /**
     * 真正的处理将map中的数据进行输出
     * @param map
     * @param context
     */
    private void handleLog(Map<String, String> map, Context context) {
        try {
            for (Map.Entry<String,String> en: map.entrySet() ) {
                //this.k.setB_iev(en.getValue());
                switch (en.getKey()){
                    case "ver": this.k.setVer(en.getValue()); break;
                    case "s_time": this.k.setS_time(en.getValue()); break;
                    case "en": this.k.setEn(en.getValue()); break;
                    case "u_ud": this.k.setU_ud(en.getValue()); break;
                    case "u_mid": this.k.setU_mid(en.getValue()); break;
                    case "u_sd": this.k.setU_sd(en.getValue()); break;
                    case "c_time": this.k.setC_time(en.getValue()); break;
                    case "l": this.k.setL(en.getValue()); break;
                    case "b_iev": this.k.setB_iev(en.getValue()); break;
                    case "b_rst": this.k.setB_rst(en.getValue()); break;
                    case "p_url": this.k.setP_url(en.getValue()); break;
                    case "p_ref": this.k.setP_ref(en.getValue()); break;
                    case "tt": this.k.setTt(en.getValue()); break;
                    case "pl": this.k.setPl(en.getValue()); break;
                    case "ip": this.k.setIp(en.getValue()); break;
                    case "oid": this.k.setOid(en.getValue()); break;
                    case "on": this.k.setOn(en.getValue()); break;
                    case "cua": this.k.setCua(en.getValue()); break;
                    case "cut": this.k.setCut(en.getValue()); break;
                    case "pt": this.k.setPt(en.getValue()); break;
                    case "ca": this.k.setCa(en.getValue()); break;
                    case "ac": this.k.setAc(en.getValue()); break;
                    case "kv_": this.k.setKv_(en.getValue()); break;
                    case "du": this.k.setDu(en.getValue()); break;
                    case "browserName": this.k.setBrowserName(en.getValue()); break;
                    case "browserVersion": this.k.setBrowserVersion(en.getValue()); break;
                    case "osName": this.k.setOsName(en.getValue()); break;
                    case "osVersion": this.k.setOsVersion(en.getValue()); break;
                    case "country": this.k.setCountry(en.getValue()); break;
                    case "province": this.k.setProvince(en.getValue()); break;
                    case "city": this.k.setCity(en.getValue()); break;
                    default:break;
                }
            }
            outputRecords++;
            context.write(k,NullWritable.get());
        } catch (Exception e) {
            logger.error("etl最终输出错误",e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("inputRecords:" + inputRecords +
                "   filterRecords:" + filterRecords +
                "   outputRecords:" + outputRecords);
    }
}
