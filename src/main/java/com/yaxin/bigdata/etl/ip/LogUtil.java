package com.yaxin.bigdata.etl.ip;

import com.yaxin.bigdata.common.Constants;
import com.yaxin.bigdata.etl.IpUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogUtil {
    private static Logger logger = Logger.getLogger(LogUtil.class);
    /**
     * 处理整行的日志
     * @param log
     * @return
     */
    public static Map<String,String> parserLog(String log){
        Map<String,String> map = new ConcurrentHashMap<>();

        if (StringUtils.isNotEmpty(log)){
            String[] fields = log.split("\\^A");

            if (fields.length == 4){

                //存储
                map.put(Constants.LOG_IP,fields[0]);
                map.put(Constants.LOG_SERVER_TIME,fields[1].replaceAll("\\.",""));

                //参数列表，单独进行处理
                String params = fields[3];

                handleParams(params,map);

                //处理ip解析
                handleIp(map);

                //处理userAgent
                hadnleUserAgent(map);
            }
        }
        return map;
    }

    /**
     * 处理参数列表
     * /qf.png?en=e_l&ver=1&pl=website&sdk=js&u_ud=0DCFADBE-375A-4AED-AF7C-C95A7FE975B4&
     * u_sd=AD5BCC19-2067-4E22-B163-CC8EA8DAF0D4&c_time=1535616633889&
     * b_iev=Mozilla%2F4.0%20(compatible%3B%20MSIE%208.0%3B%20Windows%20NT%206.1%3B%20Win64%3B%20x64%3B%20Trident%2F4.0%3B%20.NET%20CLR%202.0.50727%3B%20SLCC2%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20.NET4.0C%3B%20.NET4.0E)&
     * b_rst=1600*900
     * @param params
     * @param map
     */
    private static void handleParams(String params, Map<String, String> map) {
        try {
            if(StringUtils.isNotEmpty(params)){
                int index = params.indexOf("?");

                if (index > 0){
                    String[] fields = params.substring(index + 1).split("&");
                    //c_time=1535616633889
                    for (String field : fields ) {
                        String[] kvs = field.split("=");
                        String k = kvs[0];
                        String v = URLDecoder.decode(kvs[1],"utf-8");

                        //判断key是否为空
                        if (StringUtils.isNotEmpty(k)){
                            //将数据存储到map中
                            map.put(k,v);
                        }
                    }
                }
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("value进行urldecode解码异常",e);
        }
    }

    /**
     * 将map中的ip取出来，然后解析成国家省市，然后再放入map中
     * @param map
     */
    private static void handleIp(Map<String, String> map) {
       if(map.containsKey(Constants.LOG_IP)){
           IpUtil.RegionInfo info = IpUtil.getRegionInfoByIp(map.get(Constants.LOG_IP));

           //将结果放入到map中
           map.put(Constants.LOG_COUNTRY,info.getCountry());
           map.put(Constants.LOG_PROVINCE,info.getProvince());
           map.put(Constants.LOG_CITY,info.getCity());
       }
    }

    /**
     * 从map中取出b_iev的值，然后解析userAgent，再存入到map中
     * @param map
     */
    private static void hadnleUserAgent(Map<String, String> map) {
        if(map.containsKey(Constants.LOG_USERAGENT)){
            userAgentUtil.userAgentInfo info = userAgentUtil.parserUserAgent(map.get(Constants.LOG_USERAGENT));
            //将值存入到map中
            map.put(Constants.LOG_BROWSER_NAME,info.getBrowserName());
            map.put(Constants.LOG_BROWSER_VERSION,info.getBrowserVersion());
            map.put(Constants.LOG_OS_NAME,info.getOsName());
            map.put(Constants.LOG_OS_VERSION,info.getOsVersion());
        }
    }
}
