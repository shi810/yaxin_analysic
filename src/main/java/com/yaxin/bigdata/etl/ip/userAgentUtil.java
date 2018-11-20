package com.yaxin.bigdata.etl.ip;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

public class userAgentUtil {
    private static Logger logger = Logger.getLogger(userAgentUtil.class);

    private static UASparser uasParser = null;

    //初始化对象
    static {
        try {
            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.error("获取uasparser对象异常",e);
        }
    }

    public static userAgentInfo parserUserAgent(String userAgent){
        userAgentInfo info = null;
        //判断输入参数是否为空，如果不为空，则解析
        if(StringUtils.isNotEmpty(userAgent)){
            try {
                UserAgentInfo userAgentInfo = uasParser.parse(userAgent);

                if (userAgentInfo != null){
                    info = new userAgentInfo();

                    //将userAgentInfo中的值赋给info
                    info.setBrowserName(userAgentInfo.getUaFamily());
                    info.setBrowserVersion(userAgentInfo.getBrowserVersionInfo());
                    info.setOsName(userAgentInfo.getOsFamily());
                    info.setOsVersion(userAgentInfo.getOsName());
                }
            } catch (IOException e) {
                logger.error("解析userAgent异常",e);
            }
        }

        return info;
    }

    /**
     * 封装userAgent解析出来的信息
     */
    public static class userAgentInfo{
        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        public userAgentInfo() {
        }

        public userAgentInfo(String browserName, String browserVersion, String osName, String osVersion) {
            this.browserName = browserName;
            this.browserVersion = browserVersion;
            this.osName = osName;
            this.osVersion = osVersion;
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "userAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}
