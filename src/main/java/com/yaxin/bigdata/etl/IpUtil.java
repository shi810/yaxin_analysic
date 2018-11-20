package com.yaxin.bigdata.etl;

import com.alibaba.fastjson.JSONObject;
import com.yaxin.bigdata.common.GlobalConstants;
import com.yaxin.bigdata.etl.ip.IPSeeker;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * ip转成国家省市
 * 121.134.223.13 -- 中国 北京 昌平区
 * 1、纯真数据库解析
 * 2、淘宝API解析
 * 3、自己收集Ip信息
 */
public class IpUtil extends IPSeeker {
    private static Logger logger = Logger.getLogger(IpUtil.class);

    private static RegionInfo info = new RegionInfo();

    public static RegionInfo getRegionInfoByIp(String ip){
        if (StringUtils.isEmpty(ip)) {
            logger.warn("解析的ip为空.");
            return info;
        }

        try {
            //通过ipSeekeer来获取ip所对应的信息  贵州省铜仁地区| 局域网
            String country = IPSeeker.getInstance().getCountry(ip);
            if (country.equals("局域网")) {
                info.setCountry("中国");
                info.setProvince("北京市");
                info.setCity("昌平区");
            } else if (!StringUtils.isEmpty(country)) {
                //查找返回的字符串中是否有省
                int index = country.indexOf("省");
                info.setCountry("中国");
                if (index > 0) {
                    //证明有省份
                    info.setProvince(country.substring(0, index + 1));
                    //查找是否有市
                    int index2 = country.indexOf("市");
                    if (index2 > 0) {
                        info.setCity(country.substring(index + 1, index2 + 1));
                    }
                } else {
                    //查找不到省
                    String flag = country.substring(0, 2);
                    switch (flag) {
                        case "内蒙":
                            //设置省份
                            info.setProvince(flag + "古");
                            country.substring(3);
                            index = country.indexOf("市");
                            if (index > 0) {
                                info.setCity(country.substring(0, index + 1));
                            }
                            break;

                        case "广西":
                        case "宁夏":
                        case "新疆":
                        case "西藏":
                            //设置省份
                            info.setProvince(flag);
                            country.substring(2);
                            index = country.indexOf("市");
                            if (index > 0) {
                                info.setCity(country.substring(0, index + 1));
                            }
                            break;

                        case "北京":
                        case "上海":
                        case "重庆":
                        case "天津":
                            info.setProvince(flag + "市");
                            country.substring(2);
                            index = country.indexOf("区");
                            if (index > 0) {
                                char ch = country.charAt(index - 1);
                                if (ch != '小' || ch != '校' || ch != '军') {
                                    info.setCity(country.substring(0, index + 1));
                                }
                            }

                            index = country.indexOf("县");
                            if (index > 0) {
                                info.setCity(country.substring(0, index + 1));
                            }
                            break;

                        case "香港":
                        case "澳门":
                        case "台湾":
                            info.setProvince(flag + "特别行政区");
                            break;

                        default:
                            break;
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("解析ip异常.", e);
        }
        return info;
    }


    /**
     * 使用淘宝的ip解析库解析ip
     *
     * @param url
     * @param charset
     * @return
     * @throws Exception
     */
    public static RegionInfo parserIp1(String url, String charset) throws Exception {
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);

        if (null == url || !url.startsWith("http")) {
            throw new Exception("请求地址格式不对");
        }
        // 设置请求的编码方式
        if (null != charset) {
            method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=" + charset);
        } else {
            method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=" + "utf-8");
        }
        //发送请求
        int statusCode = client.executeMethod(method);

        if (statusCode != HttpStatus.SC_OK) {// 打印服务器返回的状态
            System.out.println("Method failed: " + method.getStatusLine());
        }
        // 返回响应消息
        byte[] responseBody = method.getResponseBodyAsString().getBytes(method.getResponseCharSet());
        // 在返回响应消息使用编码(utf-8或gb2312)
        String response = new String(responseBody, "utf-8");
        System.out.println(response);
        /**
         * {"code":0,"data":{"ip":"0.255.255.255","country":"XX","area":"","region":"XX","city":"内网IP","county":"内网IP","isp":"内网IP","country_id":"xx","area_id":"",
         * "region_id":"xx","city_id":"local","county_id":"local","isp_id":"local"}}
         */
        //将reponse的字符串转换成json对象
        JSONObject jo = JSONObject.parseObject(response);
        JSONObject jo1 = JSONObject.parseObject(jo.getString("data"));

        //赋值
        info.setCountry(jo1.getString("country"));
        info.setProvince(jo1.getString("region"));
        info.setCity(jo1.getString("city"));
        return info;
    }

    /**
     * 封装ip解析出来的国家省市
     */
    public static class RegionInfo{
        private String defaultValue= GlobalConstants.DEFAULT_VALUE;
        private String country = defaultValue;
        private String province = defaultValue;
        private String city = defaultValue;

        public RegionInfo() {
        }

        public RegionInfo(String country, String province, String city) {
            this.country = country;
            this.province = province;
            this.city = city;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo{" +
                    "country='" + country + '\'' +
                    ", province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }
}
