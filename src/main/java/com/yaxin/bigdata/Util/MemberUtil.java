package com.yaxin.bigdata.Util;

import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @ClassName MemberUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 查询数据库中历史的会员id，如果有就返回false,没有返回true
 **/
public class MemberUtil {
    //缓存
    private static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 1000;
        }
    };

    /**
     * 检测会员ID是否合法
     *
     * @param memeberid
     * @return true是合法，false不合法
     */
    public static boolean checkMemberId(String memeberid) {
        String regex = "^[0-9a-zA-Z].*$";
        if (StringUtils.isNotEmpty(memeberid)) {
            return memeberid.trim().matches(regex);
        }
        return false;
    }

    /**
     * 是否是一个新增的会员
     *
     * @param memberId
     * @param conn
     * @param conf
     * @return true是新增会员，false不是新增会员
     */
    public static boolean isNewMember(String memberId, Connection conn, Configuration conf) {

        PreparedStatement ps = null;
        ResultSet rs = null;
        Boolean res = false;
        try {
            res = cache.get(memberId);
            if (res == null) {
                //缓存中没有，去数据库查询
                String sql = conf.get("other_" + "member_info");
                ps = conn.prepareStatement(sql);
                ps.setString(1, memberId);
                rs = ps.executeQuery();
                if (rs.next()) {
                    res = false;
                } else {
                    res = true;
                }
                //添加到cache中
                cache.put(memberId, res);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res == null ? false : res.booleanValue();
    }

    /**
     * 删除莫一天的会员，意在重新跑某一天的新增会员
     * @param conf
     */
    public static void deleteByDay(Configuration conf,Connection conn){
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(conf.get("other_delete_member_info"));
            ps.setString(1,conf.get(GlobalConstants.RUNNING_DATE));
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null,ps,null);
        }

    }
}