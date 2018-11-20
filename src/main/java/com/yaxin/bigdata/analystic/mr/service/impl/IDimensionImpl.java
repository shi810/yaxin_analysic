package com.yaxin.bigdata.analystic.mr.service.impl;

import com.yaxin.bigdata.Util.JdbcUtil;
import com.yaxin.bigdata.analystic.model.base.*;
import com.yaxin.bigdata.analystic.mr.service.IDimension;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 获取基础维度Id的实现
 */
public class IDimensionImpl implements IDimension {
    //定义内存缓存，用来缓存维度--维度Id
    private Map<String,Integer> cache = new LinkedHashMap<String,Integer>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    //1、根据维度对象里面的属性值，赋值给对应的sql，然后查询，如果有则返回对应维度的Id
    //  如果没有，则先添加到数据库中然后返回新增的Id号
    @Override
    public int getDimensionIdByObject(BaseDimension dimension) throws IOException, SQLException {
        Connection conn = null;
        try {
            //构造cachekey
            String cacheKey = buildCacheKey(dimension);
            //查询缓存中是否存在
            if(this.cache.containsKey(cacheKey)){
                return this.cache.get(cacheKey);
            }
            //代码走到这儿
            String sqls [] = null;
            if(dimension instanceof KpiDimension){
                sqls  = buildKpiSqls(dimension);
            } else if(dimension instanceof PlatformDimension){
                sqls = buildPlatformSqls(dimension);
            } else if(dimension instanceof DateDimension){
                sqls = buildDateSqls(dimension);
            } else if(dimension instanceof BrowserDimension){
                sqls = buildBrowserSqls(dimension);
            } else if(dimension instanceof LocationDimension){
                sqls = buildLocalSqls(dimension);
            } else if(dimension instanceof EventDimension){
                sqls = buildEventSqls(dimension);
            }

            //获取jdbc的连接
            conn = JdbcUtil.getConn();
            int id = -1;
            synchronized (this){
                id = this.executSql(conn,sqls,dimension);
            }
            //将结果存储到cache中
            this.cache.put(cacheKey,id);
            return id;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn,null,null);
        }
        throw new RuntimeException("插入基础维度类异常.");
    }


    /**
     * 构建kpi的插入和查询sql语句
     * @param dimension
     * @return
     */
    private String[] buildKpiSqls(BaseDimension dimension) {
        String insertSql = "insert into `dimension_kpi`(kpi_name) values(?)";
        String selectSql = "select id from `dimension_kpi` where kpi_name = ?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildPlatformSqls(BaseDimension dimension) {
        String insertSql = "insert into `dimension_platform`(platform_name) values(?)";
        String selectSql = "select id from `dimension_platform` where platform_name = ?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildDateSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_date`(`year`, `season`, `month`, `week`, `day`, `type`, `calendar`) VALUES(?, ?, ?, ?, ?, ?, ?)";
        String selectSql = "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildBrowserSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_browser`(`browser_name`, `browser_version`) VALUES(?,?)";
        String selectSql = "SELECT `id` FROM `dimension_browser` WHERE `browser_name` = ? AND `browser_version` = ?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildLocalSqls(BaseDimension dimension) {
        String query = "select id from `dimension_location` where `country` = ? and `province` = ? and `city` = ? ";
        String insert = "insert into `dimension_location`(`country` , `province` , `city`) values(?,?,?)";
        return new String[]{insert,query};
    }

    private String[] buildEventSqls(BaseDimension dimension) {
        String query = "select id from `dimension_event` where `category` = ? and `action` = ? ";
        String insert = "insert into `dimension_event`(`category` , `action` ) values(?,?)";
        return new String[]{insert,query};
    }

    /**
     * 构建维度key
     * @param dimension
     * @return
     */
    private String buildCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();
        if(dimension instanceof BrowserDimension){
            sb.append("browser_");
            BrowserDimension browser = (BrowserDimension)dimension;
            sb.append(browser.getBrowserName());
            sb.append(browser.getBrowserVersion());
            //browser_IE88.0
        } else if(dimension instanceof KpiDimension){
            sb.append("kpi_");
            KpiDimension kpi = (KpiDimension)dimension;
            sb.append(kpi.getKpiName());
            //kpi_new_user
        } else if(dimension instanceof DateDimension){
            sb.append("date_");
            DateDimension date = (DateDimension)dimension;
            sb.append(date.getYear());
            sb.append(date.getSeason());
            sb.append(date.getMonth());
            sb.append(date.getWeek());
            sb.append(date.getDay());
            sb.append(date.getType());
        } else if(dimension instanceof PlatformDimension){
            sb.append("platform_");
            PlatformDimension platform = (PlatformDimension)dimension;
            sb.append(platform.getPlatformName());
            //new_user
        } else if(dimension instanceof LocationDimension){
            LocationDimension local = (LocationDimension) dimension;
            sb.append("local_");
            sb.append(local.getCountry());
            sb.append(local.getProvince());
            sb.append(local.getCity());
        } else if(dimension instanceof EventDimension){
            EventDimension event = (EventDimension) dimension;
            sb.append("event_");
            sb.append(event.getCategory());
            sb.append(event.getAction());
        }
        return sb != null ? sb.toString() : null;
    }

    /**
     * 执行sql
     * @param conn
     * @param sqls
     * @param dimension
     * @return
     */
    private int executSql(Connection conn, String[] sqls, BaseDimension dimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取查询的sql
            String selectSql = sqls[1];
            ps = conn.prepareStatement(selectSql);
            this.setArgs(dimension,ps); //为查询语句赋值
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getInt(1);
            }
            //没有查询出来，就插入并返回维度的id
            ps = conn.prepareStatement(sqls[0], Statement.RETURN_GENERATED_KEYS);
            this.setArgs(dimension,ps); //为插入语句赋值
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if(rs.next()){
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null,ps,rs);
        }
        return -1;
    }

    /**
     * 赋值参数
     * @param dimension
     * @param ps
     */
    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try {
            int i = 0;
            if(dimension instanceof KpiDimension){
                KpiDimension kpi = (KpiDimension)dimension;
                ps.setString(++i,kpi.getKpiName());
            } else if(dimension instanceof DateDimension){
                DateDimension date = (DateDimension)dimension;
                ps.setInt(++i,date.getYear());
                ps.setInt(++i,date.getSeason());
                ps.setInt(++i,date.getMonth());
                ps.setInt(++i,date.getWeek());
                ps.setInt(++i,date.getDay());
                ps.setString(++i,date.getType());
                ps.setDate(++i,new Date(date.getCalendar().getTime()));
            }  else if(dimension instanceof PlatformDimension){
                PlatformDimension platform = (PlatformDimension)dimension;
                ps.setString(++i,platform.getPlatformName());
            }  else if(dimension instanceof BrowserDimension){
                BrowserDimension browser = (BrowserDimension)dimension;
                ps.setString(++i,browser.getBrowserName());
                ps.setString(++i,browser.getBrowserVersion());
            } else if(dimension instanceof LocationDimension){
                LocationDimension local = (LocationDimension) dimension;
                ps.setString(++i,local.getCountry());
                ps.setString(++i,local.getProvince());
                ps.setString(++i,local.getCity());
            } else if(dimension instanceof EventDimension){
                EventDimension event = (EventDimension) dimension;
                ps.setString(++i,event.getCategory());
                ps.setString(++i,event.getAction());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //自定输出格式OutputFormat
//    DBOutputFormat
}
