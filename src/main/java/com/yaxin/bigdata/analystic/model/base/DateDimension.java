package com.yaxin.bigdata.analystic.model.base;

import com.yaxin.bigdata.Util.TimeUtil;
import com.yaxin.bigdata.common.DateEnum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * 时间维度
 */
public class DateDimension extends BaseDimension {
    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private String type;
    private Date calendar = new Date();

    public DateDimension() {
    }

    public DateDimension(int year, int season, int month, int week, int day, String type, Date calendar) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.type = type;
        this.calendar = calendar;
    }

    public DateDimension(int id, int year, int season, int month, int week, int day, String type, Date calendar) {
        this.id = id;
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.type = type;
        this.calendar = calendar;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeInt(year);
        dataOutput.writeInt(season);
        dataOutput.writeInt(month);
        dataOutput.writeInt(week);
        dataOutput.writeInt(day);
        dataOutput.writeUTF(type);
        dataOutput.writeLong(calendar.getTime());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        year = dataInput.readInt();
        season = dataInput.readInt();
        month = dataInput.readInt();
        week = dataInput.readInt();
        day =dataInput.readInt();
        type = dataInput.readUTF();
        calendar.setTime(dataInput.readLong());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }

        DateDimension other = (DateDimension) o;
        int tmp = this.id - other.id;
        if(tmp !=0){
            return tmp;
        }

        tmp = this.year - other.year;
        if(tmp !=0){
            return tmp;
        }

        tmp = this.season - other.season;
        if(tmp !=0){
            return tmp;
        }

        tmp = this.month - other.month;
        if(tmp !=0){
            return tmp;
        }

        tmp = this.week - other.week;
        if(tmp !=0){
            return tmp;
        }

        tmp = this.day - other.day;
        if(tmp !=0){
            return tmp;
        }

        return this.type.compareTo(other.type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return id == that.id &&
                year == that.year &&
                season == that.season &&
                month == that.month &&
                week == that.week &&
                day == that.day &&
                Objects.equals(type, that.type) &&
                Objects.equals(calendar, that.calendar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, year, season, month, week, day, type, calendar);
    }

    @Override
    public String toString() {
        return "DateDimension{" +
                "id=" + id +
                ", year=" + year +
                ", season=" + season +
                ", month=" + month +
                ", week=" + week +
                ", day=" + day +
                ", type='" + type + '\'' +
                ", calendar=" + calendar +
                '}';
    }

    public static DateDimension buildDate(long time,DateEnum type){
        int year = TimeUtil.getDateInfo(time, DateEnum.YEAR);

        Calendar calendar = Calendar.getInstance();
        //清空calendar
        calendar.clear();

        //年指标，指该年的1月1日
        if(type.equals(DateEnum.YEAR)){
            calendar.setTimeInMillis(time);
           return new DateDimension(year,0,1,0,1,type.dataType,calendar.getTime()) ;
        }

        int season = TimeUtil.getDateInfo(time,DateEnum.SEASON);
        if(type.equals(DateEnum.SEASON)){
            //季度指标，指该季度的第一个月的1号
            int month = season * 3 - 2;
            calendar.set(year,month -1,1);

            return new DateDimension(year,season,month,0,1,type.dataType,calendar.getTime());

        }

        int month = TimeUtil.getDateInfo(time,DateEnum.MONTH);

        if (type.equals(DateEnum.MONTH)){
           //月指标，指该月的1号
           return new DateDimension(year,season,month,0,1,type.dataType,calendar.getTime()) ;

        }

        int week = TimeUtil.getDateInfo(time,DateEnum.WEEK);
        if (type.equals(DateEnum.WEEK)){
            //获取该周的第一天
            long firstDayOfWeek = TimeUtil.getFirstDayOfWeek(time);
            year = TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.MONTH);

            int day = TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.DAY);

            calendar.set(year,month -1,day);

            //周指标，指该周的第一天
            return new DateDimension(year,season,month,week,day,type.dataType,calendar.getTime()) ;
        }

        int day = TimeUtil.getDateInfo(time,DateEnum.DAY);
        if(type.equals(DateEnum.DAY)){
            calendar.set(year,month-1,day);
            //天指标，指该天
            return new DateDimension(year,season,month,week,day,type.dataType,calendar.getTime());
        }
        throw new RuntimeException("该日期类型不支持获取时间维度对象，dateType:" + type.dataType);
    }
}
