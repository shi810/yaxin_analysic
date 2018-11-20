package com.yaxin.bigdata.common;

public enum DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour")
    ;

    public String dataType;

    DateEnum(String dateType){
        this.dataType = dateType;
    }

    public static DateEnum valueOfDateType(String type){
        for (DateEnum date:values()) {
            if(date.dataType.equals(type)){
                return date;
            }
        }
        return null;
    }
}
