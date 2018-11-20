package com.yaxin.bigdata.analystic.model.value.map;

import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
import com.yaxin.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName LocationOutputValue
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class LocationOutputValue extends StatsOutputValue {
    private String uid = ""; //
    private String sid = "";

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if(StringUtils.isNotEmpty(uid)){
            dataOutput.writeUTF(this.uid);
        } else {
            dataOutput.writeUTF("");
        }
        if(StringUtils.isNotEmpty(sid)){
            dataOutput.writeUTF(this.sid);
        } else {
            dataOutput.writeUTF("");
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid = dataInput.readUTF();
        this.sid = dataInput.readUTF();
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }
}