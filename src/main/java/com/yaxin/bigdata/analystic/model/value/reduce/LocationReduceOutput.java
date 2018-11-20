package com.yaxin.bigdata.analystic.model.value.reduce;

import com.yaxin.bigdata.common.KpiType;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName TextOutputValue
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description location
 **/
public class LocationReduceOutput extends OutputWritable {
    private KpiType kpi;
    private int aus;
    private int sessions;
    private int bounce_sessions;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput,kpi);
        dataOutput.writeInt(this.aus);
        dataOutput.writeInt(this.sessions);
        dataOutput.writeInt(this.bounce_sessions);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        WritableUtils.readEnum(dataInput,KpiType.class);
        this.aus = dataInput.readInt();
        this.sessions = dataInput.readInt();
        this.bounce_sessions = dataInput.readInt();
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public int getAus() {
        return aus;
    }

    public void setAus(int aus) {
        this.aus = aus;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBounce_sessions() {
        return bounce_sessions;
    }

    public void setBounce_sessions(int bounce_sessions) {
        this.bounce_sessions = bounce_sessions;
    }
}