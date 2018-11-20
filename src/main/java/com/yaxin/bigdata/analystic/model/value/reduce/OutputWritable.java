package com.yaxin.bigdata.analystic.model.value.reduce;

import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
import com.yaxin.bigdata.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于reduce阶段输出的value的类型
 */
public class OutputWritable extends StatsOutputValue {
    private KpiType kpi;
    private MapWritable value = new MapWritable();

    @Override
    public KpiType getKpi() {
        return kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput,kpi);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        kpi = WritableUtils.readEnum(dataInput,KpiType.class);
        value.readFields(dataInput);
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }
}
