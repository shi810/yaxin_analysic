package com.yaxin.bigdata.analystic.model.base;

import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 平台维度
 */
public class PlatformDimension extends BaseDimension{
    private int id;
    private String platformName;

    public PlatformDimension() {
    }

    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimension(int id,String platformName) {
        this.platformName = platformName;
        this.id = id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(platformName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        platformName = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        PlatformDimension other = (PlatformDimension)o;
        int tmp = this.id - other.id;
        if (tmp != 0){
            return tmp;
        }

        return this.platformName.compareTo(other.platformName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, platformName);
    }

    @Override
    public String toString() {
        return "PlatformDimension{" +
                "id=" + id +
                ", platformName='" + platformName + '\'' +
                '}';
    }

    public static PlatformDimension getInstance(String platformName){
        if(StringUtils.isEmpty(platformName)){
            platformName = GlobalConstants.DEFAULT_VALUE;
        }
        return new PlatformDimension(platformName);
    }
}
