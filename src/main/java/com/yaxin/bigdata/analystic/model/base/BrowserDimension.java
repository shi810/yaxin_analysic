package com.yaxin.bigdata.analystic.model.base;

import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 *  浏览器维度类
 */
public class BrowserDimension extends BaseDimension{
    private int id;
    private String browserName;
    private String browserVersion;

    public BrowserDimension() {
    }

    public BrowserDimension(String browserName, String browserVersion) {
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    public BrowserDimension(int id,String browserName, String browserVersion) {
        this(browserName,browserVersion);
        this.id = id;
    }

    /**
     * 构建浏览器维度的集合对象
     * @param browserName
     * @param browserVersion
     * @return
     */
    public static BrowserDimension getInstance(String browserName, String browserVersion){
        if(StringUtils.isEmpty(browserName)){
            browserName = browserVersion = GlobalConstants.DEFAULT_VALUE;
        }

        if(StringUtils.isEmpty(browserVersion)){
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }
        return new BrowserDimension(browserName,browserVersion);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(browserName);
        dataOutput.writeUTF(browserVersion);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        browserName = dataInput.readUTF();
        browserVersion = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        BrowserDimension other = (BrowserDimension)o;
        int tmp = this.id - other.id;
        if (tmp != 0){
            return tmp;
        }

        tmp = this.browserName.compareTo(other.browserName);
        if (tmp != 0){
            return tmp;
        }

        return this.browserVersion.compareTo(other.browserVersion);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowserDimension that = (BrowserDimension) o;
        return id == that.id &&
                Objects.equals(browserName, that.browserName) &&
                Objects.equals(browserVersion, that.browserVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, browserName, browserVersion);
    }

    @Override
    public String toString() {
        return "BrowserDimension{" +
                "id=" + id +
                ", browserName='" + browserName + '\'' +
                ", browserVersion='" + browserVersion + '\'' +
                '}';
    }
}
