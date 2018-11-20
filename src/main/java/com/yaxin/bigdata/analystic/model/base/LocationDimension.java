package com.yaxin.bigdata.analystic.model.base;

import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName LocationDimension
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 地域维度的基础类
 **/
public class LocationDimension extends BaseDimension{

    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDimension(){
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(int id,String country, String province, String city) {
       this(country,province,city);
       this.id = id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.country);
        dataOutput.writeUTF(this.province);
        dataOutput.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.country = dataInput.readUTF();
        this.province = dataInput.readUTF();
        this.city = dataInput.readUTF();
    }

    public static LocationDimension getInstance(String country, String province, String city){

        if(StringUtils.isEmpty(country)){
            country = province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(province)){
            province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(city)){
            city = GlobalConstants.DEFAULT_VALUE;
        }
        return new LocationDimension(country,province,city);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o == this){
            return 0;
        }
        LocationDimension other = (LocationDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.country.compareTo(other.country);
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.province.compareTo(other.province);
        if(tmp != 0){
            return  tmp;
        }
        return this.city.compareTo(other.city);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationDimension that = (LocationDimension) o;
        return id == that.id &&
                Objects.equals(country, that.country) &&
                Objects.equals(province, that.province) &&
                Objects.equals(city, that.city);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, country, province, city);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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
}