package com.yaxin.bigdata.etl.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogWritable implements Writable {
    private String ver;
    private String s_time;
    private String en;
    private String u_ud;
    private String u_mid;
    private String u_sd;
    private String c_time;
    private String l;
    private String b_iev;
    private String b_rst;
    private String p_url;
    private String p_ref;
    private String tt;
    private String pl;
    private String ip;
    private String oid;
    private String on;
    private String cua;
    private String cut;
    private String pt;
    private String ca;
    private String ac;
    private String kv_;
    private String du;
    private String browserName;
    private String browserVersion;
    private String osName;
    private String osVersion;
    private String country;
    private String province;
    private String city;

    public LogWritable() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ver);
        dataOutput.writeUTF(s_time);
        dataOutput.writeUTF(en);
        dataOutput.writeUTF(u_ud);
        dataOutput.writeUTF(u_mid);
        dataOutput.writeUTF(u_sd);
        dataOutput.writeUTF(c_time);
        dataOutput.writeUTF(l);
        dataOutput.writeUTF(b_iev);
        dataOutput.writeUTF(b_rst);
        dataOutput.writeUTF(p_url);
        dataOutput.writeUTF(p_ref);
        dataOutput.writeUTF(tt);
        dataOutput.writeUTF(pl);
        dataOutput.writeUTF(ip);
        dataOutput.writeUTF(oid);
        dataOutput.writeUTF(on);
        dataOutput.writeUTF(cua);
        dataOutput.writeUTF(cut);
        dataOutput.writeUTF(pt);
        dataOutput.writeUTF(ca);
        dataOutput.writeUTF(ac);
        dataOutput.writeUTF(kv_);
        dataOutput.writeUTF(du);
        dataOutput.writeUTF(browserName);
        dataOutput.writeUTF(browserVersion);
        dataOutput.writeUTF(osName);
        dataOutput.writeUTF(osVersion);
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(province);
        dataOutput.writeUTF(city);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ver = dataInput.readUTF();
        this.s_time = dataInput.readUTF();
        this.en = dataInput.readUTF();
        this.u_ud = dataInput.readUTF();
        this.u_mid = dataInput.readUTF();
        this.u_sd = dataInput.readUTF();
        this.c_time = dataInput.readUTF();
        this.l = dataInput.readUTF();
        this.b_iev = dataInput.readUTF();
        this.b_rst = dataInput.readUTF();
        this.p_url = dataInput.readUTF();
        this.p_ref = dataInput.readUTF();
        this.tt = dataInput.readUTF();
        this.pl = dataInput.readUTF();
        this.ip = dataInput.readUTF();
        this.oid = dataInput.readUTF();
        this.on = dataInput.readUTF();
        this.cua = dataInput.readUTF();
        this.cut = dataInput.readUTF();
        this.pt = dataInput.readUTF();
        this.ca = dataInput.readUTF();
        this.ac = dataInput.readUTF();
        this.kv_ = dataInput.readUTF();
        this.du = dataInput.readUTF();
        this.browserName = dataInput.readUTF();
        this.browserVersion = dataInput.readUTF();
        this.osName = dataInput.readUTF();
        this.osVersion = dataInput.readUTF();
        this.country = dataInput.readUTF();
        this.province = dataInput.readUTF();
        this.city = dataInput.readUTF();
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getS_time() {
        return s_time;
    }

    public void setS_time(String s_time) {
        this.s_time = s_time;
    }

    public String getEn() {
        return en;
    }

    public void setEn(String en) {
        this.en = en;
    }

    public String getU_ud() {
        return u_ud;
    }

    public void setU_ud(String u_ud) {
        this.u_ud = u_ud;
    }

    public String getU_mid() {
        return u_mid;
    }

    public void setU_mid(String u_mid) {
        this.u_mid = u_mid;
    }

    public String getU_sd() {
        return u_sd;
    }

    public void setU_sd(String u_sd) {
        this.u_sd = u_sd;
    }

    public String getC_time() {
        return c_time;
    }

    public void setC_time(String c_time) {
        this.c_time = c_time;
    }

    public String getL() {
        return l;
    }

    public void setL(String l) {
        this.l = l;
    }

    public String getB_iev() {
        return b_iev;
    }

    public void setB_iev(String b_iev) {
        this.b_iev = b_iev;
    }

    public String getB_rst() {
        return b_rst;
    }

    public void setB_rst(String b_rst) {
        this.b_rst = b_rst;
    }

    public String getP_url() {
        return p_url;
    }

    public void setP_url(String p_url) {
        this.p_url = p_url;
    }

    public String getP_ref() {
        return p_ref;
    }

    public void setP_ref(String p_ref) {
        this.p_ref = p_ref;
    }

    public String getTt() {
        return tt;
    }

    public void setTt(String tt) {
        this.tt = tt;
    }

    public String getPl() {
        return pl;
    }

    public void setPl(String pl) {
        this.pl = pl;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getOn() {
        return on;
    }

    public void setOn(String on) {
        this.on = on;
    }

    public String getCua() {
        return cua;
    }

    public void setCua(String cua) {
        this.cua = cua;
    }

    public String getCut() {
        return cut;
    }

    public void setCut(String cut) {
        this.cut = cut;
    }

    public String getPt() {
        return pt;
    }

    public void setPt(String pt) {
        this.pt = pt;
    }

    public String getCa() {
        return ca;
    }

    public void setCa(String ca) {
        this.ca = ca;
    }

    public String getAc() {
        return ac;
    }

    public void setAc(String ac) {
        this.ac = ac;
    }

    public String getKv_() {
        return kv_;
    }

    public void setKv_(String kv_) {
        this.kv_ = kv_;
    }

    public String getDu() {
        return du;
    }

    public void setDu(String du) {
        this.du = du;
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

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
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

    @Override
    public String toString() {
        return ver + '\u0001' + s_time + '\u0001' + en + '\u0001' +
                u_ud + '\u0001' + u_mid + '\u0001' + u_sd + '\u0001' +
                c_time + '\u0001' + l + '\u0001' + b_iev + '\u0001' +
                b_rst + '\u0001' + p_url + '\u0001' + p_ref + '\u0001' +
                tt + '\u0001' + pl + '\u0001' + ip + '\u0001' +
                oid + '\u0001' + on + '\u0001' + cua + '\u0001' +
                cut + '\u0001' + pt + '\u0001' + ca + '\u0001' +
                ac + '\u0001' + kv_ + '\u0001' + du + '\u0001' +
                browserName + '\u0001' + browserVersion + '\u0001' +
                osName + '\u0001' + osVersion + '\u0001' + country + '\u0001' +
                province + '\u0001' + city;
    }
}
