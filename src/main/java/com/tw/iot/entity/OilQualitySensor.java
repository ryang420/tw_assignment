package com.tw.iot.entity;

import com.tw.iot.utils.Utils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Author: Yang Ren
 * Package: com.tw.iot.utils
 * Description: 油品质量POJO
 * Created: 2021/8/7 2:13 PM
 */
public class OilQualitySensor implements Sensor {
    // 油品质量传感器id
    public String id;

    // 传感器数据生成时的时间戳
    public long ts;

    // 酸度
//    public String acidity;
    public double acidityValue;

    // 稠度
//    public String denseness;
    public double densenessValue;

    // 含水量
//    public String moisture;
    public double moistureValue;

    public OilQualitySensor() {
    }

    public OilQualitySensor(String id, long ts, double acidityValue, double densenessValue, double moistureValue) {
        this.id = id;
        this.ts = ts;
        this.acidityValue = acidityValue;
        this.densenessValue = densenessValue;
        this.moistureValue = moistureValue;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public void setAcidityValue(double acidityValue) {
        this.acidityValue = acidityValue;
    }

    public void setDensenessValue(double densenessValue) {
        this.densenessValue = densenessValue;
    }

    public void setMoistureValue(double moistureValue) {
        this.moistureValue = moistureValue;
    }

    @Override
    public OilQualitySensor of(String[] itemStrArr) {
        setId(itemStrArr[0]);
        setTs(Utils.string2Timestamp(itemStrArr[1]));
        setAcidityValue(Double.parseDouble(itemStrArr[2].replaceAll("[^\\d^\\.]+", "")));
        setDensenessValue(Double.parseDouble(itemStrArr[3].replaceAll("[^\\d^\\.]+", "")));
        setMoistureValue(Double.parseDouble(itemStrArr[4].replaceAll("[^\\d^\\.]+", "")));

        return this;
    }

    public static OilQualitySensor of(String id, long ts, double acidityValue, double densenessValue, double moistureValue) {
        return new OilQualitySensor(id, ts, acidityValue, densenessValue, moistureValue);
    }

    @Override
    public String toString() {
        return "OilQualitySensor{" +
                "id=" + id +
                ", ts=" + ts +
                ", acidityValue=" + acidityValue +
                ", densenessValue=" + densenessValue +
                ", moistureValue=" + moistureValue +
                '}';
    }
}
