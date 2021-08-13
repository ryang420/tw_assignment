package com.tw.iot.entity;

/**
 * Author: Yang Ren
 * Package: com.tw.iot.utils
 * Description: 油品质量POJO
 * Created: 2021/8/7 2:13 PM
 */
public class OilQualitySensor {
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
