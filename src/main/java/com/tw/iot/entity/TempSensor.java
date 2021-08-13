package com.tw.iot.entity;

/**
 * Author: Yang Ren
 * Package: com.tw.iot.utils
 * Description: 温度传感器POJO
 * Created: 2021/8/7 2:08 PM
 */
public class TempSensor {
    //传感器id
    public String id;
    //传感器时间戳
    public long ts;

    //传感器时间，精确到天，如yyyy-MM-dd
    public String dt;

    //温度值
    public double temp;

    public TempSensor() {
    }

    public TempSensor(String id, long ts, String dt, double temp) {
        this.id = id;
        this.dt = dt;
        this.ts = ts;
        this.temp = temp;
    }

    public static TempSensor of(String id, long ts, String dt, double temp) {
        return new TempSensor(id, ts, dt, temp);
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "id=" + id +
                ", ts=" + ts +
                ", dt=" + dt +
                ", temp=" + temp +
                '}';
    }
}
