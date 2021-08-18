package com.tw.iot.entity;

import com.tw.iot.utils.Utils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Author: Yang Ren
 * Package: com.tw.iot.utils
 * Description: 温度传感器POJO
 * Created: 2021/8/7 2:08 PM
 */
public class TempSensor implements Sensor {
    //传感器id
    public String id;

    public void setId(String id) {
        this.id = id;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

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

    @Override
    public Sensor of(String[] itemStrArr) {
        setId(itemStrArr[0]);
        setTs(Utils.string2Timestamp(itemStrArr[1]));
        setDt(itemStrArr[1].substring(0, 10));
        setTemp(Double.parseDouble(itemStrArr[2]));

        return this;
    }
}
