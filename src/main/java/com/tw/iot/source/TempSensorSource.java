package com.tw.iot.source;

import com.tw.iot.entity.OilQualitySensor;
import com.tw.iot.entity.SensorSource;
import com.tw.iot.entity.TempSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Author: Yang Ren
 * Package: com.tw.iot.source
 * Description: 我们根据温度传感器数据的时间，来模拟一个实时的数据流，类似于从消息系统获取事件时间的数据。
 * Created: 2021/8/8 9:22 PM
 */
public class TempSensorSource implements SourceFunction<TempSensor> {
    // Source是否正在运行
    private boolean isRunning = true;
    // 数据集文件名
    private String path;
    private InputStream streamSource;

    public TempSensorSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<TempSensor> sourceContext) throws Exception {
        SensorSource sensorSource = new SensorSourceImpl();
        sensorSource.readText(sourceContext, path, new TempSensor());
    }

    // 停止发送数据
    @Override
    public void cancel() {
        try {
            streamSource.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        isRunning = false;
    }
}
