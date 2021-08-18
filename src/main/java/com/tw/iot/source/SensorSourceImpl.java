package com.tw.iot.source;

import com.tw.iot.entity.OilQualitySensor;
import com.tw.iot.entity.Sensor;
import com.tw.iot.entity.SensorSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SensorSourceImpl<T extends Sensor> implements SensorSource<Sensor> {
    private boolean isRunning = true;

    @Override
    public void readText(SourceFunction.SourceContext<Sensor> sourceContext, String path, Sensor sensor)
            throws IOException, InterruptedException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // 从项目的resources目录获取输入文件
        InputStream streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));
        String line;
        boolean isFirstLine = true;
        long timeDiff = 0;
        long lastEventTs = 0;
        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split(",");
            LocalDateTime dateTime = LocalDateTime.parse(itemStrArr[1], formatter);
            long eventTs = Timestamp.valueOf(dateTime).getTime();

            // 从第一行提取时间戳
            if (isFirstLine) {
                lastEventTs = eventTs;
                isFirstLine = false;
            }

//            OilQualitySensor sensor = new OilQualitySensor().of(itemStrArr);
            sensor = sensor.of(itemStrArr);

            // 输入文件中的时间戳是从小到大排列的
            // 新读入的行如果比上一行大，则等待，这样来模拟一个有时间间隔的输入流
            timeDiff = eventTs - lastEventTs;
            if (timeDiff > 0) {
                Thread.sleep(timeDiff);
            }

            sourceContext.collect(sensor);
            lastEventTs = eventTs;
        }
    }
}
