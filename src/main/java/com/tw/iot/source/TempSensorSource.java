package com.tw.iot.source;

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
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // 从项目的resources目录获取输入文件
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
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

            TempSensor tempSensor = TempSensor.of(itemStrArr[0],
                    eventTs,
                    itemStrArr[1].substring(0, 10),
                    Double.parseDouble(itemStrArr[2]));

            // 输入文件中的时间戳是从小到大排列的
            // 新读入的行如果比上一行大，则等待，这样来模拟一个有时间间隔的输入流
            timeDiff = eventTs - lastEventTs;
            if (timeDiff > 0) {
                Thread.sleep(timeDiff);
            }

            sourceContext.collect(tempSensor);
            lastEventTs = eventTs;
        }
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
