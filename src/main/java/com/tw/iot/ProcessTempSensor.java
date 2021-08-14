package com.tw.iot;

import com.tw.iot.entity.TempSensor;
import com.tw.iot.source.TempSensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.tw.iot.utils.Utils.reserve2Digits;

/**
 * Author: Yang Ren
 * Package: com.tw.iot
 * Description: 温度传感器数据实时检测异常
 * Created: 2021/8/8 9:42 PM
 */
public class ProcessTempSensor {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);


        DataStream<TempSensor> tempSensorStream = env
                .addSource(new TempSensorSource("iot/temp_sensor.txt"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TempSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((tempSensor, timestamp) -> tempSensor.ts)
                );

        // 温度异常警告
        tEnv.createTemporaryView("temp_sensor", tempSensorStream, "id, temp, ts.rowtime, dt");
        Table tumbleTempSensor = tEnv.sqlQuery("" +
                "SELECT id, ts, temp," +
                "AVG(temp) OVER w AS diff " +
                "FROM temp_sensor " +
                "WINDOW w AS (" +
                "   PARTITION BY id, dt " +
                "   ORDER BY ts " +
                "   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ");
        DataStream<Tuple2<Boolean, Row>> avgTempSensorStream = tEnv.toRetractStream(tumbleTempSensor, Row.class);

        StreamingFileSink<String> tempAlertSink =
                StreamingFileSink.forRowFormat(new Path("output/temp/alert"),
                        new SimpleStringEncoder<String>("UTF-8"))
                        .withBucketAssigner(new DateTimeBucketAssigner<>())
                        .build();

        avgTempSensorStream
                .map(new MapTempSensorResult())
                .keyBy(tuple -> tuple.f0)
                .process(new TempSensorAlert())
                .addSink(tempAlertSink);
//                .print();

        // 每日平均温度报表
        Table avgTempReport = tEnv.sqlQuery("SELECT " +
                "dt, " +
                "avg(temp) AS avg_temp " +
                "FROM temp_sensor " +
                "GROUP BY dt, TUMBLE(ts, INTERVAL '1' DAY)");
        DataStream<Tuple2<Boolean, Row>> avgTempReportStream = tEnv.toRetractStream(avgTempReport, Row.class);

        StreamingFileSink<String> avgTempSink =
                StreamingFileSink.forRowFormat(new Path("output/temp/avg"),
                        new SimpleStringEncoder<String>("UTF-8"))
                        .withBucketAssigner(new DateTimeBucketAssigner<>())
                        .build();

        avgTempReportStream
                .map(new MapFunction<Tuple2<Boolean, Row>, String>() {
                    @Override
                    public String map(Tuple2<Boolean, Row> tuple2) throws Exception {
                        return tuple2.f1.getField(0) + " " + reserve2Digits((Double) tuple2.f1.getField(1));
                    }
                })
                .addSink(avgTempSink);
//                .print();
        env.execute("Temperature sensor stream");
    }


    public static class MapTempSensorResult implements MapFunction<Tuple2<Boolean, Row>, Tuple4<String, LocalDateTime, Double, Double>> {
        @Override
        public Tuple4<String, LocalDateTime, Double, Double> map(Tuple2<Boolean, Row> tuple2) throws Exception {
            Row row = tuple2.f1;
            String id = (String) row.getField(0);
            LocalDateTime dt = (LocalDateTime) row.getField(1);
            dt = dt.plusHours(8L);
            Double temp = (Double) row.getField(2);
            Double avgTemp = (Double) row.getField(3);

            return new Tuple4<>(id, dt, temp, avgTemp);
        }
    }

    public static class TempSensorAlert extends KeyedProcessFunction<String, Tuple4<String, LocalDateTime, Double, Double>, String> {
        private ValueState<Double> lastAvgTemp;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从RuntimeContext中获取状态
            lastAvgTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("lastAvgTemp", Types.DOUBLE));
        }

        @Override
        public void processElement(Tuple4<String, LocalDateTime, Double, Double> record, Context context, Collector<String> collector) throws Exception {
            if (lastAvgTemp.value() != null) {
                String id = record.f0;
                double prevAvgTemp = lastAvgTemp.value();
                double currentTemp = record.f2;

                LocalDateTime dt = record.f1;
                DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                String temp_dt = dt.format(pattern);

                String tempFlag;
                String str = id +
                        "," +
                        temp_dt +
                        "," +
                        currentTemp;
                if (currentTemp - prevAvgTemp > 5.0) {
                    tempFlag = "; 温度过高";
                    collector.collect(str + tempFlag);
                }

                if (prevAvgTemp - currentTemp > 5.0) {
                    tempFlag = "; 温度过低";
                    collector.collect(str + tempFlag);
                }
            }

            lastAvgTemp.update(record.f3);
        }
    }
}
