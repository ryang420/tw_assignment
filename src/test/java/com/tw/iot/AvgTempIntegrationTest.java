package com.tw.iot;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class AvgTempIntegrationTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testAvgTempAlertPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        Tuple4<String, LocalDateTime, Double, Double> t1
                = Tuple4.of("T1", LocalDateTime.parse("2020-01-30 19:00:00", df), 25.0, 25.0);
        Tuple4<String, LocalDateTime, Double, Double> t2
                = Tuple4.of("T1", LocalDateTime.parse("2020-01-30 19:00:01", df), 22.0, 23.5);
        Tuple4<String, LocalDateTime, Double, Double> t3
                = Tuple4.of("T1", LocalDateTime.parse("2020-01-30 19:00:02", df), 29.0, 25.33);

        env.fromElements(t1, t2, t3)
                .keyBy(t -> t.f0)
                .process(new ProcessTempSensor.TempSensorAlert())
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSink.values.contains("T1,2020-01-30 19:00:02,29.0; 温度过高"));
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<String> {

        // must be static
        public static final List<String> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(String value) throws Exception {
            values.add(value);
        }
    }
}
