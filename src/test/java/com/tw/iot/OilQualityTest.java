package com.tw.iot;

import com.tw.iot.entity.OilQualitySensor;
import com.tw.iot.utils.DateUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class OilQualityTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testOilQualityPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        OilQualitySensor q1 = OilQualitySensor
                .of("Q1", DateUtils.getTimestamp("2020-01-30 19:30:25"), 37.8, 100, 0.11);
        OilQualitySensor q2 = OilQualitySensor
                .of("Q1", DateUtils.getTimestamp("2020-01-30 19:30:32"), 48.9, 100, 0.11);
        OilQualitySensor q3 = OilQualitySensor
                .of("Q1", DateUtils.getTimestamp("2020-01-30 19:30:40"), 49.9, 100, 0.11);


        env.fromElements(q1, q2, q3)
                .keyBy(t -> t.id)
                .process(new ProcessOilQualitySensor.OilQualityuAlert("acidity"))
                .addSink(new CollectSink());

        // execute
        env.execute();

        List<String> results = Arrays.asList("Q1,2020-01-30 19:30:32,酸度:48.900000, 第一次酸度过高",
                "Q1,2020-01-30 19:30:40,酸度:49.900000, 第二次酸度过高");
        // verify your results
        assertTrue(CollectSink.values.containsAll(results));
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
