package com.tw.iot;

import com.tw.iot.entity.OilQualitySensor;
import com.tw.iot.source.OilQualitySensorSource;
import com.tw.iot.utils.Utils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.util.Collector;

/**
 * Author: Yang Ren
 * Package: com.tw.iot
 * Description:
 * Created: 2021/8/10 2:24 PM
 */
public class ProcessOilQualitySensor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<OilQualitySensor> oilQualityStream = env
                .addSource(new OilQualitySensorSource("iot/oil_quality.txt"));


        StreamingFileSink<String> aciditySink =
                StreamingFileSink.forRowFormat(new Path("output/oil/acidity"),
                        new SimpleStringEncoder<String>("UTF-8"))
                        .withBucketAssigner(new DateTimeBucketAssigner<>())
                        .build();

        oilQualityStream.keyBy(oilQualitySensor -> oilQualitySensor.id)
                .process(new OilQualityuAlert("acidity"))
                .addSink(aciditySink);

        StreamingFileSink<String> densenessSink =
                StreamingFileSink.forRowFormat(new Path("output/oil/denseness"),
                        new SimpleStringEncoder<String>("UTF-8"))
                        .withBucketAssigner(new DateTimeBucketAssigner<>())
                        .build();
        oilQualityStream.keyBy(oilQualitySensor -> oilQualitySensor.id)
                .process(new OilQualityuAlert("denseness"))
                .addSink(densenessSink);

        StreamingFileSink<String> moistureSink =
                StreamingFileSink.forRowFormat(new Path("output/oil/moisture"),
                        new SimpleStringEncoder<String>("UTF-8"))
                        .withBucketAssigner(new DateTimeBucketAssigner<>())
                        .build();
        oilQualityStream.keyBy(oilQualitySensor -> oilQualitySensor.id)
                .process(new OilQualityuAlert("moisture"))
                .addSink(moistureSink);

        env.execute("Oil Quality Stream");
    }

    /**
     *
     */
    public static class OilQualityuAlert extends KeyedProcessFunction<String, OilQualitySensor, String> {
        private String item;
        // 记录前2次酸度值
        private ValueState<OilQualitySensor> firstValue;
        private ValueState<OilQualitySensor> secondValue;

        public OilQualityuAlert(String item) {
            this.item = item;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建两个state，分别代表过去两条数据的状态
            firstValue = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstValue", OilQualitySensor.class));
            secondValue = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("secondValue", OilQualitySensor.class));
        }

        /**
         *
         * @param oilQualitySensor
         * @param context
         * @param collector
         * @throws Exception
         * 输入： oilQualitySensor POJO
         * 输出：String
         * description： 逐行比较油品指标值，first代表上上次的指标值，second代表上一次的指标值。
         */
        @Override
        public void processElement(OilQualitySensor oilQualitySensor, Context context, Collector<String> collector) throws Exception {
            OilQualitySensor first = firstValue.value();
            OilQualitySensor second = secondValue.value();

            if (first == null && second == null) {
                // 处理第一条数据时， 把当前数据存入"第二个状态"变量中
                secondValue.update(oilQualitySensor);
            } else if (first == null) {
                // 处理第二条数据时， 把"第二个状态"变量中的数据放入"第一个状态"变量中， 并且把当前数据存入"第二个状态"变量中
                firstValue.update(secondValue.value());
                secondValue.update(oilQualitySensor);
            } else {
                // 处理第三条数据时，用第三条数据的当前指标值去依次比较第一次和第二次指标的值
                double itemA1 = 0;
                double itemA2 = 0;
                double itemCurrent = 0;
                String itemName = "";
                if (this.item.equals("acidity")) {
                    itemA1 = first.acidityValue;
                    itemA2 = second.acidityValue;
                    itemCurrent = oilQualitySensor.acidityValue;
                    itemName = "酸度";
                }

                if (this.item.equals("denseness")) {
                    itemA1 = first.densenessValue;
                    itemA2 = second.densenessValue;
                    itemCurrent = oilQualitySensor.densenessValue;
                    itemName = "稠度";
                }

                if (this.item.equals("moisture")) {
                    itemA1 = first.moistureValue;
                    itemA2 = second.moistureValue;
                    itemCurrent = oilQualitySensor.moistureValue;
                    itemName = "含水量";
                }

                if (itemCurrent > itemA1 * 1.1
                        && itemA2 > itemA1 * 1.1) {
                    String str1 = String.format("%s,%s,%s:%f, 第一次%s过高",
                            second.id, Utils.timestamp2String(second.ts), itemName, itemA2, itemName);
                    collector.collect(str1);

                    String str2 = String.format("%s,%s,%s:%f, 第二次%s过高",
                            oilQualitySensor.id,
                            Utils.timestamp2String(oilQualitySensor.ts), itemName, itemCurrent, itemName);

                    collector.collect(str2);
                }
                firstValue.update(second);
                secondValue.update(oilQualitySensor);
            }
        }
    }
}

