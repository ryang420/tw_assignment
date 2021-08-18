package com.tw.iot.entity;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;

public interface SensorSource<T extends Sensor> {
    void readText(SourceFunction.SourceContext<T> sourceContext, String path, Sensor sensor)
            throws IOException, InterruptedException;
}
