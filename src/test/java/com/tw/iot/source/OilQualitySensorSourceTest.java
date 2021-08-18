package com.tw.iot.source;

import junit.framework.TestCase;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static org.mockito.Mockito.*;

public class OilQualitySensorSourceTest extends TestCase {

    public void testTestRun() throws Exception {
        SourceFunction.SourceContext source = mock(SourceFunction.SourceContext.class);
        OilQualitySensorSource oilQualitySensorSource = new OilQualitySensorSource("iot/oil_quality.txt");
        oilQualitySensorSource.run(source);
        verify(source, times(5)).collect(any());

    }
}