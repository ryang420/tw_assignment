##IoT数据处理
简介：该数据处理项目使用Flink DataStream 和FlinkSQL 构建。计算平均温度预警时，使用了FlinkSQL来计算当前温度是否高于此时的平均温度，这里的平均温度值不包括当前的温度。计算油品指标时，使用了FLink的State来记录前2次的油品指标，以便后续进行比较。
###1. 项目运行环境需求：
    * JDK1.8
    * Maven3

###2. 目录结构()
```
-com
    -tw
        -iot
            -entity
                OilQualitySensor.java
                TempSensor.java
            -source
                OilQualitySensorSource.java
                TempSensorSource.java
            -utils
                Utils.java
            ProcessOilQualitySensor.java
            ProcessTempSenso.java
```

###3. 项目运行步骤：
    *ProcessTempSensor的main方法处理温度告警
    *ProcessOilQualitySensor的main方法处理油品质量检测

###4. 数据输出目录：
```
温度告警：output/temp/alert 
平均温度报表：output/temp/avg

油品指标检测：output/iol
酸度：output/iol/acidity
稠度：output/iol/denseness
含水量：output/iol/mositure
```
