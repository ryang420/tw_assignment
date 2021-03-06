package com.tw.iot.utils;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Author: Yang Ren
 * Package: com.tw.iot.utils
 * Description:
 * Created: 2021/8/10 10:50 AM
 */
public class Utils {
    public static String reserve2Digits(double d) {
        DecimalFormat df = new DecimalFormat("######0.00");
        return df.format(d);
    }

    public static String timestamp2String(long ts) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(ts));
    }

    public static long string2Timestamp(String datetime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(datetime, formatter);
        long eventTs = Timestamp.valueOf(dateTime).getTime();
        return eventTs;
    }
}
