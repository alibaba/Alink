package com.alibaba.alink.common.io.kafka;

import java.text.SimpleDateFormat;

public class KafkaUtils {
    /**
     * Parse a string to unix time stamp in milliseconds.
     */
    public static long parseDateStringToMs(String dateStr, String dateFormat) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
            return simpleDateFormat.parse(dateStr).getTime();
        } catch (Exception e) {
            throw new RuntimeException("Fail to parse date string: " + dateStr);
        }
    }

    /**
     * Parse a string to unix time stamp in milliseconds. The format is "yyyy-MM-dd HH:mm:ss".
     */
    public static long parseDateStringToMs(String dateStr) {
        return parseDateStringToMs(dateStr, "yyyy-MM-dd HH:mm:ss");
    }
}
