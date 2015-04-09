/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openx.data.jsonserde.objectinspector.primitive;

import java.sql.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

/**
 *
 * @author rcongiu
 */
public class ParsePrimitiveUtils {

    public static boolean isHex(String s) {
        return s.startsWith("0x") || s.startsWith("0X");
    }

    public static byte parseByte(String s) {
        if (isHex(s)) {
            return Byte.parseByte(s.substring(2), 16);
        } else {
            return Byte.parseByte(s);
        }
    }

    public static int parseInt(String s) {
        if (isHex(s)) {
            return Integer.parseInt(s.substring(2), 16);
        } else {
            return Integer.parseInt(s);
        }
    }

    public static short parseShort(String s) {
        if (isHex(s)) {
            return Short.parseShort(s.substring(2), 16);
        } else {
            return Short.parseShort(s);
        }
    }

    public static long parseLong(String s) {
        if (isHex(s)) {
            return Long.parseLong(s.substring(2), 16);
        } else {
            return Long.parseLong(s);
        }
    }

    public static Timestamp parseTimestamp(String s) {
        Timestamp value;
        //DateISO8601 Format Acceptance
        String datePattern = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]";

        if (s == null || s.length() == 0) {
            return null;
        }

        if(s.contains("T")) {
            value = new Timestamp(javax.xml.bind.DatatypeConverter.parseDateTime(s).getTimeInMillis());
        } else if (s.indexOf(':') > 0) {
            value = Timestamp.valueOf(s);
        } else if(s.matches(datePattern)){
            value = new Timestamp(javax.xml.bind.DatatypeConverter.parseDate(s).getTimeInMillis());
        } else if (s.indexOf('.') >= 0) {
            // it's a float
            value = new Timestamp(
                (long) ((double) (Double.parseDouble(s) * 1000)));
        } else {
            // integer
            value = new Timestamp(Long.parseLong(s) * 1000);
        }
        return value;
    }

}
