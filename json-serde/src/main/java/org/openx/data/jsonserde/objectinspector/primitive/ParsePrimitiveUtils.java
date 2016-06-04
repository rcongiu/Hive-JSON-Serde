/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openx.data.jsonserde.objectinspector.primitive;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 *
 * @author rcongiu
 */
public final class ParsePrimitiveUtils {

    private ParsePrimitiveUtils() {
        throw new InstantiationError("This class must not be instantiated.");
    }

    private static DateFormat UTC_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static DateFormat NON_UTC_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
        final String sampleUnixTimestampInMs = "1454612111000";

        Timestamp value;
        if (s.indexOf(':') > 0) {
            value = Timestamp.valueOf(nonUTCFormat(s));
        } else if (s.indexOf('.') >= 0) {
            // it's a float
            value = new Timestamp(
                    (long) ((double) (Double.parseDouble(s) * 1000)));
        } else {
            // integer 
            Long timestampValue = Long.parseLong(s);
            Boolean isTimestampInMs = s.length() >= sampleUnixTimestampInMs.length();
            if(isTimestampInMs) {
                value = new Timestamp(timestampValue);
            } else {
                value = new Timestamp(timestampValue * 1000);
            }            
        }
        return value;
    }

    public static String nonUTCFormat(String s) {
        if(s.endsWith("Z")) {
            try {
                return NON_UTC_FORMAT.format(UTC_FORMAT.parse(s));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return s;
    }

}
