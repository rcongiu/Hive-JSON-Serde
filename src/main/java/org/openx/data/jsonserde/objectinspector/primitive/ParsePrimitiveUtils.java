/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openx.data.jsonserde.objectinspector.primitive;

/**
 *
 * @author rcongiu
 */
public class ParsePrimitiveUtils {
    public static boolean isHex(String s) {
        return s.startsWith("0x") || s.startsWith("0X");
    }
    
    public static byte parseByte(String s) {
        if( isHex(s)) {
            return Byte.parseByte(s.substring(2), 16);
        } else {
            return Byte.parseByte(s);
        }
    }
    
    public static int parseInt(String s) {
        if( isHex(s)) {
            return Integer.parseInt(s.substring(2), 16);
        } else {
            return Integer.parseInt(s);
        }
    }
    
    public static short parseShort(String s) {
        if( isHex(s)) {
            return Short.parseShort(s.substring(2), 16);
        } else {
            return Short.parseShort(s);
        }
    }
    
    public static long parseLong(String s) {
        if( isHex(s)) {
            return Long.parseLong(s.substring(2), 16);
        } else {
            return Long.parseLong(s);
        }
    }
    
    
}
