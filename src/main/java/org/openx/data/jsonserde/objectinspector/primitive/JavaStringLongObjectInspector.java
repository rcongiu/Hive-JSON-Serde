/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openx.data.jsonserde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringLongObjectInspector
        extends AbstractPrimitiveJavaObjectInspector
        implements SettableLongObjectInspector {

    public JavaStringLongObjectInspector() {
        super(PrimitiveObjectInspectorUtils.longTypeEntry);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;
        
        if(o instanceof String) {
           return new LongWritable(Long.parseLong((String)o)); 
        } else {
          return new LongWritable(((Long) o).longValue());
        }
    }

    @Override
    public long get(Object o) {
        
        if(o instanceof String) {
           return Long.parseLong((String)o); 
        } else {
          return (((Long) o).longValue());
        }
    }

    @Override
    public Object create(long value) {
        return Long.valueOf(value);
    }

    @Override
    public Object set(Object o, long value) {
        return Long.valueOf(value);
    }
}
