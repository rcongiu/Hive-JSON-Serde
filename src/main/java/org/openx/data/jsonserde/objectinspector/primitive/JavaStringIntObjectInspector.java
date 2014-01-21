/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openx.data.jsonserde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringIntObjectInspector 
    extends AbstractPrimitiveJavaObjectInspector
        implements SettableIntObjectInspector {

    public JavaStringIntObjectInspector() {
        super(PrimitiveObjectInspectorUtils.longTypeEntry);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;
        
        if(o instanceof String) {
           return new IntWritable(Integer.parseInt((String)o)); 
        } else {
           return new IntWritable(((Integer) o).intValue());
        }
    }

    @Override
    public int get(Object o) {
        if(o instanceof String) {
           return Integer.parseInt((String)o); 
        } else {
           return ((Integer) o).intValue();
        }
    }

    @Override
    public Object create(int value) {
        return Integer.valueOf(value);
    }

    @Override
    public Object set(Object o, int value) {
        return Integer.valueOf(value);
    }
}
