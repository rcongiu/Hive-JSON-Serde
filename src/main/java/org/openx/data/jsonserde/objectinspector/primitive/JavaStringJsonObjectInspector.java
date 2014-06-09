package org.openx.data.jsonserde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class JavaStringJsonObjectInspector extends AbstractPrimitiveJavaObjectInspector
                                                                                       implements
                                                                                           SettableStringObjectInspector {

    Logger logger = Logger.getLogger(JavaStringJsonObjectInspector.class);

    public JavaStringJsonObjectInspector() {
        super(PrimitiveObjectInspectorUtils.stringTypeEntry);
    }

    @Override
    public Text getPrimitiveWritableObject(Object o) {
        return o == null ? null : new Text(((String) o.toString()));
    }

    @Override
    public String getPrimitiveJavaObject(Object o) {
        return o == null ? null : o.toString();
    }

    @Override
    public Object create(Text value) {
        return value == null ? null : value.toString();
    }

    @Override
    public Object set(Object o, Text value) {
        return value == null ? null : value.toString();
    }

    @Override
    public Object create(String value) {
        return value;
    }

    @Override
    public Object set(Object o, String value) {
        return value;
    }
}
