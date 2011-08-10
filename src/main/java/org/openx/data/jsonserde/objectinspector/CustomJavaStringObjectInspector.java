package org.openx.data.jsonserde.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.Text;

/**
 * A JavaStringObjectInspector inspects a Java String Object.
 */
public class CustomJavaStringObjectInspector extends
    AbstractPrimitiveJavaObjectInspector implements
    SettableStringObjectInspector {

  CustomJavaStringObjectInspector() {
    super(PrimitiveObjectInspectorUtils.stringTypeEntry);
  }

  @Override
  public Text getPrimitiveWritableObject(Object o) {
    return o == null ? null : new Text(getPrimitiveJavaObject(o));
  }

  @Override
  public String getPrimitiveJavaObject(Object o) {
  	return o.toString(); //this way no casting exceptions occur!
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
