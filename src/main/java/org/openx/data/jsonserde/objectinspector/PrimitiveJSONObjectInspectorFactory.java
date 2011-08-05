

package org.openx.data.jsonserde.objectinspector;

import java.util.HashMap;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;


public final class PrimitiveJSONObjectInspectorFactory {

	 private static HashMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector> cachedInspectors =
      new HashMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector>();
  static {
    cachedInspectors.put(PrimitiveCategory.STRING, new CustomJavaStringObjectInspector());
    
  }



	public static AbstractPrimitiveJavaObjectInspector getPrimitiveJavaObjectInspector(PrimitiveCategory cat) {
		AbstractPrimitiveJavaObjectInspector result = cachedInspectors.get(cat);
		if(result == null) result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(cat);
		return result;				
	}



}