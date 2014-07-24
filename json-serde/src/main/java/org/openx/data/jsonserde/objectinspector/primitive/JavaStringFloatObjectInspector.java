/*======================================================================*
 * Copyright (c) 2011, OpenX Technologies, Inc. All rights reserved.    *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License. Unless required     *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/

package org.openx.data.jsonserde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.io.FloatWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringFloatObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements SettableFloatObjectInspector {

    public JavaStringFloatObjectInspector() {
        super(TypeEntryShim.floatType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;
        
        if(o instanceof String) {
          return new FloatWritable(Float.parseFloat((String)o)); 
        } else {
          return new FloatWritable((Float) o);
        }
    }

    @Override
    public float get(Object o) {  
        if(o instanceof String) {
          return Float.parseFloat((String)o); 
        } else {
          return ((Float) o);
        }
    }

    @Override
    public Object create(float value) {
        return value;
    }

    @Override
    public Object set(Object o, float value) {
        return value;
    }
    
}
