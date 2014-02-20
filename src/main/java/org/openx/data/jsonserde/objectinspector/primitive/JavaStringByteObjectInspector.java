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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.io.ByteWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringByteObjectInspector   extends AbstractPrimitiveJavaObjectInspector
        implements SettableByteObjectInspector {

    public JavaStringByteObjectInspector() {
        super(PrimitiveObjectInspectorUtils.byteTypeEntry);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;
        
        if(o instanceof String) {
           return new ByteWritable(ParsePrimitiveUtils.parseByte((String)o)); 
        } else {
           return new ByteWritable(((Byte) o).byteValue());
        }
    }

    @Override
    public byte get(Object o) {
        if(o instanceof String) {
           return ParsePrimitiveUtils.parseByte((String)o); 
        } else {
           return ((Byte) o).byteValue();
        }
    }

    @Override
    public Object create(byte value) {
        return Byte.valueOf(value);
    }

    @Override
    public Object set(Object o, byte value) {
        return Byte.valueOf(value);
    }
}
