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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringLongObjectInspector
        extends AbstractPrimitiveJavaObjectInspector
        implements SettableLongObjectInspector {

    public JavaStringLongObjectInspector() {
        super(TypeInfoFactory.longTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;
        
        if(o instanceof String) {
           return new LongWritable(ParsePrimitiveUtils.parseLong((String)o)); 
        } else {
          return new LongWritable(((Long) o).longValue());
        }
    }

    @Override
    public long get(Object o) {
        
        if(o instanceof String) {
           return ParsePrimitiveUtils.parseLong((String)o); 
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
