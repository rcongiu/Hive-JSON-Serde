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

import java.sql.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * A timestamp that is stored in a String
 * @author rcongiu
 */
public class JavaStringTimestampObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements SettableTimestampObjectInspector {
    
    public JavaStringTimestampObjectInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    
    @Override
    public Object set(Object o, byte[] bytes, int offset) {
        return create(bytes,offset);
    }

    @Override
    public Object set(Object o, Timestamp tmstmp) {
        return tmstmp.toString();
    }

    @Override
    public Object set(Object o, TimestampWritable tw) {
        return create(tw.getTimestamp());
    }

    @Override
    public Object create(byte[] bytes, int offset) {
       return new TimestampWritable(bytes, offset).toString();
    }

    @Override
    public Object create(Timestamp tmstmp) {
        return tmstmp.toString();
    }

    @Override
    public TimestampWritable getPrimitiveWritableObject(Object o) {
        if(o == null) return null;
        
        if(o instanceof String) {
           return new TimestampWritable(ParsePrimitiveUtils.parseTimestamp((String)o)); 
        } else {
          return new TimestampWritable(((Timestamp) o));
        }
    }

    @Override
    public Timestamp getPrimitiveJavaObject(Object o) {
         if(o instanceof String) {
           return ParsePrimitiveUtils.parseTimestamp((String)o); 
        } else {
          return ((Timestamp) o);
        }
    }

   
}
