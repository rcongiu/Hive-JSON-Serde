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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;

/**
 *
 * @author rcongiu
 */
public class JavaStringShortObjectInspector 
        extends AbstractPrimitiveJavaObjectInspector
        implements SettableShortObjectInspector {

    public JavaStringShortObjectInspector() {
        super(TypeEntryShim.shortType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;
        
        if(o instanceof String) {
            if (StringUtils.isEmpty((String) o)) {
                return null;
            }
          return new ShortWritable(ParsePrimitiveUtils.parseShort((String)o)); 
        } else {
          return new ShortWritable((Short) o);
        }
    }

    @Override
    public short get(Object o) {
        
        if(o instanceof String) {
          if (StringUtils.isEmpty((String) o)) {
            return 0;
          }
          return ParsePrimitiveUtils.parseShort((String)o);
        } else {
          return ((Short) o);
        }
    }

    @Override
    public Object getPrimitiveJavaObject(Object o)
    {
        return get(o);
    }

    @Override
    public Object create(short value) {
        return value;
    }

    @Override
    public Object set(Object o, short value) {
        return value;
    }
}
