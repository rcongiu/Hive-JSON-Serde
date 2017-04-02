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
        super(TypeEntryShim.intType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;
        return new IntWritable(get(o));
    }

    @Override
    public int get(Object o) {
        if(ParsePrimitiveUtils.isString(o)) {
           return ParsePrimitiveUtils.parseInt(o.toString());
        } else {
           return (Integer) o;
        }
    }

    @Override
    public Object getPrimitiveJavaObject(Object o)
    {
        return get(o);
    }

    @Override
    public Object create(int value) {
        return value;
    }

    @Override
    public Object set(Object o, int value) {
        return value;
    }
}
