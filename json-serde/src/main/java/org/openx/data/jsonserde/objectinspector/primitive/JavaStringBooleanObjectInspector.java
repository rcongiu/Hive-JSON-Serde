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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 *
 * @author rajat.85@gmail.com
 */
public class JavaStringBooleanObjectInspector extends AbstractPrimitiveJavaObjectInspector
  implements SettableBooleanObjectInspector {

  public JavaStringBooleanObjectInspector() {
    super(TypeEntryShim.booleanType);
  }

  @Override
  public Object getPrimitiveWritableObject(Object o) {
    if(o == null) return null;
    return new BooleanWritable(get(o));
  }

  @Override
  public boolean get(Object o) {

    if(ParsePrimitiveUtils.isString(o)) {
      return Boolean.parseBoolean(o.toString());
    } else {
      return (Boolean) o;
    }
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    return get(o);
  }

  @Override
  public Object create(boolean value) {
    return value;
  }

  @Override
  public Object set(Object o, boolean value) {
    return value;
  }

}
