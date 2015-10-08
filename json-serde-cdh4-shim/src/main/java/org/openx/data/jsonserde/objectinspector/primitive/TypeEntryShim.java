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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 *
 * @author Roberto Congiu <rcongiu@yahoo.com>
 */
public class TypeEntryShim {
    public static PrimitiveTypeEntry byteType = PrimitiveObjectInspectorUtils.byteTypeEntry;
    public static PrimitiveTypeEntry doubleType = PrimitiveObjectInspectorUtils.doubleTypeEntry;
    public static PrimitiveTypeEntry floatType = PrimitiveObjectInspectorUtils.floatTypeEntry;
    public static PrimitiveTypeEntry intType = PrimitiveObjectInspectorUtils.intTypeEntry;
    public static PrimitiveTypeEntry longType = PrimitiveObjectInspectorUtils.longTypeEntry;
    public static PrimitiveTypeEntry shortType = PrimitiveObjectInspectorUtils.shortTypeEntry;
    public static PrimitiveTypeEntry timestampType = PrimitiveObjectInspectorUtils.timestampTypeEntry;
    public static PrimitiveTypeEntry stringType = PrimitiveObjectInspectorUtils.stringTypeEntry;
    public static PrimitiveTypeEntry booleanType = PrimitiveObjectInspectorUtils.booleanTypeEntry;
    
}
