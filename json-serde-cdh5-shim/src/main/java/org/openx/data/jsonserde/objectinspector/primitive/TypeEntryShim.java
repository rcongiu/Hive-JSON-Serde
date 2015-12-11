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

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.Map;

/**
 *
 * @author Roberto Congiu <rcongiu@yahoo.com>
 */
public class TypeEntryShim {
    public static PrimitiveTypeInfo byteType   = TypeInfoFactory.byteTypeInfo;
    public static PrimitiveTypeInfo doubleType = TypeInfoFactory.doubleTypeInfo;
    public static PrimitiveTypeInfo floatType = TypeInfoFactory.floatTypeInfo;
    public static PrimitiveTypeInfo intType = TypeInfoFactory.intTypeInfo;
    public static PrimitiveTypeInfo longType = TypeInfoFactory.longTypeInfo;
    public static PrimitiveTypeInfo shortType = TypeInfoFactory.shortTypeInfo;
    public static PrimitiveTypeInfo timestampType = TypeInfoFactory.timestampTypeInfo;
    public static PrimitiveTypeInfo stringType = TypeInfoFactory.stringTypeInfo;
    public static PrimitiveTypeInfo booleanType = TypeInfoFactory.booleanTypeInfo;
    public static PrimitiveTypeInfo dateType = TypeInfoFactory.dateTypeInfo;

    public static void addObjectInspectors(Map<PrimitiveObjectInspector.PrimitiveCategory, AbstractPrimitiveJavaObjectInspector> primitiveOICache) {
        primitiveOICache.put(dateType.getPrimitiveCategory(), new JavaStringDateObjectInspector());
    }

}
