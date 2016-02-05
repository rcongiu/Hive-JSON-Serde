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
package org.openx.data.jsonserde.objectinspector;


import java.util.*;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.openx.data.jsonserde.objectinspector.primitive.*;

/**
 *
 * @author rcongiu
 */
public class JsonObjectInspectorFactory {

    static HashMap<TypeInfo, ObjectInspector> cachedJsonObjectInspector = new HashMap<TypeInfo, ObjectInspector>();

    /**
     *
     *
     * @param options
     * @see JsonUtils
     * @param typeInfo
     * @return
     */
    public static ObjectInspector getJsonObjectInspectorFromTypeInfo(
            TypeInfo typeInfo, JsonStructOIOptions options) {
        ObjectInspector result = cachedJsonObjectInspector.get(typeInfo);
        if (result == null) {
            switch (typeInfo.getCategory()) {
                case PRIMITIVE: {
                    PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;

                    result
                            = getPrimitiveJavaObjectInspector(pti.getPrimitiveCategory());
                    break;
                }
                case LIST: {
                    ObjectInspector elementObjectInspector
                            = getJsonObjectInspectorFromTypeInfo(
                            ((ListTypeInfo) typeInfo).getListElementTypeInfo(),
                            options);
                    result = JsonObjectInspectorFactory.getJsonListObjectInspector(elementObjectInspector);
                    break;
                }
                case MAP: {
                    MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                    ObjectInspector keyObjectInspector = getJsonObjectInspectorFromTypeInfo(mapTypeInfo.getMapKeyTypeInfo(), options);
                    ObjectInspector valueObjectInspector = getJsonObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo(), options);
                    result = JsonObjectInspectorFactory.getJsonMapObjectInspector(keyObjectInspector,
                            valueObjectInspector);
                    break;
                }
                case STRUCT: {
                    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                    List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
                    List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
                    List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
                            fieldTypeInfos.size());
                    for (int i = 0; i < fieldTypeInfos.size(); i++) {
                        fieldObjectInspectors.add(getJsonObjectInspectorFromTypeInfo(
                                fieldTypeInfos.get(i), options));
                    }
                    result = JsonObjectInspectorFactory.getJsonStructObjectInspector(fieldNames,
                            fieldObjectInspectors, options);
                    break;
                }
                case UNION:{
                    List<ObjectInspector> ois = new LinkedList<ObjectInspector>();
                    for(  TypeInfo ti : ((UnionTypeInfo) typeInfo).getAllUnionObjectTypeInfos()) {
                        ois.add(getJsonObjectInspectorFromTypeInfo(ti, options));
                    }
                    result = getJsonUnionObjectInspector(ois, options);
                    break;
                }

                default: {
                    result = null;
                }
            }
            cachedJsonObjectInspector.put(typeInfo, result);
        }
        return result;
    }


    static HashMap<ArrayList<Object>, JsonUnionObjectInspector> cachedJsonUnionObjectInspector
            = new HashMap<ArrayList<Object>, JsonUnionObjectInspector>();

    public static JsonUnionObjectInspector getJsonUnionObjectInspector(
            List<ObjectInspector> ois,
            JsonStructOIOptions options) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(ois);
        signature.add(options);
        JsonUnionObjectInspector result = cachedJsonUnionObjectInspector
                .get(signature);
        if (result == null) {
            result = new JsonUnionObjectInspector(ois, options);
            cachedJsonUnionObjectInspector.put(signature,result);

        }
        return result;
    }

    /*
     * Caches Struct Object Inspectors
     */
    static HashMap<ArrayList<Object>, JsonStructObjectInspector> cachedStandardStructObjectInspector
            = new HashMap<ArrayList<Object>, JsonStructObjectInspector>();


    public static JsonStructObjectInspector getJsonStructObjectInspector(
            List<String> structFieldNames,
            List<ObjectInspector> structFieldObjectInspectors,
            JsonStructOIOptions options) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(structFieldNames);
        signature.add(structFieldObjectInspectors);
        signature.add(options);

        JsonStructObjectInspector result = cachedStandardStructObjectInspector.get(signature);
        if (result == null) {
            result = new JsonStructObjectInspector(structFieldNames,
                    structFieldObjectInspectors, options);
            cachedStandardStructObjectInspector.put(signature, result);
        }
        return result;
    }

    /*
     * Caches the List objecvt inspectors
     */
    static HashMap<ArrayList<Object>, JsonListObjectInspector> cachedJsonListObjectInspector
            = new HashMap<ArrayList<Object>, JsonListObjectInspector>();

    public static JsonListObjectInspector getJsonListObjectInspector(
            ObjectInspector listElementObjectInspector) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(listElementObjectInspector);
        JsonListObjectInspector result = cachedJsonListObjectInspector
                .get(signature);
        if (result == null) {
            result = new JsonListObjectInspector(listElementObjectInspector);
            cachedJsonListObjectInspector.put(signature, result);
        }
        return result;
    }

    /*
     * Caches Map ObjectInspectors
     */
    static HashMap<ArrayList<Object>, JsonMapObjectInspector> cachedJsonMapObjectInspector
            = new HashMap<ArrayList<Object>, JsonMapObjectInspector>();

    public static JsonMapObjectInspector getJsonMapObjectInspector(
            ObjectInspector mapKeyObjectInspector,
            ObjectInspector mapValueObjectInspector) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(mapKeyObjectInspector);
        signature.add(mapValueObjectInspector);
        JsonMapObjectInspector result = cachedJsonMapObjectInspector
                .get(signature);
        if (result == null) {
            result = new JsonMapObjectInspector(mapKeyObjectInspector,
                    mapValueObjectInspector);
            cachedJsonMapObjectInspector.put(signature, result);
        }
        return result;
    }

   // static JsonStringJavaObjectInspector cachedStringObjectInspector = new JsonStringJavaObjectInspector();

    static final Map<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector> primitiveOICache
            = new EnumMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector>(PrimitiveCategory.class);

    static {
        primitiveOICache.put(PrimitiveCategory.STRING, new JavaStringJsonObjectInspector());
        primitiveOICache.put(PrimitiveCategory.BYTE, new JavaStringByteObjectInspector());
        primitiveOICache.put(PrimitiveCategory.SHORT, new JavaStringShortObjectInspector());
        primitiveOICache.put(PrimitiveCategory.INT, new JavaStringIntObjectInspector());
        primitiveOICache.put(PrimitiveCategory.LONG, new JavaStringLongObjectInspector());
        primitiveOICache.put(PrimitiveCategory.FLOAT, new JavaStringFloatObjectInspector());
        primitiveOICache.put(PrimitiveCategory.DOUBLE, new JavaStringDoubleObjectInspector());
        primitiveOICache.put(PrimitiveCategory.TIMESTAMP, new JavaStringTimestampObjectInspector());
        primitiveOICache.put(PrimitiveCategory.BOOLEAN, new JavaStringBooleanObjectInspector());
        // add the OIs that were introduced in different versions of hive
        TypeEntryShim.addObjectInspectors(primitiveOICache);
    }

    /**
     * gets the appropriate adapter wrapper around the object inspector if
     * necessary, that is, if we're dealing with numbers. The JSON parser won't
     * parse the number because it's deferred (lazy).
     *
     * @param primitiveCategory
     * @return
     */
    public static AbstractPrimitiveJavaObjectInspector getPrimitiveJavaObjectInspector(
            PrimitiveCategory primitiveCategory) {

            if(! primitiveOICache.containsKey(primitiveCategory)) {
                primitiveOICache.put(primitiveCategory, PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(primitiveCategory));
            }
            return  primitiveOICache.get(primitiveCategory);
    }


}
