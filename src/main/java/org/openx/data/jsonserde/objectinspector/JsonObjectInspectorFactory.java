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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 *
 * @author rcongiu
 */
public class JsonObjectInspectorFactory {
   

    static HashMap<TypeInfo, ObjectInspector> cachedJsonObjectInspector = new HashMap<TypeInfo, ObjectInspector>();
    /**
     * 
     * 
     * @see JsonUtils
     * @param typeInfo
     * @return 
     */
    public static ObjectInspector getJsonObjectInspectorFromTypeInfo(
            TypeInfo typeInfo) {
        ObjectInspector result = cachedJsonObjectInspector.get(typeInfo);
        if (result == null) {
            switch (typeInfo.getCategory()) {
                case PRIMITIVE: {
                    result = PrimitiveJSONObjectInspectorFactory.getPrimitiveJavaObjectInspector(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
                    break;
                }
                case LIST: {
                    ObjectInspector elementObjectInspector = getJsonObjectInspectorFromTypeInfo(((ListTypeInfo) typeInfo).getListElementTypeInfo());
                    result = JsonObjectInspectorFactory.getJsonListObjectInspector(elementObjectInspector);
                    break;
                }
                case MAP: {
                    MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                    ObjectInspector keyObjectInspector = getJsonObjectInspectorFromTypeInfo(mapTypeInfo.getMapKeyTypeInfo());
                    ObjectInspector valueObjectInspector = getJsonObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo());
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
                        fieldObjectInspectors.add(getJsonObjectInspectorFromTypeInfo(fieldTypeInfos.get(i)));
                    }
                    result = JsonObjectInspectorFactory.getJsonStructObjectInspector(fieldNames,
                            fieldObjectInspectors);
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

    
    
    /*
     * Caches Struct Object Inspectors
     */
    static HashMap<ArrayList<List<?>>, JsonStructObjectInspector> 
            cachedStandardStructObjectInspector =
            new HashMap<ArrayList<List<?>>, JsonStructObjectInspector>();

    public static JsonStructObjectInspector getJsonStructObjectInspector(
            List<String> structFieldNames,
            List<ObjectInspector> structFieldObjectInspectors) {
        ArrayList<List<?>> signature = new ArrayList<List<?>>();
        signature.add(structFieldNames);
        signature.add(structFieldObjectInspectors);
        JsonStructObjectInspector result = cachedStandardStructObjectInspector.get(signature);
        if (result == null) {
            result = new JsonStructObjectInspector(structFieldNames,
                    structFieldObjectInspectors);
            cachedStandardStructObjectInspector.put(signature, result);
        }
        return result;
    }
    
    /*
     * Caches the List objecvt inspectors
     */
  static HashMap<ArrayList<Object>, JsonListObjectInspector> 
          cachedJsonListObjectInspector = 
          new HashMap<ArrayList<Object>, JsonListObjectInspector>();

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
  
  static HashMap<ArrayList<Object>, JsonMapObjectInspector> 
          cachedJsonMapObjectInspector = 
          new HashMap<ArrayList<Object>, JsonMapObjectInspector>();

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

  
}
