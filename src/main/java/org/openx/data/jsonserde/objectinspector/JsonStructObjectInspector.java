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
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;

/**
 * This Object Inspector is used to look into a JSonObject object.
 * We couldn't use StandardStructObjectInspector since that expects 
 * something that can be cast to an Array<Object>.
 * @author rcongiu
 */
public class JsonStructObjectInspector extends StandardStructObjectInspector {
    public JsonStructObjectInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
       super(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    JSONObject obj = (JSONObject) data;
    MyField f = (MyField) fieldRef; 

    int fieldID = f.getFieldID();
    assert (fieldID >= 0 && fieldID < fields.size());

    try {
        Object result = obj.get(f.getFieldName());
        //if(!f.getFieldObjectInspector().getTypeName().equalsIgnoreCase(result.getClass().getName())) return null;
        return result;
    } catch (JSONException ex) {
        // if key does not exist
        return null; 
    }
  }

    static List<Object> values = new ArrayList<Object>();
    @Override
    public List<Object> getStructFieldsDataAsList(Object o) {
        JSONObject jObj = (JSONObject) o;
        values.clear();
        
        for(int i =0; i< fields.size(); i ++) {
            Object result = null;
            try {
                result = jObj.get(fields.get(i).getFieldName());
                 } catch (JSONException ex) {
                    try{
                        result = jObj.get(fields.get(i).getFieldName().replaceAll("_", "-"));
                    } catch(JSONException ex2){
                        
                    }
                    // this can happen if a key doesn't exist in the json object
                    // not totally out of the bounds of imagination for log data.
                }
                values.add(result);
        }

        return values;
    }
    
}
