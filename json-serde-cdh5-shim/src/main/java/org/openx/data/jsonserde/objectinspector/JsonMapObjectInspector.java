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

import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;

/**
 *
 * @author rcongiu
 */
public class JsonMapObjectInspector extends StandardMapObjectInspector {
  
    public JsonMapObjectInspector(ObjectInspector mapKeyObjectInspector, 
            ObjectInspector mapValueObjectInspector) {
        super(mapKeyObjectInspector, mapValueObjectInspector);
    }


  @Override
  public Map<?, ?> getMap(Object data) {
    if (data == null) {
      return null;
    }
    
    JSONObject jObj = (JSONObject) data;
    
    return new JSONObjectMapAdapter(jObj);
  }

  @Override
  public int getMapSize(Object data) {
    if (data == null) {
      return -1;
    }
     JSONObject jObj = (JSONObject) data;
    return jObj.length();
  }

  @Override
  public Object getMapValueElement(Object data, Object key) {
    if (data == null) {
      return -1;
    }
    
     JSONObject jObj = (JSONObject) data;
        try {
            Object obj = jObj.get(key.toString());
            if(JSONObject.NULL == obj) {
                return null;
            } else {
                return obj;
            }
        } catch (JSONException ex) {
            // key does not exists -> like null
            return null;
        }
  }   
}
