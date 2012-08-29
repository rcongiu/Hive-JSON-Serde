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

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.openx.data.jsonserde.json.JSONArray;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;

/**
 *
 * @author rcongiu
 */
public class JsonListObjectInspector extends StandardListObjectInspector {

    JsonListObjectInspector(ObjectInspector listElementObjectInspector) {
        super(listElementObjectInspector);
    }

     @Override
  public List<?> getList(Object data) {
    if (data == null || JSONObject.NULL.equals(data)) {
      return null;
    }
    JSONArray array = (JSONArray) data;
    return array.getAsArrayList();
  }

  @Override
  public Object getListElement(Object data, int index) {
    if (data == null) {
      return null;
    }
    JSONArray array = (JSONArray) data;
    try {
        return array.get(index);
    } catch(JSONException ex) {
        return null;
    }
  }

  @Override
  public int getListLength(Object data) {
    if (data == null) {
      return -1;
    }
    JSONArray array = (JSONArray) data;
    return array.length();
  }
    
}
