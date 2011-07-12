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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;

/**
 * JSONObject is technically different from a map, since a json object
 * has string keys and values can be of any kind, while hive could 
 * define a column as map<string,string>. This consistency is left
 * to the user to satisfy.
 * 
 * Since we want to present a map, and we don't want to extract keys
 * and values every time, we keep a cache
 * 
 * @author rcongiu
 */
/* TODO: having an initialize() that copies everything into a map is expensive
   and won't work well for large maps. Good enough for now since
   we seldom - if ever - use maps. */
public class JSONObjectMapAdapter implements Map {
    HashMap cache;
    JSONObject jsonObject;
    
    public JSONObjectMapAdapter(JSONObject obj) {
        jsonObject = obj;
        initialize();
    }

    public JSONObjectMapAdapter() {
        
    }

    public JSONObject getJSONObject() {
        return jsonObject;
    }

    public void setJSONObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        initialize();
    }
    
    

    @Override
    public int size() {
        return jsonObject.length();
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }
    
    protected final void initialize() {
        if(cache==null) cache = new HashMap();
        
        for(Iterator<String> i = jsonObject.keys(); i.hasNext(); ) {
            String o = i.next();
            try {
                cache.put(o, jsonObject.get(o));
            } catch (JSONException ex) {
                // if key does not exist - should not happen
                throw new RuntimeException("Non existent key - should never happen!");
            }
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return cache.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
            return cache.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return get(key);
    }

    @Override
    public Object put(Object key, Object value) {
        return put(key,value);
    }

    @Override
    public Object remove(Object key) {
       return cache.remove(key);
    }

    @Override
    public void putAll(Map m) {
        cache.putAll(m);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public Set keySet() {
        return cache.keySet();
    }

    @Override
    public Collection values() {
        return cache.values();
    }

    @Override
    public Set entrySet() {
        return cache.entrySet();
    }

    public Map<?, ?> getMap() {
        return cache;
    }
    
}
