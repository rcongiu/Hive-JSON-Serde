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

/**
 *
 * @author rcongiu
 */
 /**
     * We introduce this to carry mappings and other options
     * we may want to support in the future.
     * Signature for caching will be built using this.
     */
public  class JsonStructOIOptions {
        Map<String,String> mappings;
        public JsonStructOIOptions (Map<String,String> mp) {
            mappings = mp;
        }

        public Map<String, String> getMappings() {
            return mappings;
        }

        
        
        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof JsonStructOIOptions) || obj == null) {
                return false ;
            } else {
                JsonStructOIOptions oio = (JsonStructOIOptions) obj;
                
                if(mappings != null) {
                    return mappings.equals(oio.mappings);
                } else {
                    return mappings == oio.mappings;
                }
            }
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 67 * hash + (this.mappings != null ? this.mappings.hashCode() : 0);
            return hash;
        }
        
        
    }
