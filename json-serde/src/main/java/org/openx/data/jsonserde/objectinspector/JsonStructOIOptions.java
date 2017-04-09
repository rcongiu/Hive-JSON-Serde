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
        boolean caseInsensitive = true; // converts JSON keys to case sensitive
        boolean dotsInKeyNames = false;
        public Map<String, String> getMappings() {
            return mappings;
        }

     public boolean isDotsInKeyNames() {
         return dotsInKeyNames;
     }

     public void setDotsInKeyNames(boolean dotsInKeyNames) {
         this.dotsInKeyNames = dotsInKeyNames;
     }
     public void setCaseInsensitive(boolean c) { this.caseInsensitive = c; }
     public boolean isCaseInsensitive() { return this.caseInsensitive; }

     @Override
     public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         JsonStructOIOptions that = (JsonStructOIOptions) o;

         if (dotsInKeyNames != that.dotsInKeyNames) return false;
         return mappings != null ? mappings.equals(that.mappings) : that.mappings == null;

     }

     @Override
     public int hashCode() {
         int result = mappings != null ? mappings.hashCode() : 0;
         result = 31 * result + (dotsInKeyNames ? 1 : 0);
         return result;
     }
 }
