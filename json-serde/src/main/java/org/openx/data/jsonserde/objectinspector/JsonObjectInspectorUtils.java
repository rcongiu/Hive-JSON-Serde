/*
 * Copyright 2014 rcongiu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openx.data.jsonserde.objectinspector;

import org.apache.hadoop.io.Text;

/**
 *
 * @author rcongiu
 */
public class JsonObjectInspectorUtils {
      public static Object checkObject(Object data) {
      // just check for null first thing
      if(data == null) return data;
      
      if(data instanceof String ||
         data instanceof Text) {
          String str = data instanceof String ? (String)data : ((Text)data).toString();
          if(str.trim().isEmpty())
            return null;
      }
      return data;
  }
    
}
