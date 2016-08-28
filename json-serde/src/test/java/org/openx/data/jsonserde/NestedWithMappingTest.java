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

package org.openx.data.jsonserde;

import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONObject;
import org.openx.data.jsonserde.objectinspector.primitive.JavaStringIntObjectInspector;



/**
 *
 * @author rcongiu
 */
public class NestedWithMappingTest {
     static JsonSerDe instance;

  @Before
  public void setUp() throws Exception {
    initialize();
  }

  static public void initialize() throws Exception {
    instance = new JsonSerDe();
    Configuration conf = null;
    Properties tbl = new Properties();
 /*
    create table json_84(
       ts string,
       t int,
       request struct<
       path:string,
       ip:string,
       headers: struct<
       useragent:array<string>
       >
       >
       )
       ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
       WITH SERDEPROPERTIES ("mapping.useragent" = "User-Agent")
       STORED AS TEXTFILE;

    */
    tbl.setProperty(serdeConstants.LIST_COLUMNS, "ts,t,request");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, ("string,int," +
        "struct<path:string,ip:string,headers:struct<useragent:array<string>>>").
            toLowerCase());
    tbl.setProperty("mapping.useragent" , "User-Agent");

     instance.initialize(conf, tbl);
  }

  @Test
  public void testDeSerialize() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{ \"ts\":\"2014-08-25T00:24:27.41103928Z\", \"t\":36529, \"Request\":{ \"path\":\"/foo/bar\", \"query\":{\"baz\": [\"ban\"]}, \"headers\":{ \"Accept\":[\"image/webp,*/*;q=0.8\"], \"Accept-Encoding\":[\"identity\"], \"Accept-Language\":[\"en-US,en;q=0.8\"], \"Connection\":[\"keep-alive\"], \"Referer\":[\"http://foo.com/bar\"], \"User-Agent\":[\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36\"] }, \"ip\":\"10.0.0.1\" } }");

    JSONObject result = (JSONObject) instance.deserialize(w);
    
    StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
    
    StructField tSF = soi.getStructFieldRef("t");
    assertEquals(36529, ((JavaStringIntObjectInspector)tSF.getFieldObjectInspector()).get(soi.getStructFieldData(result, tSF )));
    assertEquals("2014-08-25T00:24:27.41103928Z"
                , soi.getStructFieldData(result, soi.getStructFieldRef("ts")));
    
    StructField requestSF = soi.getStructFieldRef("request");
    
    Object request = soi.getStructFieldData(result, requestSF);
    
    StructObjectInspector requestOI = (StructObjectInspector) requestSF.getFieldObjectInspector();
    
    assertEquals(3, requestOI.getAllStructFieldRefs().size());
    

    StructField headersSF = requestOI.getStructFieldRef("headers");
    Object headers = requestOI.getStructFieldData(request, headersSF);
    
    assertTrue(headersSF.getFieldObjectInspector().getCategory() == Category.STRUCT);
    StructObjectInspector headersOI = (StructObjectInspector) headersSF.getFieldObjectInspector();
    
    // now get the user agent with the mapping
    StructField useragentSF = headersOI.getStructFieldRef("useragent");
    Object useragent = requestOI.getStructFieldData(headers, useragentSF);
    ListObjectInspector useragentOI = (ListObjectInspector) useragentSF.getFieldObjectInspector();
    assertEquals(useragentOI.getCategory(), Category.LIST);
    
    // get value
    List d = useragentOI.getList(useragent);
    assertEquals(d.size(),1);
  }

  

}
