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

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONObject;



/**
 *
 * @author rcongiu
 */
public class NestedStructureTest {
     static JsonSerDe instance;

  @Before
  public void setUp() throws Exception {
    initialize();
  }

  static public void initialize() throws Exception {
    instance = new JsonSerDe();
    Configuration conf = null;
    Properties tbl = new Properties();
    // from google video API
    tbl.setProperty(Constants.LIST_COLUMNS, "kind,etag,pageInfo,v_items");
    tbl.setProperty(Constants.LIST_COLUMN_TYPES, ("string,string,"+ 
                "struct<totalResults:INT,resultsPerPage:INT>," + 
                "ARRAY<STRUCT<kind:STRING," +
                    "etag:STRING," +
                     "id:STRING," +
                     "v_statistics:STRUCT<viewCount:INT,likeCount:INT,dislikeCount:INT,favoriteCount:INT,commentCount:INT>," +
                     "topicDetails:STRUCT<topicIds:ARRAY<STRING>,relevantTopicIds:ARRAY<STRING>>" +
                      ">>").toLowerCase());
    tbl.setProperty("mapping.v_items" , "items");
    tbl.setProperty("mapping.v_statistics" , "statistics");

     instance.initialize(conf, tbl);
  }

  @Test
  public void testDeSerialize() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": { \"totalResults\": 1, \"resultsPerPage\": 1 }, \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }");

    JSONObject result = (JSONObject) instance.deserialize(w);
    
    StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
    
    assertEquals("youtube#videoListResponse", soi.getStructFieldData(result, soi.getStructFieldRef("kind")));
    assertEquals("\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\""
                , soi.getStructFieldData(result, soi.getStructFieldRef("etag")));
    
    // now, the trickier fields. pageInfo
    StructField pageInfoSF = soi.getStructFieldRef("pageinfo");
    
    Object pageInfo = soi.getStructFieldData(result, pageInfoSF);
    StructObjectInspector pageInfoOI = (StructObjectInspector) pageInfoSF.getFieldObjectInspector();
    
    // should have only 2 elements, totalResults and ResultsPerPage
    assertEquals(2, pageInfoOI.getAllStructFieldRefs().size());
    
    // now, let's check totalResults
    StructField trSF = pageInfoOI.getStructFieldRef("totalresults");
    Object totalResults = pageInfoOI.getStructFieldData(pageInfo, trSF);
    
    assertTrue(trSF.getFieldObjectInspector().getCategory() == Category.PRIMITIVE);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) trSF.getFieldObjectInspector();
    assertTrue(poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT);
    
    SettableIntObjectInspector sioi = (SettableIntObjectInspector) poi;
    int value = sioi.get(totalResults);
    
    
    assertEquals(1, value);
    
  }

  

}
