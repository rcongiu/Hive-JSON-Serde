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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONArray;
import org.openx.data.jsonserde.json.JSONObject;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

/**
 * User: guyrt
 */
public class JsonSerDeTimeStampTest {

  static JsonSerDe instance;

  @Before
  public void setUp() throws Exception {
    initialize();
  }

  static public void initialize() throws Exception {
    instance = new JsonSerDe();
    Configuration conf = null;
    Properties tbl = new Properties();
    tbl.setProperty(Constants.LIST_COLUMNS, "one,two,three,four,five");
    tbl.setProperty(Constants.LIST_COLUMN_TYPES, "boolean,float,array<string>,string,timestamp");

    instance.initialize(conf, tbl);
  }

  @Test
  public void testTimestampDeSerialize() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":\"2013-03-27 23:18:40\"}");

    JSONObject result = (JSONObject) instance.deserialize(w);
    assertEquals(Timestamp.valueOf("2013-03-27 23:18:40.0"), result.get("five"));
  }

  @Test
  public void testTimestampDeSerializeWithNanoseconds() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":\"2013-03-27 23:18:40.123456\"}");

    JSONObject result = (JSONObject) instance.deserialize(w);
    assertEquals( Timestamp.valueOf("2013-03-27 23:18:40.123456"), result.get("five"));
  }
  
   @Test
  public void testTimestampDeSerializeNumericTimestamp() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":1367801925}");

    
    JSONObject result = (JSONObject) instance.deserialize(w);
    assertEquals(getDate("2013-05-05 17:58:45.0" ), result.get("five")   );
  }

  @Test
  public void testTimestampDeSerializeNumericTimestampWithNanoseconds() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":1367801925.123}");
// 
    JSONObject result = (JSONObject) instance.deserialize(w);
    
    assertEquals(getDate("2013-05-05 17:58:45.123"), result.get("five") );
  }
  
  /** 
   * for tests, if time zone not specified, make sure that it's in the correct
   * timezone
   */
  static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS zzz");
  public static Timestamp getDate(String s) throws ParseException {
      
      s = s + " PDT";
      
      Date dt = sdf.parse(s);
      Calendar cal = Calendar.getInstance();
      cal.setTime(dt);
      Timestamp ts = new Timestamp(cal.getTimeInMillis());
      return ts;
      
  }


}
