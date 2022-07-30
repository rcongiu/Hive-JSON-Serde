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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.velocity.runtime.directive.Parse;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.openx.data.jsonserde.objectinspector.primitive.JavaStringTimestampObjectInspector;
import org.openx.data.jsonserde.objectinspector.primitive.ParsePrimitiveUtils;


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
    tbl.setProperty(serdeConstants.LIST_COLUMNS, "one,two,three,four,five");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, "boolean,float,array<string>,string,timestamp");

    instance.initialize(conf, tbl);
  }

  @Test
  public void testTimestampDeSerialize() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":\"2013-03-27 23:18:40\"}");

    JSONObject result = (JSONObject) instance.deserialize(w);
    
    StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
    
    JavaStringTimestampObjectInspector jstOi = (JavaStringTimestampObjectInspector) 
            soi.getStructFieldRef("five").getFieldObjectInspector();
    assertEquals(Timestamp.valueOf("2013-03-27 23:18:40.0"), jstOi.getPrimitiveJavaObject(result.get("five")));
  }

  @Test
  public void testUTCTimestampDeSerialize() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":\"2013-03-27T23:18:40Z\"}");

    JSONObject result = (JSONObject) instance.deserialize(w);

    StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

    JavaStringTimestampObjectInspector jstOi = (JavaStringTimestampObjectInspector)
            soi.getStructFieldRef("five").getFieldObjectInspector();
    assertEquals(Timestamp.valueOf("2013-03-27 23:18:40.0"), jstOi.getPrimitiveJavaObject(result.get("five")));
  }

  @Test
  public void testTimestampDeSerializeWithNanoseconds() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":\"2013-03-27 23:18:40.123456\"}");

    JSONObject result = (JSONObject) instance.deserialize(w);
    
    StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
    
    JavaStringTimestampObjectInspector jstOi = (JavaStringTimestampObjectInspector) 
            soi.getStructFieldRef("five").getFieldObjectInspector();
    assertEquals( Timestamp.valueOf("2013-03-27 23:18:40.123456"),  jstOi.getPrimitiveJavaObject(result.get("five")));
  }

  @Test
  public void testTimestampDeSerializeWithPlusHours() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":\"2017-08-17T10:46:04+0300\"}");

    JSONObject result = (JSONObject) instance.deserialize(w);

    StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

    JavaStringTimestampObjectInspector jstOi = (JavaStringTimestampObjectInspector)
            soi.getStructFieldRef("five").getFieldObjectInspector();
    assertEquals( Timestamp.valueOf("2017-08-17 07:46:04.0"),  jstOi.getPrimitiveJavaObject(result.get("five")));
  }
  
   @Test
  public void testTimestampDeSerializeNumericTimestamp() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":1367801925}");

    JSONObject result = (JSONObject) instance.deserialize(w);
     StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
    JavaStringTimestampObjectInspector jstOi = (JavaStringTimestampObjectInspector) 
            soi.getStructFieldRef("five").getFieldObjectInspector();
    assertEquals(getDate("2013-05-05 17:58:45.0" ), 
            jstOi.getPrimitiveJavaObject(result.get("five"))   );
  }

  @Test
  public void testTimestampDeSerializeNumericTimestampWithNanoseconds() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":1367801925.123}");
// 
    JSONObject result = (JSONObject) instance.deserialize(w);
     StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
    JavaStringTimestampObjectInspector jstOi = (JavaStringTimestampObjectInspector) 
            soi.getStructFieldRef("five").getFieldObjectInspector();
    assertEquals(getDate("2013-05-05 17:58:45.123"), 
            jstOi.getPrimitiveJavaObject(result.get("five")) );
  }

  @Test
  public void testTimestampDeSerializeNumericTimestampWithMilliseconds() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":1367801925123}");
// 
    JSONObject result = (JSONObject) instance.deserialize(w);
     StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
    JavaStringTimestampObjectInspector jstOi = (JavaStringTimestampObjectInspector) 
            soi.getStructFieldRef("five").getFieldObjectInspector();
    assertEquals(getDate("2013-05-05 17:58:45.123"), 
            jstOi.getPrimitiveJavaObject(result.get("five")) );
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
  
  @Test
  public void testformatDateFromUTC() throws ParseException {
    System.out.println("testFormatDateFromUTC");
    String string1 = "2001-07-04T12:08:56Z";
    assertEquals("2001-07-04 12:08:56.000", ParsePrimitiveUtils.nonUTCFormat(string1));
  }

  @Test
  public void testParseUTCTimestampWithMillis() throws ParseException {
    assertEquals(Timestamp.valueOf("2020-07-30 15:20:05.424"),
        ParsePrimitiveUtils.parseTimestamp("2020-07-30T15:20:05.424Z"));
  }

  @Test
  public void testSerializeTimestamp() throws SerDeException, JSONException {
    System.out.println("testSerializeTimestamp");

    JsonSerDe serde = new JsonSerDe();
    Configuration conf = null;
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.LIST_COLUMNS, "one,two,three");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, "boolean,string,timestamp"); // one timestamp field
    serde.initialize(conf, tbl);

    ArrayList<Object> row = new ArrayList<Object>(3);

    List<ObjectInspector> lOi = new LinkedList<ObjectInspector>();
    List<String> fieldNames = new LinkedList<String>();

    row.add(Boolean.TRUE);
    fieldNames.add("one");
    lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

    row.add("field");
    fieldNames.add("two");
    lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA));


    java.sql.Timestamp ts = new Timestamp(1326439500L);
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.set(2015,10,12,22,33,44); // month is zero-based!

    row.add( new Timestamp(cal.getTime().getTime())); // see http://docs.oracle.com/javase/7/docs/api/java/util/Date.html#UTC(int,%20int,%20int,%20int,%20int,%20int)
    fieldNames.add("three");
    lOi.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);

    StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, lOi);

    Object obj = serde.serialize(row, soi);

    assertTrue(obj instanceof Text);
    String serialized = obj.toString();
    System.out.println("Returned " + serialized);
    // serialization does not guarantee the order of the fields, so we only check the fields
    // one by one
    assertTrue(serialized.contains("\"one\":true"));
    assertTrue(serialized.contains("\"two\":\"field\""));
    assertTrue(serialized.contains("\"three\":\"2015-11-12T22:33:44Z\"")); // UTC

  }

}
