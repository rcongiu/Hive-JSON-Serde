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
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    assertEquals(result.get("five"), Timestamp.valueOf("2013-03-27 23:18:40.0"));
  }

  @Test
  public void testTimestampDeSerializeWithNanoseconds() throws Exception {
    // Test that timestamp object can be deserialized
    Writable w = new Text("{\"one\":true,\"five\":\"2013-03-27 23:18:40.123456\"}");

    JSONObject result = (JSONObject) instance.deserialize(w);
    assertEquals(result.get("five"), Timestamp.valueOf("2013-03-27 23:18:40.123456"));
  }


}
