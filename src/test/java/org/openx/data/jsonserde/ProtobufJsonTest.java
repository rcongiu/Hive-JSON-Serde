package org.openx.data.jsonserde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONObject;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: ps16819
 * Date: 6/3/14
 * Time: 1:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProtobufJsonTest {
    public void initialize(JsonSerDe instance) throws Exception {
        System.out.println("initialize");

        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, "one");
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "string");

        instance.initialize(conf, tbl);
    }

    @Test
    public void deserializeStrangeString() throws Exception {
        JsonSerDe instance = new JsonSerDe();
        initialize(instance);

        System.out.println("deserialize");
        Writable w = new Text("{\"one\":\"\\a\\v\"}");

        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals("",result.get("one").toString().trim());
    }
}
