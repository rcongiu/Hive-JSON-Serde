package org.openx.data.jsonserde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONArray;
import org.openx.data.jsonserde.json.JSONObject;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by rcongiu on 8/30/15.
 */
public class JsonUnionTest {
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
        tbl.setProperty(serdeConstants.LIST_COLUMNS, "country,stuff");
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,uniontype<int,double,array<string>,struct<a:int,b:string>,string>".toLowerCase());

        instance.initialize(conf, tbl);
    }

    @Test
    public void testDeSerialize() throws Exception {
        // Test that timestamp object can be deserialized


        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

        StructField sfr = soi.getStructFieldRef("stuff");

        assertEquals(sfr.getFieldObjectInspector().getCategory(), ObjectInspector.Category.UNION);

        UnionObjectInspector uoi = (UnionObjectInspector) sfr.getFieldObjectInspector();

        // first, string
        Writable w = new Text("{\"country\":\"Switzerland\",\"stuff\":\"Italian\"}");
        JSONObject result = (JSONObject) instance.deserialize(w);
        Object val =  soi.getStructFieldData(result, sfr) ;
        assertEquals("Italian", uoi.getField(val));

        uoi.getTypeName();

        // now, int
        w = new Text("{\"country\":\"Switzerland\",\"stuff\":2}");
        result = (JSONObject) instance.deserialize(w);
        val =  soi.getStructFieldData(result, sfr) ;
        assertEquals("2", val.toString());
        assertEquals(0, uoi.getTag(val));

        // now, struct
        w = new Text("{\"country\":\"Switzerland\",\"stuff\": { \"a\": \"OK\" } }");
        result = (JSONObject) instance.deserialize(w);
        val =  soi.getStructFieldData(result, sfr) ;
        assertTrue(val instanceof JSONObject);
        assertEquals(3, uoi.getTag(val));

        // now, array
        w = new Text("{\"country\":\"Switzerland\",\"stuff\": [ 1, 2 ] }");
        result = (JSONObject) instance.deserialize(w);
        val =  soi.getStructFieldData(result, sfr) ;
        assertTrue(val instanceof JSONArray);
        assertEquals(2, uoi.getTag(val));
    }
}
