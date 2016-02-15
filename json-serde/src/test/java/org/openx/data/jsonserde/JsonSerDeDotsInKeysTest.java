package org.openx.data.jsonserde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONObject;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by OpenDataFlow on 14/02/16.
 */
public class JsonSerDeDotsInKeysTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testDeSerialize() throws Exception {
        JsonSerDe instance = new JsonSerDe();
        Configuration conf = null;
        Properties tbl = new Properties();
        // from google video API
        tbl.setProperty(serdeConstants.LIST_COLUMNS, "kind_with_dots,etag,pageInfo");
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, ("string,string,"+
                "struct<inner_with_dots:string,resultsPerPage:INT>").toLowerCase());

        tbl.setProperty(JsonSerDe.PROP_DOTS_IN_KEYS, "true");

        instance.initialize(conf, tbl);


        // Test that timestamp object can be deserialized
        Writable w = new Text("{ \"kind.with.dots\": \"youtube#videoListResponse\"," +
                "\"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\"," +
                "\"pageInfo\": { \"inner.with.dots\": \"hello\", \"resultsPerPage\": 1 }}");

        JSONObject result = (JSONObject) instance.deserialize(w);

        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

        assertEquals("youtube#videoListResponse", soi.getStructFieldData(result, soi.getStructFieldRef("kind_with_dots")));
        assertEquals("\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\""
                , soi.getStructFieldData(result, soi.getStructFieldRef("etag")));

        // now, the trickier fields. pageInfo
        StructField pageInfoSF = soi.getStructFieldRef("pageinfo");

        Object pageInfo = soi.getStructFieldData(result, pageInfoSF);
        StructObjectInspector pageInfoOI = (StructObjectInspector) pageInfoSF.getFieldObjectInspector();

        // should have only 2 elements, totalResults and ResultsPerPage
        assertEquals(2, pageInfoOI.getAllStructFieldRefs().size());

        // now, let's check totalResults
        StructField trSF = pageInfoOI.getStructFieldRef("inner_with_dots");
        Object innerWithDotsResult = pageInfoOI.getStructFieldData(pageInfo, trSF);

        assertTrue(trSF.getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE);
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) trSF.getFieldObjectInspector();
        assertTrue(poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING);

        assertEquals("hello", innerWithDotsResult);
    }

    @Test
    public void testDisabledByDefault() throws Exception {
        JsonSerDe instance = new JsonSerDe();
        Configuration conf = null;
        Properties tbl = new Properties();
        // from google video API
        tbl.setProperty(serdeConstants.LIST_COLUMNS, "kind_with_dots,etag,pageInfo");
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, ("string,string,"+
                "struct<inner_with_dots:string,resultsPerPage:INT>").toLowerCase());

        instance.initialize(conf, tbl);

        // Test that timestamp object can be deserialized
        Writable w = new Text("{ \"kind.with.dots\": \"youtube#videoListResponse\"," +
                "\"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\"," +
                "\"pageInfo\": { \"inner.with.dots\": \"hello\", \"resultsPerPage\": 1 }}");

        JSONObject result = (JSONObject) instance.deserialize(w);

        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

        assertEquals(null, soi.getStructFieldData(result, soi.getStructFieldRef("kind_with_dots")));
        assertEquals("\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\""
                , soi.getStructFieldData(result, soi.getStructFieldRef("etag")));

        // now, the trickier fields. pageInfo
        StructField pageInfoSF = soi.getStructFieldRef("pageinfo");

        Object pageInfo = soi.getStructFieldData(result, pageInfoSF);
        StructObjectInspector pageInfoOI = (StructObjectInspector) pageInfoSF.getFieldObjectInspector();

        // should have only 2 elements, totalResults and ResultsPerPage
        assertEquals(2, pageInfoOI.getAllStructFieldRefs().size());

        // now, let's check totalResults
        StructField trSF = pageInfoOI.getStructFieldRef("inner_with_dots");
        Object innerWithDotsResult = pageInfoOI.getStructFieldData(pageInfo, trSF);

        assertTrue(trSF.getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE);
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) trSF.getFieldObjectInspector();
        assertTrue(poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING);

        assertEquals(null, innerWithDotsResult);
    }
}
