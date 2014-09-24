package org.openx.data.jsonserde;
import static org.junit.Assert.*;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;
/**
 * Tests getJson Object
 *
 * 
 */
public class GetJsonObjectTest {
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
        tbl.setProperty(
                Constants.LIST_COLUMN_TYPES,
                ("string,string," + "string,"
                        + "ARRAY<STRUCT<kind:STRING,"
                        + "etag:STRING,"
                        + "id:STRING,"
                        + "v_statistics:STRUCT<viewCount:INT,likeCount:INT,dislikeCount:INT,favoriteCount:INT,commentCount:INT>,"
                        + "topicDetails:STRUCT<topicIds:ARRAY<STRING>,relevantTopicIds:ARRAY<STRING>>"
                        + ">>").toLowerCase());
        tbl.setProperty("mapping.v_items", "items");
        tbl.setProperty("mapping.v_statistics", "statistics");
        instance.initialize(conf, tbl);
        tbl.setProperty("mapping.v_items", "items");
        tbl.setProperty("mapping.v_statistics", "statistics");
        instance.initialize(conf, tbl);
    }
    @Test
    public void testGetJsonObject() throws SerDeException, JSONException {
        Writable w = new Text(
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": { \"totalResults\": 1, \"resultsPerPage\": 1 }, \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }");
        JSONObject result = (JSONObject) instance.deserialize(w);
        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
        Object res = soi.getStructFieldData(result, soi.getStructFieldRef("pageinfo"));
        StringObjectInspector loi = (StringObjectInspector) soi.getStructFieldRef("pageinfo")
                .getFieldObjectInspector();
        UDFJson udfJson = new UDFJson();
        Text output = udfJson.evaluate(loi.getPrimitiveJavaObject(res), "$.totalresults");
        assertEquals("1", output.toString());
    }
    @Test
    public void testNestedGetJsonObject() throws SerDeException, JSONException {
        Writable w = new Text(
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": { \"pagehit\":{ \"kind\": \"youtube#video\" } ,\"totalResults\": 1, \"resultsPerPage\": 1 }, \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }");
        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
        JSONObject result = (JSONObject) instance.deserialize(w);
        Object res = soi.getStructFieldData(result, soi.getStructFieldRef("pageinfo"));
        StringObjectInspector loi = (StringObjectInspector) soi.getStructFieldRef("pageinfo")
                .getFieldObjectInspector();
        UDFJson udfJson = new UDFJson();
        Text output = udfJson.evaluate(loi.getPrimitiveJavaObject(res), "$.pagehit");
        assertEquals("{\"kind\":\"youtube#video\"}", output.toString());
    }
    @Test
    public void testStringWhenNotJson() throws SerDeException, JSONException {
        Writable w = new Text(
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": \"page\", \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }");
        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
        JSONObject result = (JSONObject) instance.deserialize(w);
        Object res = soi.getStructFieldData(result, soi.getStructFieldRef("pageinfo"));
        StringObjectInspector loi = (StringObjectInspector) soi.getStructFieldRef("pageinfo")
                .getFieldObjectInspector();
        UDFJson udfJson = new UDFJson();
        Text output = udfJson.evaluate(loi.getPrimitiveJavaObject(res), "$.test_field");
        assertNull(output);
    }
    @Test
    public void testStringWhenFieldIsNotInJson() throws SerDeException, JSONException {
        Writable w = new Text(
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": { \"totalResults\": 1, \"resultsPerPage\": 1 }, \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }");
        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
        JSONObject result = (JSONObject) instance.deserialize(w);
        Object res = soi.getStructFieldData(result, soi.getStructFieldRef("pageinfo"));
        StringObjectInspector loi = (StringObjectInspector) soi.getStructFieldRef("pageinfo")
                .getFieldObjectInspector();
        UDFJson udfJson = new UDFJson();
        Text output = udfJson.evaluate(loi.getPrimitiveJavaObject(res), "$.test_field");
        assertNull(output);
    }
    @Test
    public void testStringWhenJson() throws SerDeException, JSONException {
        Writable w = new Text(
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": \"page\", \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }");
        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
        JSONObject result = (JSONObject) instance.deserialize(w);
        Object res = soi.getStructFieldData(result, soi.getStructFieldRef("pageinfo"));
        StringObjectInspector loi = (StringObjectInspector) soi.getStructFieldRef("pageinfo")
                .getFieldObjectInspector();
        String sres = loi.getPrimitiveJavaObject(res);
        assertEquals("page", sres);
    }
}