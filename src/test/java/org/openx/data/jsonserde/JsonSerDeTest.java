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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import org.openx.data.jsonserde.json.JSONArray;
import org.apache.hadoop.io.Text;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rcongiu
 */
public class JsonSerDeTest {

    public JsonSerDeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() {
    }
    
    public void initialize(JsonSerDe instance) throws Exception {
        System.out.println("initialize");

        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, "one,two,three,four");
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "boolean,float,array<string>,string");

        instance.initialize(conf, tbl);
    }
    
     public void initialize2(JsonSerDe instance) throws Exception {
        System.out.println("initialize");

        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, "one,two,three,four,five");
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "boolean,float,array<string>,string,string");

        instance.initialize(conf, tbl);
    }
    

    /**
     * Test of deserialize method, of class JsonSerDe.
     */
    @Test
    public void testDeserialize() throws Exception {
        JsonSerDe instance = new JsonSerDe();
        initialize(instance);
        
        System.out.println("deserialize");
        Writable w = new Text("{\"one\":true,\"three\":[\"red\",\"yellow\",\"orange\"],\"two\":19.5,\"four\":\"poop\"}");

        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("four"), "poop");

        assertTrue(result.get("three") instanceof JSONArray);

        assertTrue(((JSONArray) result.get("three")).get(0) instanceof String);
        assertEquals(((JSONArray) result.get("three")).get(0), "red");
    }

    //   {"one":true,"three":["red","yellow",["blue","azure","cobalt","teal"],"orange"],"two":19.5,"four":"poop"}
    @Test
    public void testDeserialize2() throws Exception {
        JsonSerDe instance = new JsonSerDe();
        initialize(instance);
        
        Writable w = new Text("{\"one\":true,\"three\":[\"red\",\"yellow\",[\"blue\",\"azure\",\"cobalt\",\"teal\"],\"orange\"],\"two\":19.5,\"four\":\"poop\"}");

        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("four"), "poop");

        assertTrue(result.get("three") instanceof JSONArray);

        assertTrue(((JSONArray) result.get("three")).get(0) instanceof String);
        assertEquals(((JSONArray) result.get("three")).get(0), "red");
    }
    
    @Test
    public void testDeserialize2Initializations() throws Exception {
        JsonSerDe instance = new JsonSerDe();
        initialize(instance);
        
        Writable w = new Text("{\"one\":true,\"three\":[\"red\",\"yellow\",[\"blue\",\"azure\",\"cobalt\",\"teal\"],\"orange\"],\"two\":19.5,\"four\":\"poop\"}");

        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("four"), "poop");

        assertTrue(result.get("three") instanceof JSONArray);

        assertTrue(((JSONArray) result.get("three")).get(0) instanceof String);
        assertEquals(((JSONArray) result.get("three")).get(0), "red");
        
        // second initialization, new column
        initialize2(instance);
        
        result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("four"), "poop");

        assertTrue(result.get("three") instanceof JSONArray);

        assertTrue(((JSONArray) result.get("three")).get(0) instanceof String);
        assertEquals(((JSONArray) result.get("three")).get(0), "red");
    }
    

    /**
     * Test of getSerializedClass method, of class JsonSerDe.
     */
    @Test
    public void testGetSerializedClass() throws Exception {
        JsonSerDe instance = new JsonSerDe();
        initialize(instance);
        
        System.out.println("getSerializedClass");
        Class expResult = Text.class;
        Class result = instance.getSerializedClass();
        assertEquals(expResult, result);

    }

    /**
     * Test of serialize method, of class JsonSerDe.
     */
    /*    @Test
     public void testSerialize() throws Exception {
     System.out.println("serialize");
     Object o = null;
     ObjectInspector oi = null;
     JsonSerDe instance = new JsonSerDe();
     Writable expResult = null;
     Writable result = instance.serialize(o, oi);
     assertEquals(expResult, result);
     }
     *  
     */
    // @Test
    public void testSerialize() throws SerDeException, JSONException, Exception {
        System.out.println("serialize");
        
        JsonSerDe instance = new JsonSerDe();
        initialize(instance);
        
        ArrayList row = new ArrayList(5);

        List<ObjectInspector> lOi = new LinkedList<ObjectInspector>();
        List<String> fieldNames = new LinkedList<String>();

        row.add("HELLO");
        fieldNames.add("atext");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        row.add(10);
        fieldNames.add("anumber");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        List<String> array = new LinkedList<String>();
        array.add("String1");
        array.add("String2");

        row.add(array);
        fieldNames.add("alist");
        lOi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));

        Map<String, String> m = new HashMap<String, String>();
        m.put("k1", "v1");
        m.put("k2", "v2");

        row.add(m);
        fieldNames.add("amap");
        lOi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));


        StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, lOi);

        Object result = instance.serialize(row, soi);

        JSONObject res = new JSONObject(result.toString());
        assertEquals(res.getString("atext"), row.get(0));

        assertEquals(res.get("anumber"), row.get(1));

        // after serialization the internal contents of JSONObject are destroyed (overwritten by their string representation
        // (for map and arrays) 

        System.out.println("Serialized to " + result.toString());

    }
    
    
    public JsonSerDe getMappedSerde() throws SerDeException {
        System.out.println("testMapping");
        JsonSerDe serde = new JsonSerDe();
        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, "one,two,three,four,ts");
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "boolean,float,array<string>,string,int");
        // this means, we call it ts but in data it's 'timestamp'
        tbl.setProperty("mapping.ts", "timestamp");

        serde.initialize(conf, tbl);
        return serde;
    }
    
    @Test
    public void testSerializeWithMapping() throws SerDeException, JSONException {
        System.out.println("testSerializeWithMapping");  
        
        JsonSerDe serde = getMappedSerde();
        
        System.out.println("serialize");
        ArrayList row = new ArrayList(5);

        List<ObjectInspector> lOi = new LinkedList<ObjectInspector>();
        List<String> fieldNames = new LinkedList<String>();
        
        row.add(Boolean.TRUE);
        fieldNames.add("one");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        
        row.add(new Float(43.2));
        fieldNames.add("two");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Float.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        
        List<String> lst = new LinkedList<String>();
        row.add(lst);
        fieldNames.add("three");
        lOi.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
                .getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));
        
        row.add("value1");
        fieldNames.add("four");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        
        row.add(new Integer(7898));
        fieldNames.add("ts");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        
        StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, lOi);
        
        Object obj = serde.serialize(row, soi);
        
        assertTrue(obj instanceof Text);
        assertEquals("{\"timestamp\":7898,\"two\":43.2,\"one\":true,\"three\":[],\"four\":\"value1\"}", obj.toString());
        
        System.out.println("Output object " + obj.toString());
    }
    
    // {"one":true, "timestamp":1234567, "three":["red","yellow",["blue","azure","cobalt","teal"],"orange"],"two":19.5,"four":"poop"}
    @Test
    public void testMapping() throws SerDeException, IOException {
        System.out.println("testMapping");
        JsonSerDe serde = getMappedSerde();
        
        InputStream is = this.getClass().getResourceAsStream("/testkeyword.txt");
        
        LineNumberReader lnr = new LineNumberReader(new InputStreamReader(is));
        
        StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
        StructField sf = soi.getStructFieldRef("ts");
        
        String line;
        while( (line = lnr.readLine()) != null ) {
            Text t = new Text(line);
            
            Object res = serde.deserialize(t);   
            assertEquals(1234567, soi.getStructFieldData(res, sf)  );
            
        }
        
        try {
            is.close();
        } catch (Exception ex){}
    }
    
}
