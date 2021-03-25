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
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONObject;

import java.util.Properties;

import static org.junit.Assert.*;

/**
 * @author dblock
 */
public class JsonCrLfTest {
    static JsonSerDe instance;

    @Before
    public void setUp() throws Exception {
        initialize();
    }

    static public void initialize() throws Exception {
        instance = new JsonSerDe();
        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(serdeConstants.LIST_COLUMNS, "text");
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string".toLowerCase());
        instance.initialize(conf, tbl);
    }

    @Test
    public void testDeSerializeStringWithLf() throws Exception {
        Writable w = new Text("{\"text\":\"hello\\nworld\"}");

        JSONObject result = (JSONObject) instance.deserialize(w);

        assertEquals("hello\nworld", result.getString("text"));

        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
        StructField sfText = soi.getStructFieldRef("text");
        assertEquals(sfText.getFieldObjectInspector().getCategory(), ObjectInspector.Category.PRIMITIVE);
    }

    @Test
    public void testDeSerializeStringWithCrLf() throws Exception {
        Writable w = new Text("{\"text\":\"hello\\n\\rworld\"}");

        JSONObject result = (JSONObject) instance.deserialize(w);

        assertEquals("hello\n\rworld", result.getString("text"));

        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
        StructField sfText = soi.getStructFieldRef("text");
        assertEquals(sfText.getFieldObjectInspector().getCategory(), ObjectInspector.Category.PRIMITIVE);
    }

    @Test
    public void testDeSerializeStringWithExtraLf() throws Exception {
        Writable w = new Text("{\"text\":\"hello\\n\\rworld\"}\n");

        JSONObject result = (JSONObject) instance.deserialize(w);

        assertEquals("hello\n\rworld", result.getString("text"));

        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
        StructField sfText = soi.getStructFieldRef("text");
        assertEquals(sfText.getFieldObjectInspector().getCategory(), ObjectInspector.Category.PRIMITIVE);
    }
}
