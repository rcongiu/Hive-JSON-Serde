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
 * @author rcongiu
 */
public class JsonArrayTest {
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
        tbl.setProperty(serdeConstants.LIST_COLUMNS, "country,languages,religions");
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,array<string>,array<string>".toLowerCase());

        instance.initialize(conf, tbl);
    }


    @Test
    public void testDeSerializeEmptyArray() throws Exception {
        // Test that timestamp object can be deserialized
        Writable[] wa = new Writable[]{
                new Text("{\"country\":\"Switzerland\",\"languages\":[\"Italian\"],\"religions\":\"\"}")
        };

        for (Writable w : wa) {
            JSONObject result = (JSONObject) instance.deserialize(w);
            StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();
            StructField sfr = soi.getStructFieldRef("religions");

            assertEquals(sfr.getFieldObjectInspector().getCategory(), ObjectInspector.Category.LIST);

            ListObjectInspector loi = (ListObjectInspector) sfr.getFieldObjectInspector();
            Object val = soi.getStructFieldData(result, sfr);

            assertEquals(-1, loi.getListLength(val));
        }

    }


    @Test
    public void testDeSerializeArray() throws Exception {
        // Test that timestamp object can be deserialized
        Writable w = new Text("{\"country\":\"Switzerland\",\"languages\": [ \"Italian\" ],\"religions\": \"christian\"  }");

        JSONObject result = (JSONObject) instance.deserialize(w);
        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

        // this one is an actuall array
        StructField sflang = soi.getStructFieldRef("languages");
        // this one has a scalar, which will be promoted to a one element array
        StructField sfrel = soi.getStructFieldRef("religions");

        assertEquals(sflang.getFieldObjectInspector().getCategory(), ObjectInspector.Category.LIST);
        assertEquals(sfrel.getFieldObjectInspector().getCategory(), ObjectInspector.Category.LIST);

        ListObjectInspector loi = (ListObjectInspector) sflang.getFieldObjectInspector();
        Object val = soi.getStructFieldData(result, sflang);
        assertEquals(1, loi.getListLength(val));
        assertEquals("Italian", loi.getListElement(val, 0));

        loi = (ListObjectInspector) sfrel.getFieldObjectInspector();
        val = soi.getStructFieldData(result, sfrel);
        assertEquals(1, loi.getListLength(val));
        assertEquals("christian", loi.getListElement(val, 0));

    }


}
