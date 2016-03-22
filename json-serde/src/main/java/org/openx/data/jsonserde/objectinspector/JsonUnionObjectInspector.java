package org.openx.data.jsonserde.objectinspector;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.openx.data.jsonserde.json.JSONArray;
import org.openx.data.jsonserde.json.JSONObject;

import java.util.List;

/**
 * Created by rcongiu on 8/29/15.
 */
public class JsonUnionObjectInspector implements UnionObjectInspector {
    JsonStructOIOptions options;
    private List<ObjectInspector> ois;


    public JsonUnionObjectInspector(List<ObjectInspector> ois,JsonStructOIOptions opts) {
        this.ois = ois;
        options = opts;
    }


    @Override
    public List<ObjectInspector> getObjectInspectors() {
        return ois;
    }


/*
 * This method looks at the object and finds which object inspector should be used.
 */
    @Override
    public byte getTag(Object o) {
        if(o==null) return 0;
        for(byte i =0; i< ois.size(); i ++) {
            ObjectInspector oi = ois.get(i);

            switch(oi.getCategory()) {
                case LIST: if(o instanceof JSONArray) return i; else break;
                case STRUCT:  if(o instanceof JSONObject) return i; else break;
                case MAP:  if(o instanceof JSONObject) return i; else break;
                case UNION: return i;

                case PRIMITIVE: {
                    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
                    try {
                        // try to parse it, return if able to
                        poi.getPrimitiveJavaObject(o);
                        return i;
                    } catch (Exception ex) { continue;}
                }
                default :throw new Error("Object Inspector " + oi.toString() + " Not supported for object " + o.toString());
            }
        }
        throw new Error("No suitable Object Inspector found for object  " + o.toString() + " of class " + o.getClass().getCanonicalName());
    }

    @Override
    public Object getField(Object o) {
        return o;
    }

    @Override
    public String getTypeName() {
        return ObjectInspectorUtils.getStandardUnionTypeName(this);

    }

    @Override
    public Category getCategory() {
        return Category.UNION;
    }
}
