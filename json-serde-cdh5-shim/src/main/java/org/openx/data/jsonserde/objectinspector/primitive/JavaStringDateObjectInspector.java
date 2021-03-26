package org.openx.data.jsonserde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;

import java.sql.Date;

/**
 * Created by rcongiu on 11/12/15.
 */
public class JavaStringDateObjectInspector  extends AbstractPrimitiveJavaObjectInspector
        implements SettableDateObjectInspector {

    private static final ThreadLocalSimpleDateFormat sdf = new ThreadLocalSimpleDateFormat("yyyy-MM-dd");

    public JavaStringDateObjectInspector() {
        super(TypeEntryShim.dateType);
    }



    @Override
    public Object set(Object o, java.sql.Date d) {
        return d.toString();
    }

    @Override
    public Object set(Object o, DateWritable tw) {
        return create( tw.get());
    }


    @Override
    public Object create(Date d) {
        return d.toString();
    }

    @Override
    public DateWritable getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
            return new DateWritable(parse((String)o));
        } else {
            return new DateWritable((Date) o);
        }
    }

    @Override
    public Date getPrimitiveJavaObject(Object o) {
        if(o instanceof String) {
           return parse((String)o);
        } else {
            if (o instanceof Date) return (Date) o;
        }
        return null;
    }

    protected java.sql.Date parse(String o) {
        try {
            return new java.sql.Date(sdf.parse(o).getTime());
        } catch(java.text.ParseException e) {
            return null;
        }
    }

}