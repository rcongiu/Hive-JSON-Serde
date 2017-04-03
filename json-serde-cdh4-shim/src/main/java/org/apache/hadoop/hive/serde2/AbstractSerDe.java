package org.apache.hadoop.hive.serde2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

public abstract class AbstractSerDe implements SerDe {
    public AbstractSerDe() {
    }

    public abstract void initialize(Configuration var1, Properties var2) throws SerDeException;

    public abstract Class<? extends Writable> getSerializedClass();

    public abstract Writable serialize(Object var1, ObjectInspector var2) throws SerDeException;

    public abstract SerDeStats getSerDeStats();

    public abstract Object deserialize(Writable var1) throws SerDeException;

    public abstract ObjectInspector getObjectInspector() throws SerDeException;
}