package com.mozilla.services.hdfs;

import org.apache.hadoop.io.SequenceFile;
import edu.umd.cloud9.io.JSONObjectWritable;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.io.File;
import java.io.IOException;
import java.lang.InstantiationException;
import java.lang.IllegalAccessException;

public class SimpleWriter implements ISimpleHDFSFile
{
    /*
     * This is a wrapper around the sequence writer.
     */

    SequenceFile.Writer _sequence_writer;
    LongWritable _key;
    JSONObjectWritable _value;
    long _count;

    public SimpleWriter(SequenceFile.Writer sequence_writer, 
            Class key_class, 
            Class value_class)
    {
        _sequence_writer = sequence_writer;
        try
        {
            _key = (LongWritable) key_class.newInstance();
            _value = (JSONObjectWritable) value_class.newInstance();
        } catch (InstantiationException inst_ex) {
            throw new RuntimeException("Error instantiating key/value pair.", inst_ex);
        } catch (IllegalAccessException ill_ex) {
            throw new RuntimeException("Error instantiating key/value pair: ", ill_ex);
        }
        _count = 0;
    }

    public void append(String json_data)
    {
        _key.set(_count);
        _value.clear();
        JSONObject jdata;
        try {
            jdata = new JSONObject(new JSONTokener(json_data));
            _value.put("json", jdata);
            _sequence_writer.append(_key, _value);
            _count += 1;
        } catch (JSONException json_ex) {
            // TODO: Log the error somewhere, but just skip the record
            // if it's not really JSON
            throw new RuntimeException("Invalid JSON data read in", json_ex);
        } catch (IOException io_ex) {
            // TODO: Log the error somewhere, but just skip the record
            // if we can't write to HDFS
            throw new RuntimeException("HDFS IO error", io_ex);
        }
    }

    public long getLength()
    {
        try
        {
            return _sequence_writer.getLength();
        } catch (IOException io_ex) {
            return -1;
        }
    }

    public void sync()
    {
        // Flush the writer to disk
        try {
            _sequence_writer.sync();
        } catch (IOException io_ex) {
            // TODO: log this somewhere, but we can probably just skip
            // this
        }

    }


    public void close()
    {
        sync();
        try {
            _sequence_writer.close();
        } catch (IOException io_ex) {
            throw new RuntimeException("Error closing HDFS file", io_ex);
        }
    }

    public Object next()
    {
        return null;
    }

    public void remove()
    {
        // TODO:  raise an error
    }

    public boolean hasNext()
    {
        return false;
    }


}
