package com.mozilla.services.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.json.JSONObject;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import edu.umd.cloud9.io.JSONObjectWritable;

class SimpleReader implements ISimpleHDFSFile
{
    public class ReaderData
    {
        boolean _syncSeen;
        JSONObject _json;
        public ReaderData(boolean syncSeen, JSONObject json)
        {
            _syncSeen = syncSeen;
            _json = json;
        }
    }

    SequenceFile.Reader _reader;
    Configuration _conf;

    LongWritable _key;
    JSONObjectWritable _value;


    public void remove()
    {
        throw new RuntimeException("Remove is not supported");
    }

    public SimpleReader(SequenceFile.Reader reader, Configuration conf) 
    {
        _reader = reader;
        _conf = conf;

        _key = (LongWritable) ReflectionUtils.newInstance(_reader.getKeyClass(), _conf);
        _value = (JSONObjectWritable) ReflectionUtils.newInstance(_reader.getValueClass(), _conf);
    }

    public boolean hasNext()
    {
        boolean result = false;
        try
        {
            result = _reader.next(_key, _value);
        } catch (IOException ioex) {
            result = false;
        }
        return result;
    }

    public Object next()
    {
        SimpleReader.ReaderData result = null;
        boolean syncSeen = _reader.syncSeen();
        JSONObject json;
        try 
        {
            json  = _value.getJSONObject("json");
            result = new SimpleReader.ReaderData(syncSeen, json);
        } catch (Exception json_ex) {
           throw new RuntimeException("Error decoding JSON", json_ex);
        }
        return result;
    }

    public void append_obj(JSONObject json_obj)
    {
    }
    public void close()
    {
        IOUtils.closeStream(_reader);
    }
}
