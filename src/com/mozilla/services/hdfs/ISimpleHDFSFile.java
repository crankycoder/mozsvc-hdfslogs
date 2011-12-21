package com.mozilla.services.hdfs;

import java.util.Iterator;
import org.json.JSONObject;

public interface ISimpleHDFSFile extends Iterator
{
    public void append(String json_data);

    public void close();
}
