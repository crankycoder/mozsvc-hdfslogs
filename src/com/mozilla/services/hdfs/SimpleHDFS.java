package com.mozilla.services.hdfs;

import edu.umd.cloud9.io.JSONObjectWritable;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.io.File;
import java.util.Scanner;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URL;

/*
 XXX: Note that you MUST have HADOOP_HOME defined, and
 HADOO_HOME/conf in your classpath or else you won't
 hit the real network enabled HDFS.  The Configuration object
 will read config files from HADOOP_HOME/conf.

 Python imports below, need to get rid of all of these
 */

public class SimpleHDFS extends Configured {

    FileSystem _fs;
    Class _key_type;
    Class _value_type;

    public SimpleHDFS()
    {
        Configuration conf = new Configuration();

        URL conf_url = new URL("file:///Users/victorng/dev/hadoop-0.20.203.0/conf/hadoop-site.xml");

        conf.addResource(conf_url);
        setConf(conf);
        System.out.println("Using configuration: " + conf);
        try 
        {
            _fs = FileSystem.get(getConf());
            if ("org.apache.hadoop.fs.LocalFileSystem".equals(_fs.getClass().getName())){
                throw new RuntimeException("Failed to load DFS configuration for Hadoop");
            }
        } catch (IOException io_ex) {
            // Convert to runtime exception since there's no possible
            // recovery here
            throw new RuntimeException("Error loading the Hadoop configuration", io_ex);
        }
        _key_type = LongWritable.class;

        // The JSONObjectWritable source can be found here
        // https://github.com/lintool/Cloud9/blob/master/src/dist/edu/umd/cloud9/io/JSONObjectWritable.java
        _value_type = JSONObjectWritable.class;
    }

    public ArrayList<String> list(String path) throws IOException
    {
        ArrayList result = new ArrayList();
        Path hdfs_path = new Path(path);

        // List filenames in a path
        for (FileStatus status : _fs.listStatus(hdfs_path)) {
            result.add(status.getPath().getName());
        }
        return result;
    }

    public String next_filename(String root_path)
    {
        String DATE_FORMAT = "yyyyMMdd";
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        Calendar c1 = Calendar.getInstance(); // today
        String today = sdf.format(c1.getTime());
        long i = 0;

        Path dir_path = new Path(root_path);


        String fname = Long.toString(i) + ".log";
        Path hdfs_log_path = new Path(new File(dir_path.toString(), fname).getPath());
        try {
            safe_mkdir(dir_path);
            while (_fs.exists(hdfs_log_path)) {
                i += 1;
                fname = Long.toString(i) + ".log";
                hdfs_log_path = new Path(new File(dir_path.toString(), fname).getPath());
            }
        } catch (IOException io_ex) {
            throw new RuntimeException("Error reading HDFS", io_ex);
        }

        return hdfs_log_path.toString();
    }

    public long filesize(String path) throws IOException
    {
        Path p = new Path(path);
        FileStatus fstat = _fs.getFileStatus(p);
        return fstat.getLen();
    }

    /*
    Open a SequenceFile object for append operations.  Note that
    once the file is closed, you *cannot* append to it anymore.
    See:
       https://issues.apache.org/jira/browse/HADOOP-3977
    */
    public ISimpleHDFSFile open(String path, String mode) throws IOException
    {

        if (mode == "w") {
            return _open_write(path);
        } else if  (mode == "r") {
            return _open_read(path);
        } else {
            throw new IllegalArgumentException("File mode must be 'r' or 'w'");
        }
    }

    public ISimpleHDFSFile _open_read(String path) throws IOException
    {
        Path p = new Path(path);

        SequenceFile.Reader reader = new SequenceFile.Reader(_fs, p, getConf());
        return new SimpleReader(reader, getConf());
    }

    private void safe_mkdir(Path f) throws IOException
    {
        FileStatus fstatus = null;
        try {
            fstatus = _fs.getFileStatus(f);
            if (fstatus.isDir()) {
                return;
            }
            else {
                throw new IOException(f.toString() + " exists but " +
                        "is not a directory");
            }
        } catch(FileNotFoundException e) {
            System.out.println("Trying to create directory: ["+f.toString()+"]");
            if (!_fs.mkdirs(f)) {
                throw new IOException("failed to create " + f.toString());
            } else {
                System.out.println("Seemed to create directory: ["+f.toString()+"]");
            }
        }
    }

    public ISimpleHDFSFile _open_write(String path) throws IOException
    {
        Path p = new Path(path);
        Path dir_path= p.getParent();

        safe_mkdir(dir_path);

        SequenceFile.Writer writer = SequenceFile.createWriter(_fs, getConf(), p, _key_type, _value_type);
        return new SimpleWriter(writer, _key_type, _value_type);
    }

}


