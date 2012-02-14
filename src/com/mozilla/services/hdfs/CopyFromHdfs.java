package com.mozilla.services.hdfs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Scanner;

import org.json.JSONException;
import org.json.JSONObject;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class CopyFromHdfs
{
    /*
     * Copy a file to HDFS.  Log any errors to syslog.
     */

    @Parameter(names = "-i", arity = 1, description = "HDFS file path", required=true)
    public String input_file;

    @Parameter(names = "-o", arity = 1, description = "Local filename path", required=true)
    public String output_file;

    @Parameter(names = "--help", description = "Print help")
    public boolean help = false;

    Syslog _syslog = new Syslog();

    /*
     * Decode the JSON blob from the SequenceFile.  Note that this
     * needs to use the JSONObjectWriteable because we are storing
     * data in HDFS packed in binary
     */
    public void decode_jsonblob() throws FileNotFoundException
    {
        SimpleReader.ReaderData r_data;
        SimpleHDFS fs = new SimpleHDFS();
        ISimpleHDFSFile reader;
        

        BufferedOutputStream buf_out = new BufferedOutputStream(new FileOutputStream(new File(output_file)));

        try {
            reader = fs.open(input_file, "r");
            try {
                while (reader.hasNext()) {
                    r_data = (SimpleReader.ReaderData) reader.next();
                    System.out.println(r_data._json.toString());
                }
            } finally {
                reader.close();
            }
        } catch (IOException io_ex) {
            String msg = "HDFS IO Error.";
            _syslog.error(msg + io_ex.toString());
            throw new RuntimeException(msg, io_ex);
        }
    }


    public static void main(String[] argv)
    {
        CopyFromHdfs obj = new CopyFromHdfs();
        JCommander cmdr = new JCommander(obj, argv);

        // Dump the help screen if we have to
        if (obj.help || argv.length == 0)
        {
            cmdr.usage();
            return;
        }

        try
        {
            obj.decode_jsonblob();
        } catch (RuntimeException re_ex) {
            Writer result = new StringWriter();
            PrintWriter printWriter = new PrintWriter(result);
            re_ex.printStackTrace(printWriter);
            new Syslog().error(result.toString());
        }
    }
}
