package com.mozilla.services.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Scanner;

import org.json.JSONException;
import org.json.JSONObject;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class CopyToHdfs
{
    /*
     * Copy a file to HDFS.  Log any errors to syslog.
     */

    @Parameter(names = "-i", arity = 1, description = "Local filesystem path", required=true)
    public String input_file;

    @Parameter(names = "-o", arity = 1, description = "HDFS directory path", required=true)
    public String output_dir;

    @Parameter(names = "--help", description = "Print help")
    public boolean help = false;

    Syslog _syslog = new Syslog();

    /*
     * This method will parse a JSON log file and push it into HDFS
     * with a datetime stamped filename
     */
    public void copy_jsonlog()
    {
        SimpleHDFS fs = new SimpleHDFS();
        ISimpleHDFSFile writer;
        Scanner scanner;


        String fname = fs.next_filename(output_dir);

        try {
            scanner  = new Scanner(new FileInputStream(input_file), "utf8");
        } catch (IOException io_ex) {
            throw new RuntimeException("Error opening the log file send to HDFS", io_ex);
        }

        try {
            writer = fs.open(fname, "w");
            try {
                while (scanner.hasNextLine()) {
                    try
                    {
                        JSONObject j_obj = new JSONObject(scanner.nextLine());
                        writer.append_obj(j_obj);
                    } catch (JSONException json_ex) {
                        String msg = "Error parsing JSON from log: " + json_ex.toString();
                        _syslog.error(msg);
                    }
                }
            } finally {
                writer.close();
            }
        } catch (IOException io_ex) {
            String msg = "HDFS IO Error.";
            _syslog.error(msg + io_ex.toString());
            throw new RuntimeException(msg, io_ex);
        }
        finally{
            scanner.close();
        }
    }

    public static void main(String[] argv)
    {
        CopyToHdfs obj = new CopyToHdfs();
        JCommander cmdr = new JCommander(obj, argv);

        // Dump the help screen if we have to
        if (obj.help || argv.length == 0)
        {
            cmdr.usage();
            return;
        }

        try
        {
            obj.copy_jsonlog();
        } catch (RuntimeException re_ex) {
            Writer result = new StringWriter();
            PrintWriter printWriter = new PrintWriter(result);
            re_ex.printStackTrace(printWriter);
            new Syslog().error(result.toString());
        }
    }
}
