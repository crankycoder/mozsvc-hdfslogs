package com.mozilla.services.hdfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import java.util.List;
import java.util.Scanner;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONException;

/*
 * This class will read in a JSON log file in one of two formats,
 *
 * 1) A series of newline delimited JSON messages, each of which will
 *    get stored into HDFS
 * 2) A list of JSON messages in a single file packed as a JSON array
 *
 * The former is easier on memory and can just be appended straight
 * into HDFS, the later is a single monolithic write.
 *
 */

public class LogReader
{
    @Parameter(names = "-jsonlist", description = "Load log as a JSON list structure")
    public boolean jsonlist = false;

    @Parameter(names = "-i", arity = 1, description = "Input JSON logfile", required=true)
    public String input_file;

    @Parameter(names = "-hdfs_path", arity = 1, description = "HDFS path to write log file to", required=true)
    public String hdfs_path;

    @Parameter(names = "-output_dir", arity = 1, description = "FS Path to move processed logs to", required=true)
    public String output_dir;

    @Parameter(names = "--help", description = "Print help")
    public boolean help = false;

    private String readFile(String path) throws IOException {
        FileInputStream stream = new FileInputStream(new File(path));
        try {
            FileChannel fc = stream.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            /* Instead of using default, pass in a decoder. */
            return Charset.defaultCharset().decode(bb).toString();
        }
        finally {
            stream.close();
        }
    }

//    /*
//     * This method will parse a JSON log file and store it into HDFS
//     */
//    public void parse_json_blob()
//    {
//
//        SimpleHDFS fs = new SimpleHDFS();
//        ISimpleHDFSFile writer;
//        Syslog _syslog = new Syslog();
//        String fname = fs.next_filename(hdfs_path);
//
//        String json_data = null;
//        JSONObject jdata = null;
//        JSONObject item = null;
//        try
//        {
//            json_data = readFile(input_file);
//            jdata = new JSONObject(new JSONTokener(json_data));
//        } catch (IOException io_ex) {
//            throw new RuntimeException("Error reading log file", io_ex);
//        } catch (JSONException json_ex) {
//            throw new RuntimeException("Error reading JSON from log file", json_ex);
//        }
//
//        try {
//            writer = fs.open(fname, "w");
//            try {
//                // The JSON blob must be a dictionary with a key of
//                // 'payload' and a value which is a list of JSON
//                // messages
//                long num_items = 0;
//                num_items = ((JSONObject)jdata.get("payload")).length();
//                for (int i = 0; i < num_items; i++)
//                {
//                    item = (JSONObject)((JSONObject)jdata.get("payload")).get(Integer.toString(i));
//                    writer.append_obj(item);
//                }
//                // Rename the log file once it has been processed
//                File f = new File(fname);
//                f.renameTo(new File(fname + ".processed"));
//            } catch (JSONException json_ex) {
//                throw new RuntimeException("Invalid JSON blob log", json_ex);
//            } finally {
//                writer.close();
//            }
//        } catch (IOException io_ex) {
//            String msg = "HDFS IO Error.";
//            _syslog.error(msg + io_ex.toString());
//            throw new RuntimeException(msg, io_ex);
//        }
//    }

    /*
     * This method will parse a JSON log file and store it into HDFS
     */
    public void parse_jsonlog()
    {
        SimpleHDFS fs = new SimpleHDFS();
        ISimpleHDFSFile writer;
        Scanner scanner;
        Syslog _syslog = new Syslog();

        String fname = fs.next_filename(hdfs_path);

        try {
            scanner  = new Scanner(new FileInputStream(input_file), "utf8");
        } catch (IOException io_ex) {
            throw new RuntimeException("Error loading the log file send to HDFS", io_ex);
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
                        System.out.println(msg);
                        _syslog.error(msg);
                    }
                }
                // Rename the log file once it has been processed
                // TODO: capture the short filename and move the
                // processed log file into the output_dir directory
                File f = new File(fname);
                f.renameTo(new File(fname + ".processed"));
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
        LogReader reader = new LogReader();
        JCommander cmdr = new JCommander(reader, argv);

        // Dump the help screen if we have to
        if (reader.help || argv.length == 0)
        {
            cmdr.usage();
            return;
        }

        try 
        {
            //if (reader.jsonlist) {
             //   // handle reading a big blob of JSON here
              //  reader.parse_json_blob();
            //} else {
                reader.parse_jsonlog();
            //}
        } catch (RuntimeException re_ex) {
            Writer result = new StringWriter();
            PrintWriter printWriter = new PrintWriter(result);
            re_ex.printStackTrace(printWriter);

            // Rename the log file if it wasn't processed properly
            File f = new File(reader.input_file);
            f.renameTo(new File(reader.input_file+ ".io_exception"));
            System.out.println(result.toString());
        }

    }
}

