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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.MalformedURLException;

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
    @Parameter(names = "-i", arity = 1, description = "Input JSON logfile", required=true)
    public String input_file;

    @Parameter(names = "-hdfs_path", arity = 1, description = "HDFS path to write log file to", required=true)
    public String hdfs_path;

    @Parameter(names = "-output_dir", arity = 1, description = "FS Path to move processed logs to", required=true)
    public String output_dir;

    @Parameter(names = "--help", description = "Print help")
    public boolean help = false;

    @Parameter(names = "-debug", description = "If debug is enabled, do *not* move processed files. Default is false.")
    public boolean debug= false;

    Syslog _syslog = new Syslog();

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

    /*
     * This method will parse a JSON log file and store it into HDFS
     */
    public void parse_jsonlog()
    {
        SimpleHDFS fs = new SimpleHDFS();
        ISimpleHDFSFile writer;
        Scanner scanner;

        String fname = fs.next_filename(hdfs_path);

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
                finished_processing(input_file);

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

    private boolean check_dstdir()
    {
        if (this.debug) {
            new Syslog().error("Skipping directory check in DEBUG mode");
            return true;
        } else {
            File dst_dir = new File(this.output_dir);
            return dst_dir.exists() && dst_dir.isDirectory();
        }
    }

    public void io_error_processing(String fname)
    {
        mv_file(fname, "io_error");
    }

    public void finished_processing(String fname)
    {
        mv_file(fname, null);
    }

    private void mv_file(String fname, String extension)
    {
        String short_name;
        String src_path = new File(fname).getParentFile().getPath();

        if (extension != null) {
            short_name  = new File(fname).getName() + "." + extension;
        } else {
            short_name  = new File(fname).getName();
        }

        File dst_dir = new File(this.output_dir);
        File dst_file = new File(dst_dir, short_name);

        if (this.debug) {
            new Syslog().error("Skipping file move in DEBUG mode for ["+fname+"]");
            return;
        }

        File f = new File(fname);
        if (!f.renameTo(dst_file)) {
            try
            {
                copyFile(f, dst_file);
                f.delete();
            } catch (IOException io_ex) {
                throw new RuntimeException("Can't move file: ["+fname+"] to ["+dst_file+"]", io_ex);
            }
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

        //check_site_xml(reader);

        if (!reader.check_dstdir()) {
            return;
        }

        try
        {
            reader.parse_jsonlog();
        } catch (RuntimeException re_ex) {
            Writer result = new StringWriter();
            PrintWriter printWriter = new PrintWriter(result);
            re_ex.printStackTrace(printWriter);
            reader.io_error_processing(reader.input_file);

            new Syslog().error(result.toString());
        }

    }

    public static void copyFile(File sourceFile, File destFile) throws IOException {
        if(!destFile.exists()) {
            destFile.createNewFile();
        }

        FileChannel source = null;
        FileChannel destination = null;
        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();

            // previous code: destination.transferFrom(source, 0, source.size());
            // to avoid infinite loops, should be:
            long count = 0;
            long size = source.size();
            while((count += destination.transferFrom(source, 0, size-count))<size);
        }
        finally {
            if(source != null) {
                source.close();
            }
            if(destination != null) {
                destination.close();
            }
        }
    }

}

