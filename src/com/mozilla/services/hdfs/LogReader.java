package com.mozilla.services.hdfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.FileInputStream;
import java.io.IOException;

import java.util.List;
import java.util.Scanner;

import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.File;
import java.io.Writer;

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

    @Parameter(names = "--help", description = "Print help")
    public boolean help = false;

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
                    writer.append(scanner.nextLine());
                }

                // Rename the log file once it has been processed
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
            if (reader.jsonlist) {
                // TODO: handle reading a big blob of JSON here
            } else {
                reader.parse_jsonlog();
            }
        } catch (RuntimeException re_ex) {
            Writer result = new StringWriter();
            PrintWriter printWriter = new PrintWriter(result);
            re_ex.printStackTrace(printWriter);

            // Rename the log file if it wasn't processed properly
            File f = new File(reader.input_file);
            f.renameTo(new File(reader.input_file+ ".io_exception"));
        }

    }
}

