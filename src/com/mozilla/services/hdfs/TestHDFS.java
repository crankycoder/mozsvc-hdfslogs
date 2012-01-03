package com.mozilla.services.hdfs;

import java.io.*;
import org.junit.*;
import java.util.*;
import org.json.JSONObject;

/*
 * You'll need to have your Hadoop instance running
 *
 */

public class TestHDFS 
{

    public static void main(String[] argv) throws Throwable
    {
        Random randomGenerator = new Random();
        int record_count = 0;
        ISimpleHDFSFile writer;
        ISimpleHDFSFile reader;
        SimpleHDFS fs = new SimpleHDFS();

        String fname = fs.next_filename("/tmp/var/lock/"+ randomGenerator.nextInt(99999));
        System.out.println("Using: " + fname);

        Scanner scanner;

        try {
            scanner  = new Scanner(new FileInputStream("fixtures/sample.json.log"), "utf8");
        } catch (IOException io_ex) {
            throw new RuntimeException("Error loading the log file send to HDFS", io_ex);
        }

        try {
            writer = fs.open(fname, "w");
            try {
                record_count = 0;
                while (scanner.hasNextLine()) {
                    JSONObject j_obj = new JSONObject(scanner.nextLine());
                    writer.append_obj(j_obj);
                    record_count += 1;
                }
            } finally {
                writer.close();
            }
        }
        finally{
            scanner.close();
        }
        assert record_count == 2000;

        SimpleReader.ReaderData r_data;
        reader = fs.open(fname, "r");
        try
        {
            record_count = 0;
            while (reader.hasNext())
            {
                r_data = (SimpleReader.ReaderData) reader.next();
                record_count += 1;
            }
        } finally {
            reader.close();
        }
        assert record_count == 2000;
        System.out.println("Record_count : " + record_count);

        Syslog logger = new Syslog();
        logger.error("This is a test from java");
        logger.closelog();
        // TODO: test the syslog
    }



}
