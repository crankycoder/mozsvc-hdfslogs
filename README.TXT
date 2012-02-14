You need *both* HADOOP_HOME and the HADOOP_HOME/conf paths in your
classpath.

So something like this:

    export HADOOP_HOME="/Users/victorng/dev/hadoop-0.20.203.0/"
    export CLASSPATH="./lib/*:dist/lib/mozsvc-hdfs-20120104.jar:/Users/victorng/dev/hadoop-0.20.203.0/conf"

2 tools:

1) copy_to_hdfs -i /path/to/source -o /hdfs/path/with/filename

Errors are logged to syslog with timestamp of file along with md5sum
for later processing

2) copy_from_hdfs -i /hdfs/path/with/filename -o /local/path/with/filename

Copies a file from HDFS to local filesystem.  If the local path is a
directory, just use the HDFS filename.  If the local path looks like a
proper filename, use that.



