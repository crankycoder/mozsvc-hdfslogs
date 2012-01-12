# Bizarrely, you need to have the hadoop configuration directory in
# your CLASSPATH or else Hadoop won't find your DFS configuration
export HADOOP_HOME="/Users/victorng/dev/hadoop-0.20.203.0/"
export CLASSPATH="./lib/*:dist/lib/*" 
/usr/bin/env java com.mozilla.services.hdfs.LogReader -i fixtures/sample.json.log -hdfs_path /hdfs/var/log/metlog -sitexml conf/hadoop-site.xml -output_dir /tmp/done
