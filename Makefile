# Need to setup some environment variables for Hadoop
HADOOP_HOME := "/Users/victorng/dev/hadoop-0.20.203.0/"
CLASSPATH := "./lib/*:dist/lib/*:/Users/victorng/dev/hadoop-0.20.203.0/conf"

all:
	ant -DJAVA_HOME=$(JAVA_HOME)

test:
	ant -DJAVA_HOME=$(JAVA_HOME) -v test

run:
	/usr/bin/env java -classpath $(CLASSPATH) com.mozilla.services.hdfs.LogReader -i fixtures/sample.json.log -hdfs_path /hdfs/var/log/metlog -output_dir /tmp/done -move
