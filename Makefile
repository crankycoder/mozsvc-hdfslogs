# Need to setup some environment variables for Hadoop
HADOOP_HOME := "/Users/victorng/dev/hadoop-0.20.203.0/"
CLASSPATH := "./lib/*:dist/lib/*"

all:
	ant -DJAVA_HOME=$(JAVA_HOME)

test:
	# This won't work because ant clobbers the CLASSPATH and the
	# Hadoop configuration won't properly load HDFS
	ant -DJAVA_HOME=$(JAVA_HOME) -v test

run:
	/usr/bin/env java -classpath $(CLASSPATH) com.mozilla.services.hdfs.LogReader -i fixtures/sample.json.log -hdfs_path /hdfs/var/log/metlog -output_dir /tmp/done -debug -sitexml conf/hadoop-site.xml

clean:
	ant clean

