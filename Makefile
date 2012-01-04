all:
	ant -DJAVA_HOME=$(JAVA_HOME)

test:
	echo "This won't work properly.  See the Makefile for details"
	# This ant command *should* work, except that the classpath 
	# in the environment is actually rewritten and effectively ignored
	# by ant.  In doing so, the HADOOP_HOME/conf directory leaves
	# CLASSPATH and the HDFS DFS can't be located properly.
	#  
	# ant -DJAVA_HOME=$(JAVA_HOME) -v test
	#
	# The only way to make it work properly is to not use ant.

run:
	./runme.sh
