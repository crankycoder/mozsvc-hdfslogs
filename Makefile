all:
	ant -DJAVA_HOME=$(JAVA_HOME)

test:
	ant -DJAVA_HOME=$(JAVA_HOME) -v test

run:
	./runme.sh
