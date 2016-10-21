EXE=MicroBlog
SCRIPTS:=$(shell /home/parallels/Downloads/dsc-cassandra-2.0.6/bin/./cqlsh -f ../create_keyspace.cql)
BROKER=

all: $(EXE) $(SCRIPTS)

MicroBlog: MicroBlog1.hs
	ghc $(BROKER) -rtsopts -threaded MicroBlog1.hs -o $(EXE)

clean:
	rm -f *.dyn* *~ *.hi *.o $(EXE)
