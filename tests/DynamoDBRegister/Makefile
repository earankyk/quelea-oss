EXE=DynamoRegister
SCRIPTS:=$(shell cqlsh -f ../create_keyspace.cql)
BROKER=

all: $(EXE) $(SCRIPTS)

DynamoRegister: Register1.hs
	ghc $(BROKER) -threaded Register1.hs

clean:
	rm -f *.dyn* *~ *.hi *.o $(EXE)
