EXE=Rubis Rubis_strong
SCRIPTS:=$(shell /home/parallels/Downloads/dsc-cassandra-2.0.6/bin/cqlsh -f ../create_keyspace.cql)
BROKER=

all: $(EXE) $(SCRIPTS)

Rubis: Rubis.hs RubisDefs.hs RubisTxns.hs
	ghc $(BROKER) -threaded -rtsopts Rubis.hs

Rubis_strong: Rubis_strong.hs RubisDefs.hs RubisTxns.hs
	ghc $(BROKER) -threaded -rtsopts Rubis_strong.hs


clean:
	rm -f *.dyn* *~ *.hi *.o $(EXE)
