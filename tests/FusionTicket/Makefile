EXE=FusionTicket
SCRIPTS:=$(shell /home/parallels/Downloads/dsc-cassandra-2.0.6/bin/cqlsh -f ../create_keyspace.cql)
BROKER=

all: $(EXE) $(SCRIPTS)

FusionTicket: FusionTicket.hs FusionTicketDefs.hs FusionTicketTxns.hs
	ghc $(BROKER) -threaded -rtsopts FusionTicket.hs

clean:
	rm -f *.dyn* *~ *.hi *.o $(EXE)
