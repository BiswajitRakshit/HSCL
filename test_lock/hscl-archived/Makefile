#Use machine's CPU-SPEED, else results will be wrong.
#example CYCLE_PER_US=2400L - 2.4 GHz processor

CYCLE_PER_US=2400L
ifndef CYCLE_PER_US
$(error CYCLE_PER_US not set. Set CYCLE_PER_US according to your machine CPU-SPEED)
endif

CC = gcc
GCCVERSIONGTEQ5 := $(shell expr `gcc -dumpversion | cut -f1 -d.` \>= 5)

# u-SCL is not tested with -O3 flag for gcc version above 5 yet.
ifeq "$(GCCVERSIONGTEQ5)" "1"
    OFLAG=-O0
else
    OFLAG=-O0
endif

FLAGS=-g -lpthread -Wall ${OFLAG} -DCYCLE_PER_US=${CYCLE_PER_US} -fno-omit-frame-pointer
TESTFLAG=-Ilocks/

AUXFILES=utils.o
DPDFLAG=-g ${TESTFLAG}
# AUXFLAGS=-lpthread -I./locks

%.o:locks/%.c
	gcc $< -c ${DPDFLAG} ${WAITINGPOLICY} ${FLAGS} $(DEPTHFLAG) $(SLICETIME)

hfairlock: main.c lock.h hfairlock.o
	gcc main.c hfairlock.o -o hfairlock ${FLAGS} -DHFAIRLOCK ${TESTFLAG} $(DEPTHFLAG)

spin:
	gcc main.c -o spin ${FLAGS} -DSPIN

mutex:
	gcc main.c -o mutex ${FLAGS} -DMUTEX

slice:
	gcc main.c hfairlock.o -o hfairlock ${FLAGS} -DHFAIRLOCK -DPRINT ${TESTFLAG} $(DEPTHFLAG) $(SLICETIME)

clean:
	rm -f *.o locks/*.o hfairlock mutex spin

