# Makefile for upscaleDB fairness test

CC = gcc
CFLAGS = -Wall -Wextra -O0 -g3 -pthread -std=gnu99
DEFINES = -D_GNU_SOURCE -DCYCLE_PER_US=2400 -DDEBUG
INCLUDES = -I. -I../hscl-archived -I../hscl-archived/locks -I../include
LIBS = -lupscaledb -lhfairlock -lpthread -lrt

# Adjust paths for upscaledb and hscl
UPS_LIBPATH = -L../src/.libs -L../hscl-archived
UPS_INCPATH = -I../include

# Source files
SOURCES = upscale_fairness_test.c
OBJECTS = $(SOURCES:.c=.o)
TARGET = upscale_fairness_test

.PHONY: all clean test debug

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $(DEFINES) -o $@ $^ $(UPS_LIBPATH) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) $(DEFINES) $(INCLUDES) $(UPS_INCPATH) -c $< -o $@

test: $(TARGET)
	./$(TARGET) 4 10 test.db 0.3 0.6

clean:
	rm -f $(OBJECTS) $(TARGET)
	rm -f test*.db test*.db-*
	rm -f *.log
