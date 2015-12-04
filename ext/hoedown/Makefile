CFLAGS = -g -O3 -ansi -pedantic -Wall -Wextra -Wno-unused-parameter -Isrc
PREFIX = /usr/local

ifneq ($(OS),Windows_NT)
	CFLAGS += -fPIC
endif

HOEDOWN_SRC=\
	src/autolink.o \
	src/buffer.o \
	src/document.o \
	src/escape.o \
	src/html.o \
	src/html_blocks.o \
	src/html_smartypants.o \
	src/stack.o \
	src/version.o

.PHONY:		all test test-pl clean

all:		libhoedown.so hoedown smartypants

# Libraries

libhoedown.so: libhoedown.so.3
	ln -f -s $^ $@

libhoedown.so.3: $(HOEDOWN_SRC)
	$(CC) -shared $^ $(LDFLAGS) -o $@

libhoedown.a: $(HOEDOWN_SRC)
	$(AR) rcs libhoedown.a $^

# Executables

hoedown: bin/hoedown.o $(HOEDOWN_SRC)
	$(CC) $^ $(LDFLAGS) -o $@

smartypants: bin/smartypants.o $(HOEDOWN_SRC)
	$(CC) $^ $(LDFLAGS) -o $@

# Perfect hashing

src/html_blocks.c: html_block_names.gperf
	gperf -L ANSI-C -N hoedown_find_block_tag -c -C -E -S 1 --ignore-case -m100 $^ > $@

# Testing

test: hoedown
	python test/runner.py

test-pl: hoedown
	perl test/MarkdownTest_1.0.3/MarkdownTest.pl \
		--script=./hoedown --testdir=test/MarkdownTest_1.0.3/Tests --tidy

# Housekeeping

clean:
	$(RM) src/*.o bin/*.o
	$(RM) libhoedown.so libhoedown.so.1 libhoedown.a
	$(RM) hoedown smartypants hoedown.exe smartypants.exe

# Installing

install:
	install -m755 -d $(DESTDIR)$(PREFIX)/lib
	install -m755 -d $(DESTDIR)$(PREFIX)/bin
	install -m755 -d $(DESTDIR)$(PREFIX)/include

	install -m644 libhoedown.* $(DESTDIR)$(PREFIX)/lib
	install -m755 hoedown $(DESTDIR)$(PREFIX)/bin
	install -m755 smartypants $(DESTDIR)$(PREFIX)/bin

	install -m755 -d $(DESTDIR)$(PREFIX)/include/hoedown
	install -m644 src/*.h $(DESTDIR)$(PREFIX)/include/hoedown

# Generic object compilations

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

src/html_blocks.o: src/html_blocks.c
	$(CC) $(CFLAGS) -Wno-static-in-inline -c -o $@ $<
