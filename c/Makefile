
TOP :=		$(PWD)

CFLAGS =	-m64 -std=gnu99 -I$(TOP)/include -Wall -Wextra -Werror
LIBS =		-lnvpair

dumper: dumper.o parser.o list.o custr.o strlist.o jsonemitter.o
	gcc $(CFLAGS) -o $@ $^ $(LIBS)

%.o: %.c
	gcc -c $(CFLAGS) -o $@ $^

%.o: deps/%.c
	gcc -c $(CFLAGS) -o $@ $^


clean:
	rm *.o dumper

