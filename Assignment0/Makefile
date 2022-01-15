CC=g++
CFLAGS=-std=c++11 -O2 -pg -fopenmp -g

sources=main.cpp classify.cpp
objects=$(sources:.cpp=.o)

classify:$(objects)
	$(CC) $(CFLAGS) $^ -o $@

run: classify
	./classify rfile dfile 1009072 4 3

clean:
	rm -f $(objects)
	rm -f classify
	rm -f gmon.out
	rm -f cachegrind.out.*

%.o: %.cpp classify.h
	$(CC) $(CFLAGS) -c $<

