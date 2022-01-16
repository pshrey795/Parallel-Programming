CC=g++
CFLAGS=-std=c++11 -O2 -pg -fopenmp -g

sources=main.cpp classify.cpp
objects=$(sources:.cpp=.o)

classify:$(objects)
	$(CC) $(CFLAGS) $^ -o $@

run: classify
	./classify rfile dfile 1009072 4 3 > modified.txt

clean:
	rm -f $(objects)
	rm -f classify
	rm -f gmon.*
	rm -f cachegrind.out.*

gprof: 
	gprof classify gmon.out > gprof.txt

valgrind: classify 
	valgrind --tool=cachegrind ./classify rfile dfile 1009072 4 1 

%.o: %.cpp classify.h
	$(CC) $(CFLAGS) -c $<

