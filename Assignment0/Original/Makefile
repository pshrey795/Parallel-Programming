CC=g++
CFLAGS=-std=c++11 -O2 -fopenmp -pg -g

sources=main.cpp classify.cpp
objects=$(sources:.cpp=.o)

classify:$(objects)
	$(CC) $(CFLAGS) $^ -o $@

run: classify
	./classify rfile dfile 1009072 4 3 > orig_output.txt

gprof: classify gmon.out
	gprof classify gmon.out > orig_gprof.txt

valgrind: classify 
	valgrind --tool=cachegrind --cachegrind-out-file=cachegrind.out.1 ./classify rfile dfile 1009072 4 1 

%.o: %.cpp classify.h
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(objects)
	rm -f classify
	rm -f gmon.*
	rm -f cachegrind.out.*
	rm -f *.txt