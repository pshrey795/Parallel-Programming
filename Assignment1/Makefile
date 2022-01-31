libsort.so: psort.cpp psort.h
	g++ -fPIC -g -c -Wall psort.cpp -fopenmp
	g++ -shared -Wl,-soname,libsort.so -o libsort.so psort.o

driver: libsort.so
	g++ -fopenmp -Wall psort.o driver_binary.cpp -o out

clean:
	rm -f *.so *.o *.bin
	rm -f out
