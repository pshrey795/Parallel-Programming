libsort.so: psort.cpp
	g++ -fPIC -g -c -Wall psort.cpp -fopenmp
	g++ -shared -Wl,-soname,libsort.so -o libsort.so psort.o -lc

driver: libsort.so
	g++ -fopenmp -Wall psort.o driver_binary.cpp -o out

clean:
	rm *.so *.o