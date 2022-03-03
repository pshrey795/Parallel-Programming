CC=mpic++
CFLAGS=-std=c++17 -g -Wall -Wextra -O3

NP=4
DATA=data/8717_31525/edges.dat
N=8717
M=31525
RESTART_PROB=0.1
NUM_STEPS=200
NUM_WALKS=30
NUM_REC=20
SEED=369

sources=main.cpp
objects=$(sources:.cpp=.o)

compile: $(objects) librandomizer.a
	$(CC) $(CFLAGS) -o $@ $^

run: compile
	mpirun -np $(NP) ./compile $(DATA) $(N) $(M) $(RESTART_PROB) $(NUM_STEPS) $(NUM_WALKS) $(NUM_REC) $(SEED) -o output.txt

%.o: %.cpp *.hpp
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f compile $(objects)
	rm -f output.dat
