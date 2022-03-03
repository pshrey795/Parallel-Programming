cd ~/Assignment2/
module load compiler/gcc/9.1/openmpi/4.0.2
make clean && make compile

array=(16 8 4 2 1)

for np in "${array[@]}"
do
  make run NP=$np DATA=data/81867_545671/edges.dat N=81867 M=545671
done

for np in "${array[@]}"
do
  make run NP=$np DATA=data/82168_870161/edges.dat N=82168 M=870161
done