#include <bits/stdc++.h>
#include <mpi.h>
#include <assert.h>
#include "randomizer.hpp"

using namespace std;

#define MIN_THREADS 16

vector<vector<int>> graph;
vector<vector<int>> recommendations;
vector<int> processedNodes; 

//Executed by every thread
void processInput(string inputFile, int n, int m){
    //Initialising data structures
    graph.resize(n);
    recommendations.resize(n);
    for(int i=0;i<n;i++){
        recommendations[i] = vector<int>(n,0);
    }

    //Reading big endian input file
    ifstream file(inputFile, ios::binary);
    if(!file.is_open()){
        cout<<"Input File Error\n";
        exit(1);
    }else{
        
    }
}

//Every thread outputs its own processed nodes at the proper offset in the output file
void processOutput(int num_nodes,int num_rec,string outputFile){

}

void RWR(int nodeID, int num_walks, int num_steps, int num_rec, Randomizer r){
    int currentNode;
    for(int j=0;j<graph[nodeID].size();j++){
        for(int k=0;k<num_walks;k++){
            int step_num = 0;
            currentNode = graph[nodeID][j];
            while(step_num < num_steps){
                //Updating the recommendation count
                recommendations[nodeID][currentNode]++;

                //Moving to next step
                int num_child = graph[currentNode].size();
                if(num_child > 0){
                    //Children exist for the given node
                    int next_step = r.get_random_value(nodeID);
                    if(next_step<0){
                        //Restart 
                        currentNode = graph[nodeID][j];
                    }else{
                        //Move onto the next child
                        currentNode = graph[currentNode][next_step % num_child];
                    }
                }else{
                    //Restart
                    currentNode = graph[nodeID][j];
                }
                step_num++;
            }
        }
    }
    processedNodes.push_back(nodeID);
}

int main(int argc, char* argv[]){
    assert(argc > 8);
    std::string graph_file = argv[1];
    int num_nodes = std::stoi(argv[2]);
    int num_edges = std::stoi(argv[3]);
    float restart_prob = std::stof(argv[4]);
    int num_steps = std::stoi(argv[5]);
    int num_walks = std::stoi(argv[6]);
    int num_rec = std::stoi(argv[7]);
    int seed = std::stoi(argv[8]);
    
    //Only one randomizer object should be used per MPI rank, and all should have same seed
    Randomizer random_generator(seed, num_nodes, restart_prob);
    int rank, size;

    //Starting MPI pipeline
    MPI_Init(NULL, NULL);
    
    // Extracting Rank and Processor Count
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    //Handle Input
    processInput(graph_file,num_nodes,num_edges);

    //Random Walk with Restart
    if(size <= MIN_THREADS){
        //Parallel Random Walk with Restart(equal division of work)
        int num_nodes_per_thread = (num_nodes+size-1)/size;
        int start_node = rank*num_nodes_per_thread;
        int end_node = min((rank+1)*num_nodes_per_thread,num_nodes);
        for(int i=start_node;i<end_node;i++){
            RWR(i,num_walks,num_steps,num_rec,random_generator);
        }
    }else{
        //Master-Slave scheduling 
        if(rank==0){
            //Master(allots work to slaves) 
        }else{
            //Slave(if idle, tries to take work from master)
        }
    }

    //Handle Output
    string outputFile;
    processOutput(num_nodes,num_rec,outputFile);

    MPI_Finalize();
}