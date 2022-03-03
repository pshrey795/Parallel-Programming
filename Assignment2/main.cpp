#include <bits/stdc++.h>
#include <mpi.h>
#include <assert.h>
#include "randomizer.hpp"

using namespace std;

#define MIN_THREADS 2

vector<vector<int>> graph;
vector<pair<int,int>> recommendations;

//Executed by every thread
void processInput(string inputFile, int n, int m){
    //Initialising data structures
    graph.resize(n);
    recommendations.resize(n);
    for(int i=0;i<n;i++){
        recommendations[i] = make_pair(0,i);
    }

    //Reading big endian input file
    ifstream file(inputFile, ios::binary | ios::in);
    if(!file.is_open()){
        cout<<"Input File Error\n";
        exit(1);
    }else{
        unsigned char buffer[8]; 
        for(int i=0;i<m;i++){
            file.read((char*)buffer, 8);
            int src = (int)buffer[0] << 24 | (int)buffer[1] << 16 | (int)buffer[2] << 8 | (int)buffer[3];
            int dest = (int)buffer[4] << 24 | (int)buffer[5] << 16 | (int)buffer[6] << 8 | (int)buffer[7];
            graph[src].push_back(dest);
        }
    }
    file.close();
}

//Every thread outputs its own processed nodes at the proper offset in the output file
void processOutput(int i, int num_rec,ofstream& file){
    //Writing to output is done in parallel which means each process writes to that node rows which it processed 
    int currentNode = i;
    int writeOffset = currentNode * (4 * (1 + 2 * num_rec));
    
    //Write outdegree
    char buffer[4];
    buffer[0] = (char)(graph[currentNode].size() >> 24 & 0xFF);
    buffer[1] = (char)(graph[currentNode].size() >> 16 & 0xFF);
    buffer[2] = (char)(graph[currentNode].size() >> 8 & 0xFF);
    buffer[3] = (char)(graph[currentNode].size() & 0xFF);
    file.seekp(writeOffset, ios::beg);
    file.write(buffer, 4);

    //Writing out the recommendations
    for(int i=0;i<num_rec;i++){
        int influenceID = recommendations[i].second;
        int influenceNum = recommendations[i].first;
        if(influenceNum>0){
            //Print recommendation ID followed by influence number
            buffer[0] = (char)(influenceID >> 24 & 0xFF);
            buffer[1] = (char)(influenceID >> 16 & 0xFF);
            buffer[2] = (char)(influenceID >> 8 & 0xFF);
            buffer[3] = (char)(influenceID & 0xFF);
            file.write(buffer, 4);
            buffer[0] = (char)(influenceNum >> 24 & 0xFF);
            buffer[1] = (char)(influenceNum >> 16 & 0xFF);
            buffer[2] = (char)(influenceNum >> 8 & 0xFF);
            buffer[3] = (char)(influenceNum & 0xFF);
            file.write(buffer, 4);
        }else{
            //Print NULL if this node cannot be recommended
            buffer[0] = 'N';
            buffer[1] = 'U';
            buffer[2] = 'L';
            buffer[3] = 'L';
            file.write(buffer, 4);
            file.write(buffer, 4);
        }
    }
}

//Custom sort function to sort the recommendations in descending order
bool customSort(pair<int,int> p1,pair<int,int> p2){
    if(p1.first > p2.first){
        return true;
    }else if(p1.first == p2.first){
        return p1.second < p2.second;
    }else{
        return false;
    }
}

//Main Random Walk
void RWR(int nodeID, int num_walks, int num_steps, Randomizer r){
    //Actual RWR
    int currentNode;
    for(int i=0;i<recommendations.size();i++){
        recommendations[i] = make_pair(0,i);
    }
    for(int j=0;j<graph[nodeID].size();j++){
        for(int k=0;k<num_walks;k++){
            int step_num = 0;
            currentNode = graph[nodeID][j];
            while(step_num < num_steps){
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

                //Updating the recommendation count
                recommendations[currentNode].first += 1;

                step_num++;
            }
        }
    }

    //Sorting nodes based on descending order of recommendations
    recommendations[nodeID].first = -1;
    for(int i=0;i<graph[nodeID].size();i++){
        recommendations[graph[nodeID][i]].first = -1;
    }
    sort(recommendations.begin(),recommendations.end(),customSort);
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
    string outputFile = "output.dat";
    ofstream file(outputFile, ios::binary | ios::out | ios::trunc);

    //To obtain the time taken by the program
    auto start = std::chrono::high_resolution_clock::now();

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
            RWR(i,num_walks,num_steps,random_generator);
            processOutput(i,num_rec,file);
        }
    }else{
        //Master-Worker scheduling
        //Message Tag 10 means the a worker is idle/ready to accept work
        //Messgae Tag 11 means the master is sending the work(or exitcode) to the worker
        if(rank==0){
            //Master(allots work to workers) 
            for(int i=0;i<num_nodes;i++){
                int worker_id;
                MPI_Recv(&worker_id, 1, MPI_INT, MPI_ANY_SOURCE, 10, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Send(&i, 1, MPI_INT, worker_id, 11, MPI_COMM_WORLD);
            }
            //End of work
            int exitCode = -1;
            for(int i=1;i<size;i++){
                int worker_id;
                MPI_Recv(&worker_id, 1, MPI_INT, MPI_ANY_SOURCE, 10, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Send(&exitCode, 1, MPI_INT, worker_id, 11, MPI_COMM_WORLD);
            }
        }else{
            //Worker(if idle, tries to take work from master)
            while(true){
                //Worker ready to take work
                int index;
                MPI_Send(&rank, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
                MPI_Recv(&index, 1, MPI_INT, 0, 11, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if(index==-1){
                    //Time to exit
                    break;
                }else{
                    RWR(index,num_walks,num_steps,random_generator);
                    processOutput(index,num_rec,file);
                }
            }
        }
    }

    MPI_Finalize();

    //Close File
    file.close();

    //End time
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end-start;
    if(rank==0){
        cout<<"Time taken by the program: "<<diff.count()<<" seconds"<<endl;
    }
    return 0;
}