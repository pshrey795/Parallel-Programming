#include <bits/stdc++.h>
#include <mpi.h>
#include <assert.h>
#include "randomizer.hpp"

using namespace std;

#define MIN_THREADS 16

vector<vector<int>> graph;
vector<vector<pair<int,int>>> recommendations;
vector<int> processedNodes; 

//Executed by every thread
void processInput(string inputFile, int n, int m){
    //Initialising data structures
    graph.resize(n);
    recommendations.resize(n);
    for(int i=0;i<n;i++){
        for(int j=0;j<n;j++){
            recommendations[i].push_back(make_pair(0,j));
        }
    }

    //Reading big endian input file
    ifstream file(inputFile, ios::binary);
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
        delete[] buffer;
    }
    file.close();
}

//Called by master thread to initialise the output file so that the indexed writing has no issues
void initialiseOutput(int n, int r, string outputFile){
    //Create a file and write to it
    ofstream file(outputFile, ios::binary);
    if(!file.is_open()){
        cout<<"Output File Error\n";
        exit(1);
    }else{
        int num = n * (1 + 2*r);
        unsigned char buffer[num];
        for(int i=0;i<num;i++){
            buffer[i] = 0;
        }
        file.write((char*)buffer, num);
    }
    file.close();
}

//Every thread outputs its own processed nodes at the proper offset in the output file
void processOutput(int num_nodes,int num_rec,string outputFile){
    //Writing to output is done in parallel which means each process writes to that node rows which it processed 
    ofstream file(outputFile, ios::binary);
    for(int i=0;i<processedNodes.size();i++){
        int currentNode = processedNodes[i];
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
            int influenceID = recommendations[currentNode][i].second;
            int influenceNum = recommendations[currentNode][i].first;
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

        delete[] buffer;
    }
    file.close();
}

//Custom binary search to decide if a particular node j is a child of node i
bool search(int i,int j){
    int low = 0;
    int high = graph[i].size() - 1;
    while(low <= high){
        int mid = (low + high) / 2;
        if(graph[i][mid] == j){
            return true;
        }else if(graph[i][mid] < j){
            low = mid + 1;
        }else{
            high = mid - 1;
        }
    }
    return false;
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
void RWR(int nodeID, int num_walks, int num_steps, int num_rec, Randomizer r){
    //Actual RWR
    int currentNode;
    for(int j=0;j<graph[nodeID].size();j++){
        for(int k=0;k<num_walks;k++){
            int step_num = 0;
            currentNode = graph[nodeID][j];
            while(step_num < num_steps){
                //Updating the recommendation count
                recommendations[nodeID][currentNode].first++;

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

    //Sorting nodes based on descending order of recommendations
    for(int i=0;i<recommendations[nodeID].size();i++){
        if(i == nodeID || search(nodeID,i)){
            recommendations[nodeID][i].first = -1;
        }
    }
    sort(recommendations[nodeID].begin(),recommendations[nodeID].end(),customSort);
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
                    RWR(index,num_walks,num_steps,num_rec,random_generator);
                }
            }
        }
    }

    string outputFile = "output.dat";

    //Handle Output
    processOutput(num_nodes,num_rec,outputFile);

    MPI_Finalize();
}