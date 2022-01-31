#include "psort.h"
#include <omp.h>
#include <bits/stdc++.h>

//Code for sequential in-place sorting
void SequentialSort(uint32_t* arr, uint32_t n){
    if(n <= 1)
        return;
    uint32_t pivot = arr[n/2];
    uint32_t* left = arr;
    uint32_t* right = arr + n - 1;
    while(left <= right){
        while(*left < pivot)
            left++;
        while(*right > pivot)
            right--;
        if(left <= right){
            uint32_t temp = *left;
            *left = *right;
            *right = temp;
            left++;
            right--;
        }
    }
    SequentialSort(arr, right - arr + 1);
    SequentialSort(left, n - (left - arr));
}

void ParallelSort(uint32_t *data, uint32_t n, int p)
{
    // Entry point to your sorting implementation.
    // Sorted array should be present at location pointed to by data.
    
    //Recursion base case, to be handled by sequential sorting
    if(p * p > n){
        SequentialSort(data, n);
        return;
    }


    //Steps 1 and 2
    uint32_t* R = new uint32_t[p*p];
    //Obtaining p^2 pseudo-splitters
    for(uint32_t i=0;i<(n % p);i++){
        for(int j=0;j<p;j++){
            //First n%p buckets will have (n/p+1) elements
            R[i*p+j] = data[i*(n/p+1)+j];
        }
    }
    for(uint32_t i=0;i<p-n%p;i++){
        for(int j=0;j<p;j++){
            //Rest p-n%p buckets will have (n/p) elements
            R[i*p+j+(n%p)*p] = data[i*(n/p)+j+(n%p)*(n/p+1)];
        }
    }
    //Sorting pseudo splitters sequentially, since p*p << n
    SequentialSort(R, p*p);   


    //Step 3(obtaining array partitions)
    uint32_t* S = new uint32_t[p-1];
    for(int i=0;i<p-1;i++){
        S[i] = R[(p+1)*i];
    }


    //Step 4(Array partitioning)
    //First task segment calculates the number of items in each bin for future use
    uint32_t* count = new uint32_t[p]();
    uint32_t* rangecount = new uint32_t[p]();
    for(int i=0;i<p;i++){
        #pragma omp task shared(data,S,count,n,p)
        {
            for(uint64_t j=0;j<n;j++){
                if(i==0){
                    if(data[j]<=S[i]){
                        count[i]++;
                    }
                }else if(i==p-1){
                    if(data[j]>S[i-1]){
                        count[i]++;
                    }
                }else{
                    if(data[j]<=S[i] && data[j]>S[i-1]){
                        count[i]++;
                    }
                }
            }
        }
    }
    #pragma omp taskwait

    //Rangecount stores the accumulated count i.e. rangecount[i] = number of items in all bins upto and not including bin i
    for(int i=1;i<p;i++){
        rangecount[i] = rangecount[i-1] + count[i-1];
    }

    //Second task segment actually assigns the elements to their respective bins using the counts and rangecounts defined earlier
    uint32_t* B = new uint32_t[n];
    for(int i=0;i<p;i++){
        #pragma omp task shared(data,count,rangecount,B,n,p)
        {
            int local_count = 0;
            for(uint64_t j=0;j<n;j++){
                if(i==0){
                    if(data[j]<=S[i]){
                        B[rangecount[i]+local_count] = data[j];
                        local_count++;
                    }
                }else if(i==p-1){
                    if(data[j]>S[i-1]){
                        B[rangecount[i]+local_count] = data[j];
                        local_count++;
                    }
                }else{
                    if(data[j]<=S[i] && data[j]>S[i-1]){
                        B[rangecount[i]+local_count] = data[j];
                        local_count++;
                    }
                }
            }
        }
    }
    #pragma omp taskwait


    //Step 5(Recursive sorting of bins)
    for(int i=0;i<p;i++){
        #pragma omp task shared(B,rangecount,count,n,p)
        {
            if(count[i] < (2*n)/p)
                SequentialSort(B+rangecount[i],count[i]);
            else
                ParallelSort(B+rangecount[i],count[i],p);
        }
    }
    #pragma omp taskwait


    //Step 6(Concatenation of bins)
    //Here the bins are not actually concatenated but the sorted elements are copied from the bins to the original array in parallel
    for(int i=0;i<p;i++){
        #pragma omp task shared(B,rangecount,count,data)
        {
            memcpy(data+rangecount[i],B+rangecount[i],count[i]*sizeof(uint32_t));
        }
    }
    #pragma omp taskwait

    //Deallocation of auxiliary memory
    delete[] R;
    delete[] S;
    delete[] B;
    delete[] count;
    delete[] rangecount;

}
