#include "classify.h"
#include <algorithm>
#include <omp.h>

Data classify(Data &D, const Ranges &R, unsigned int numt)
{ // Classify each item in D into intervals (given by R). Finally, produce in D2 data sorted by interval
   assert(numt < MAXTHREADS);
   Counter counts[R.num()]; // I need on counter per interval. Each counter can keep pre-thread subcount.

   //Optimization 1
   #pragma omp parallel num_threads(numt)
   {
      int tid = omp_get_thread_num();
      int wnd_size = (D.ndata + numt - 1) / numt;
      int lo = tid * wnd_size;
      int hi = std::min((int)D.ndata,(tid + 1) * wnd_size);
      for(int i=lo;i<hi;i++){
         int v = D.data[i].value = R.range(D.data[i].key);
         counts[v].increase(tid);
      }
   }
   
   // // First loop before optimization
   // #pragma omp parallel num_threads(numt)
   // {
   //    int tid = omp_get_thread_num(); // I am thread number tid
   //    for (int i = tid; i < D.ndata; i += numt)
   //    {                                                    // Threads together share-loop through all of Data
   //       int v = D.data[i].value = R.range(D.data[i].key); // For each data, find the interval of data's key,
   //                                                         // and store the interval id in value. D is changed.
   //       counts[v].increase(tid);                          // Found one key in interval v
   //    }
   // }
   
   // Changing definition of rangecount for optimization 2
   // Here, rangecount[r] would store the number of data items smaller than rth range(not including items of range r)
   // Accumulate all sub-counts (in each interval;'s counter) into rangecount
   unsigned int *rangecount = new unsigned int[R.num()+1];
   rangecount[0] = 0;
   for (int r = 1; r < R.num(); r++)
   { // For all intervals
      rangecount[r] = rangecount[r-1];
      for (int t = 0; t < numt; t++) // For all threads
         rangecount[r] += counts[r-1].get(t);
      // std::cout << rangecount[r] << " elements in Range " << r << "\n"; // Debugging statement
   }

   // // Accumulate all sub-counts (in each interval;'s counter) into rangecount
   // // Here, rangecount[r] would store the number of data items smaller or in the rth range
   // unsigned int *rangecount = new unsigned int[R.num()];
   // for(int r=0; r<R.num(); r++) { // For all intervals
   //    rangecount[r] = 0;
   //    for(int t=0; t<numt; t++) // For all threads
   //       rangecount[r] += counts[r].get(t);
   //    // std::cout << rangecount[r] << " elements in Range " << r << "\n"; // Debugging statement
   // }
   // // Compute prefx sum on rangecount.
   // for(int i=1; i<R.num(); i++) {
   //    rangecount[i] += rangecount[i-1];
   // }

   Data D2 = Data(D.ndata); // Make a copy

   // Optimization 2;
   #pragma omp parallel num_threads(numt)
   {
      int tid = omp_get_thread_num();
      int wnd_size = (R.num() + numt - 1) / numt;
      int lo = tid * wnd_size;
      int hi = std::min((int)R.num(),(tid + 1) * wnd_size);

      //Using local counts(optimization 3)
      int localCount[hi-lo];
      for(int i=lo;i<hi;i++)
         localCount[i-lo] = rangecount[i];
      for(int d=0;d<D.ndata;d++){
         int r = D.data[d].value;
         if(r/wnd_size == tid){             
            int newIdx = localCount[r%wnd_size]++;
            D2.data[newIdx] = D.data[d];
         }
      }

      // //Using global rangecount(to be used before optimization 2)
      // for(int d=0;d<D.ndata;d++){
      //    int r = D.data[d].value;
      //    if(r/wnd_size == tid){             
      //       int newIdx = rangecount[r]++;
      //       D2.data[newIdx] = D.data[d];
      //    }
      // }
   }
   
   // // Second loop before optimization
   // #pragma omp parallel num_threads(numt)
   // {
   //    int tid = omp_get_thread_num();
   //    for (int r = tid; r < R.num(); r += numt)
   //    { // Thread together share-loop through the intervals
   //       int rcount = 0;
   //       for (int d = 0; d < D.ndata; d++)                        // For each interval, thread loops through all of data and
   //          if (D.data[d].value == r)                             // If the data item is in this interval
   //             D2.data[rangecount[r - 1] + rcount++] = D.data[d]; // Copy it to the appropriate place in D2.
   //    }
   // }

   return D2;
}
