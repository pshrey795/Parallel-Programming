#include <iostream>
#include <fstream>
#include <stdint.h>
#include <chrono>
#include <omp.h>
#include "psort.h"
#include <bits/stdc++.h>


void check_sorted(uint32_t *data, uint32_t *data_cpy, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++)
    {
        if (data[i] != data_cpy[i])
        {
            std::cout << "Data is not sorted.\n";
            return;
        }
    }

    std::cout << "Data is sorted.\n";
    return;
}


void SortData(uint32_t *data, uint32_t n, int p, int n_threads)
{
    #pragma omp parallel num_threads(n_threads)
    {
        #pragma omp single
        ParallelSort(data, n, p);
    }
}


int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cout << "Insufficient Arguments\n"; // <datafile> <n_threads>
        return 0;
    }

    std::fstream fs(argv[1], std::ios::in | std::ios::binary);

    // Reading data
    uint32_t n, d = 0;
    int p, n_threads = atoi(argv[2]);

    fs.read((char *)&n, sizeof(n));
    fs.read((char *)&p, sizeof(p));

    uint32_t *data = new uint32_t[n];
    uint32_t *data_cpy = new uint32_t[n];

    std::cout << "n_threads = " << n_threads << std::endl;
    std::cout << "N = " << n << " p = " << p << std::endl;

    for (d = 0; d < n; d++) {
        fs.read((char *)&data[d], sizeof(uint32_t));
    }

    memcpy(data_cpy, data, n * sizeof(uint32_t));
    std::sort(data_cpy, data_cpy + n);

    fs.close();

    // Sorting
    auto begin = std::chrono::high_resolution_clock::now();
    SortData(data, n, p, n_threads);
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    double duration = (1e-6 * (std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)).count());

    check_sorted(data, data_cpy, n);
    std::cout << "Time taken for sorting " << n << " elements with "
        << p << " buckets = " << duration << "ms" << std::endl;

    // Clean-up
    delete data;

    return 0;
}