import matplotlib.pyplot as plt

threads = [1,2,4,8,12,16]
times = []
# take the threads, times value input from the file "output1.txt"
with open("Outputs/ne8_p40.txt", "r") as f:
    for line in f:
        times.append(float(line))

# plot threads in x axis and times in y axis

plt.plot(threads, times, label="n=10^8, p=40")
plt.xlabel('Number of CPUs')
plt.ylabel('Execution Time(ms)')
plt.title('Execution Time vs Number of CPUs')
plt.legend()

plt.show()
plt.savefig('plot.png')
