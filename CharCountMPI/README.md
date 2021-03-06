We present an application for parallel counting characters in a text
file by using the OpenMPI implementation. In particular, given an input file
of M bytes and N processes, a worker process, with rank not equal to 0, reads a
chunk of M/N bytes and counts each character in a private data structure. The master process, with rank equal to 0, receives the partial counts from other N-1 processes within the group with the specified tag and aggregates them.

The application code shows basic primitives of MPI: i) Init and Finalize for initializing and terminating the program; ii) bcast to broadcast messages
from the master to the workers; and iii) send and recv for point-to-point communication between master and workers. To run the application, the source code must be compiled with mpijavac command and executed using the mpirun -N command,
where N is the number of processes per node on all allocated nodes.

When the program starts, only the master process is executed. After the MPI Init
primitive within the master process, N - 1 additional processes (i.e., workers) are created to reach the number of parallel processes N indicated in the mpirun command. To identify a process, MPI uses an integer ID, called rank, for each process, which is 0 for the master and is incremented each time a new process is created. In this way, the master can check the condition rank == master_rank to perform two operations: i) establish the split size of a chunk for each worker; and ii) aggregate the partial character counts received by the workers. Communication is handled by the default communicator (i.e., MPI.COMM_WORLD), which groups all the processes to enable message exchange. Then, each process, including the master, continues to run distinct versions of the program. In particular, after receiving the split size broadcast by the master, the workers read the assigned data chunk, count the occurrences of each character, and store the result in a private structure (partial counter). Finally, each worker sends the partial counter results to the master, in order to compute the final result.
