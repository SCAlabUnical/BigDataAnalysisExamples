import mpi.Comm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Status;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Counter {
    static public void main(String[] args) throws MPIException, IOException {

        int tag = 42;

        // Initialize the message array for workers and result array for master
        int[] partial_counter = new int[26];
        int[] res = new int[26];
        int[] split_size = new int[1];

        String fileName = "test.txt";

        MPI.Init(args);  /* Initialize MPI */
        Comm comm = MPI.COMM_WORLD;

        int rank = comm.getRank();  /* Get id of this process */
        int master_rank = 0;
        int ntasks = comm.getSize();  /* Get nr of processes */


        if (rank == master_rank) {
            // The master establishes the number of bytes of input file and computes the split size
            Path path = Paths.get(fileName);
            long bytes = Files.size(path);
            split_size[0] = (int) (bytes / ntasks);
        }
        // The split size is broadcast to all worker nodes
        comm.bcast(split_size, split_size.length, MPI.INT, 0);
        System.out.printf("Process %d received %d from process 0\n", rank, split_size[0]);
//            Status status = comm.recv(split_size, split_size.length, MPI.INT, MPI.ANY_SOURCE, tag);

        int size = split_size[0];
        byte[] readBytes = new byte[size];
        try (InputStream inputStream = new FileInputStream(fileName)) {
            // Each worker node determines the chunk in which to work
            int start = (rank - 1) * size;
            inputStream.skip(start);
            inputStream.read(readBytes, 0, size);
            // Count and store all the characters of the chunk
            for (byte b : readBytes) {
                char c = (char) b;
                if (Character.isLetter(c)) {
                    int index = (int) Character.toLowerCase(c) - (int) 'a';
                    partial_counter[index] += 1;
                }
            }
            // Each worker node comunicates the partial counter to the master
            comm.send(partial_counter, partial_counter.length, MPI.INT,
                    master_rank, tag);
        }
        // The master aggregates the partial results
        if (rank == master_rank) {
            for (int i = 1; i < ntasks; i++) {
                Status status = comm.recv(partial_counter, partial_counter.length, MPI.INT, MPI.ANY_SOURCE, tag);

                for (int j = 0; j < partial_counter.length; j++)
                    res[j] += partial_counter[j];

            }
            for (int j = 0; j < res.length; j++)
                System.out.println(res[j]);
        }
        MPI.Finalize();
    }
}



