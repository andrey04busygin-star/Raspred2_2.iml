import mpi.*;

public class Main {

    public static void blocking(int myrank, int size) throws Exception {
        int TAG = 0;

        int right = (myrank + 1) % size;
        int left = (myrank - 1 + size) % size;

        int[] buf = new int[1];

        if (myrank == 0) {
            buf[0] = myrank;

            MPI.COMM_WORLD.Send(buf, 0, 1, MPI.INT, right, TAG);
            MPI.COMM_WORLD.Recv(buf, 0, 1, MPI.INT, left, TAG);

            System.out.println("Blocking sum = " + buf[0]);

        } else {
            MPI.COMM_WORLD.Recv(buf, 0, 1, MPI.INT, left, TAG);

            System.out.println(
                    "Process " + left + " sent " + buf[0] +
                            " to process " + myrank
            );

            buf[0] += myrank;

            MPI.COMM_WORLD.Send(buf, 0, 1, MPI.INT, right, TAG);
        }
    }


    public static void nonBlocking(int myrank, int size) throws Exception {
        int TAG = 1;

        int right = (myrank + 1) % size;
        int left = (myrank - 1 + size) % size;

        int[] buf = new int[1];

        if (myrank == 0) {
            buf[0] = myrank;

            Request sendRequest = MPI.COMM_WORLD.Isend(
                    buf, 0, 1, MPI.INT, right, TAG
            );
            sendRequest.Wait();

            Request recvRequest = MPI.COMM_WORLD.Irecv(
                    buf, 0, 1, MPI.INT, left, TAG
            );
            recvRequest.Wait();

            System.out.println("Non-blocking sum = " + buf[0]);

        } else {
            Request recvRequest = MPI.COMM_WORLD.Irecv(
                    buf, 0, 1, MPI.INT, left, TAG
            );
            recvRequest.Wait();

            System.out.println(
                    "Process " + left + " sent " + buf[0] +
                            " to process " + myrank
            );

            buf[0] += myrank;

            Request sendRequest = MPI.COMM_WORLD.Isend(
                    buf, 0, 1, MPI.INT, right, TAG
            );
            sendRequest.Wait();
        }
    }


    public static void main(String[] args) throws Exception {
        MPI.Init(args);

        int myrank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        if (myrank == 0) {
            System.out.println("Blocking:");
        }

        blocking(myrank, size);

        MPI.COMM_WORLD.Barrier();

        if (myrank == 0) {
            System.out.println(" ");
            System.out.println("Non-blocking:");
        }

        MPI.COMM_WORLD.Barrier();

        nonBlocking(myrank, size);

        MPI.COMM_WORLD.Barrier();

        MPI.Finalize();
    }
}