package gash.grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc;

public class WorkerServer {
    private Server svr;
    private List<Worker> workers = new ArrayList<>();
    protected static int serverID;
    protected static int leaderID;
    protected RouteServiceGrpc.RouteServiceStub comm;

    /**
	* Configuration of the server's identity, port, and role
	*/
	private static Properties getConfiguration(final File path) throws IOException {
		if (!path.exists())
			throw new IOException("missing config file for worker server");

		Properties rtn = new Properties();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(path);
			rtn.load(fis);
            String tmp = rtn.getProperty("server.id");
            serverID = Integer.parseInt(tmp);
            String ld = rtn.getProperty("leader.id");
            leaderID = Integer.parseInt(ld);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}

		return rtn;
	}
    
    public static void main(String[] args) throws Exception {
        // TODO check args!

        String path = args[0];
        try {
            Properties conf = WorkerServer.getConfiguration(new File(path));
            RouteServer.configure(conf);
            final WorkerServer ws = new WorkerServer();
            // create 4 worker threads for each worker server
            for (int i = 0; i < 5; i++) {
                ws.workers.add(new Worker(ws));
            }
            ws.start();
            ws.blockUntilShutdown();
        } catch (IOException e) {
            System.out.println("failed to load configuration");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void start() throws Exception {
		svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteServerImpl())
				.build();

		System.out.println("-- starting worker server " + serverID + " on port " + RouteServer.getInstance().getServerPort());
		svr.start();

        for (Worker w : workers) {
            w.start();
        }

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				WorkerServer.this.stop();
			}
		});
	}

    protected void stop() {
		svr.shutdown();
	}

    private void blockUntilShutdown() throws Exception {
		/* TODO what clean up is required? */
		svr.awaitTermination();
	}
    /**
	 * server received a message!
	 * transform this to a delay response (delay processing?)
	 * responseObserver used to send results
	 * can send acknowledgement of request was accepted and another of the results of the request
	 */
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {
        // check if current server is the destination
        if (request.getDestination() == serverID) {
            // deal with the HB request
            if (request.getWorkType() == 5) {
                System.out.println("Received HB request from " + request.getOrigin());
                var HBresponse = processHB(request);
                comm.request(HBresponse, responseObserver);
            } else{
                // TODO: assign the work to a worker -> call the enqueue method

            }
        } else {
            // This is not the destination, forward the request to the next server
            comm.request(request, responseObserver);
        }
	}

    protected route.Route processHB(route.Route msg) {
        route.Route.Builder hb = route.Route.newBuilder();
        hb.setOrigin(serverID);
        hb.setDestination(leaderID);
        hb.setWorkType(6);
        // TODO: get the playload for the HB, add HB work to the HB queue?
        String payload = "hb";
        hb.setPayload(ByteString.copyFromUtf8(payload));
        return hb.build();
    }
}
