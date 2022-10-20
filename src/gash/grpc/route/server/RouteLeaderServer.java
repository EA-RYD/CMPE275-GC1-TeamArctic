package gash.grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc.RouteServiceImplBase;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class RouteLeaderServer extends RouteServiceImplBase {
    /**
     * monitors work performed
     * sends heartbeat requests and accepts them
     * similar to take in that is sends requests, just doesnt depend on que for requests, does on own
     * (maybe dont need if take does similar thing)
    */
    public static class HBMonitor extends Thread { 
        // TODO

    }

    /**
     * Puts work into queue
     * determines where to send request depending on request info
    */
    public static class Put extends Thread {
        private boolean _verbose = false;
        private boolean _isRunning = true;
        private RouteServiceImplBase _BaseServer;
        private route.Route request;

        public Put(RouteServiceImplBase comm, route.Route msg) { 
            // TODO 
        }

        public void shutdown() { 
              // TODO 
        }

        @Override
        public void run() {
              // TODO 
              // get from client and maybe periodic hb goes in que but prob not
              
        }   
    }

    /**
     * takes out work from queue,
     * sends forward request that is passed until
     * receiving server is reached
    */
    public static class Take extends Thread {
        private boolean _verbose = false;
        private boolean _isRunning = true;
        private RouteServiceImplBase _BaseServer;
        private RouteServiceImplBase _ReceivingServer;

        // server's internal queue (might not need in our case)
        private LinkedBlockingDeque<route.Route> que;

        public Take(RouteServerImpl base, RouteServerImpl destination) {
              // TODO 
        }
        
        public void shutdown() { 
              // TODO 
        }

        @Override
        public void run() { 
              // TODO 
              // acts on requests from que, maybe 

        }
        
    }

    protected static final int serverID = 1; // should come from conf file eventually
    protected LinkedBlockingDeque<route.Route> que; 
    protected ConcurrentHashMap<Integer,HeartBeatServer> networkStatus;
    protected static int heartbeatsUpdated = 0; //needs to be 4 for leader to distribute a server

    private Server svr;
    // maybe there should we worker threads that take and put from que?

    public static void main(String[] args) throws Exception {
        String path = args[0]; // path to config file
		try {
			Properties conf = RouteLeaderServer.getConfiguration(new File(path));
			RouteServer.configure(conf);

			/* Similar to the socket, waiting for a connection */
			final RouteLeaderServer impl = new RouteLeaderServer();
			impl.start();
			impl.blockUntilShutdown();

		} catch (IOException e) {
			// TODO better error message
			e.printStackTrace();
		}
    }

    public void systemCheck() { //sends out requests for heartbeats
        // TODO
    }

    protected ByteString process(route.Route msg) { //process route object
		// TODO placeholder
		String content = new String(msg.getPayload().toByteArray());
		System.out.println("-- got: " + msg.getOrigin() + ", path: " + msg.getPath() + ", with: " + content);

		// TODO complete processing
		final String blank = "blank";
		byte[] raw = blank.getBytes();

		return ByteString.copyFrom(raw);
	}

    private void sendAcknowledgement(StreamObserver<route.Route> responseObserver) {
        // TODO
        
    }

    /**
	 * server received a message!
	 * transform this to a delay response (delay processing?)
	 * responseObserver used to send results
	 * can send acknowledgement of request was accepted and another of the results of the request
	 */
    @Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {
        // TODO
    }

    private static final route.Route constructMessage(int mID, String path, String payload, String type, int destination) {
		route.Route.Builder bld = route.Route.newBuilder();
		bld.setId(mID);
		bld.setOrigin(RouteLeaderServer.serverID);
		bld.setPath(path);
        bld.setType(type);
        bld.setDestination(destination);
		bld.setPayload(ByteString.copyFrom(payload.getBytes();));
		return bld.build();
	}

    // Message has valid type
    private boolean verify(route.Route request) {
        String[] validTypes = {"HeartBeat","Work"};
		return Arrays.asList(validTypes).contains(request.getType());
	}

    private void start() throws Exception {
		svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteLeaderServer())
				.build();

		System.out.println("-- starting Leader server");
		svr.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				RouteLeaderServer.this.stop();
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
	* Configuration of the server's identity, port, and role
    configure server communication setup, maybe hashmap setup goes here?

	*/
	private static Properties getConfiguration(final File path) throws IOException {
		if (!path.exists())
			throw new IOException("missing file");

		Properties rtn = new Properties();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(path);
			rtn.load(fis);
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
}
