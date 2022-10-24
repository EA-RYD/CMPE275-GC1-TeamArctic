package gash.grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc;
import route.RouteServiceGrpc.RouteServiceImplBase;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public class RouteLeaderServer extends RouteServiceImplBase {

    //might need to make some of these static
    protected final static int id = 1; //server id (used in requests)
    protected static LinkedBlockingDeque<QPair> que ; // que of requests
    protected static ConcurrentHashMap<Integer,Integer> networkStatus ; //Key: ID of server, Value: Heartbeat status
    private Server svr; // actual server
    protected static RouteServiceGrpc.RouteServiceStub next; //stub to request to next server in cycle
    private static HBMonitor hb;


    /**
     * monitors work performed
     * sends heartbeat requests and accepts them
     * similar to take in that is sends requests, just doesnt depend on que for requests, does on own
     * inf loop constantly sending requests every n seconds
    */
    public static class HBMonitor extends Thread { 
    
        private boolean _isRunning = true;
		private int hbId;
        private RouteServiceGrpc.RouteServiceStub comm;
        private ConcurrentHashMap<Integer,Integer> hbMap;
        
        public HBMonitor(RouteServiceGrpc.RouteServiceStub comm, ConcurrentHashMap<Integer,Integer> hbMap) {
            this.hbId = 0;
            this.comm = comm;
            this.hbMap = hbMap;
        }

        // makes request to every worker server, every 2 sec for ever
        // could make into 4 different threads each making request if want to improve speed later
        @Override
        public void run() {
            while(_isRunning) {
                try {
                    System.out.println("Sending out Heartbeat requests...");
                    for (int i = 2; i < 6; i++) {
                        final CountDownLatch finishLatch = new CountDownLatch(1);
                        route.Route msg = constructMessage(hbId++, "null", "Send HB to Leader", 5, i, 1);
                        comm.request(msg, new StreamObserver<route.Route>() {

                            @Override
                            public void onCompleted() {
                                finishLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable arg0) {
                                finishLatch.countDown();                                
                            }

                            @Override
                            public void onNext(route.Route newMsg) {
                                System.out.println("HB reply received for server: " + newMsg.getOrigin());
                                System.out.println("HB response content: " + new String(newMsg.getPayload().toByteArray()));
                                hbMap.put((int) newMsg.getOrigin(), Integer.valueOf(new String(newMsg.getPayload().toByteArray()))); 
                            }
                            
                        }); 
                    }
                    Thread.sleep(1000); //sleeps for 1 seconds
                } catch (Exception e) {
                    System.err.println("Exception in HBMonitor:\n" + e.getMessage());
                }
                
            }
        }

        public void stopHB() {
            _isRunning = false;
        }

        public void startHB() {
            _isRunning = true;
        }
    }

    /**
     * Puts work into queue
     * determines where to send request depending on request info
    */
    public static class Put extends Thread {
        private boolean _verbose = false;
        private LinkedBlockingDeque<QPair> que;
        private ConcurrentHashMap<Integer,Integer> map;
        private QPair msg;

        public Put(LinkedBlockingDeque<QPair> que, ConcurrentHashMap<Integer,Integer> map, route.Route msg, StreamObserver<route.Route> obs) { 
            this.que = que;
            this.map = map;
            this.msg = new QPair(obs, msg);
        }

        @Override
        public void run() {
            route.Route newMsg =  msg.getRoute();
            if (verify(newMsg)) {
                //sendAcknowledgement(msg.getObserver());
                if (newMsg.getWorkType() == 5) // HEARTBEAT TYPE
                    map.put((int) newMsg.getOrigin(), Integer.valueOf(new String(newMsg.getPayload().toByteArray()))); 
                else 
                    que.offer(msg);   // destination needs to be determined later by Take thread
            } else { // Sends non verified msg back to client
                String ackStr = "Request has invalid response and has been rejected";
                route.Route ack = constructMessage(0, "null",ackStr,-1, 0, 1); //NEED TO CHANGE TYPE MAYBE
                msg.getObserver().onNext(ack);
            }
        }   

        // Message has valid type
        private boolean verify(route.Route request) {
		    return (request.getWorkType() > 0 && request.getWorkType() < 7);
	    }

        // sends and ack response back to client
        private void sendAcknowledgement(StreamObserver<route.Route> responseObserver) {
            String ackStr = "Request has been received!";
            route.Route ack = constructMessage(0, "null",ackStr,7, 0, 1); //NEED TO CHANGE TYPE IF WE USE
            responseObserver.onNext(ack);
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
        private RouteServiceGrpc.RouteServiceStub commStub;
        private ConcurrentHashMap<Integer,Integer> map;

        private LinkedBlockingDeque<QPair> que;

        public Take(LinkedBlockingDeque<QPair> que, RouteServiceGrpc.RouteServiceStub commStub, ConcurrentHashMap<Integer,Integer> map) {
            this.que = que;
            this.commStub = commStub;
            this.map = map;
        }

        @Override
        public void run() { 
            while (_isRunning && !que.isEmpty()) { //keeps going until que is empty
				try {
                    QPair qp = que.poll();
                    if (qp != null) {
                        route.Route msg = qp.getRoute();
                        if (msg.getWorkType() > 0 && msg.getWorkType() < 5) { // send work to chosen work server
                            String payloadStr = new String(msg.getPayload().toByteArray());
                            int tempServer = findLeastBusyServer();
                            route.Route newReq = constructMessage((int) msg.getId(), msg.getPath(),payloadStr, msg.getWorkType(), tempServer, (int) msg.getOrigin());
                            System.out.println("Passing request with ID: " + msg.getId() + " to worker server " + tempServer);
                            commStub.request(newReq, qp.getObserver());
                        } 
                    }
				} catch (Exception e) {
                    System.err.println("Take thread error!\n" + e);
				}
			}
			_isRunning = false;
			if (_verbose)
				System.out.println("--> Take thread is done");
        }

        // Algo for determining best server for sending work based off of heartbeat map
        private Integer findLeastBusyServer() {
            int minHB = Integer.MAX_VALUE, minHBServer = -1; 
            for (Map.Entry<Integer,Integer> entry : map.entrySet()) //random order
                minHBServer = (entry.getValue() < minHB) ? entry.getKey() : minHBServer;
            return minHBServer;
        }
        
    }

    public static void main(String[] args) throws Exception {
        String path = args[0]; // path to config file
		try {
			Properties conf = RouteLeaderServer.getConfiguration(new File(path));
			RouteServer.configure(conf);

			/* Similar to the socket, waiting for a connection */
			final RouteLeaderServer impl = new RouteLeaderServer();
            impl.setup();
			impl.start();
			impl.blockUntilShutdown(); //what does this do?

		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    private void setup() {
        que = new LinkedBlockingDeque<>();
        networkStatus = new ConcurrentHashMap<>();
        setupNetworkStatus(networkStatus);
        ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", RouteServer.getInstance().getServerDestination()).usePlaintext().build();
		next = RouteServiceGrpc.newStub(ch);
        hb = new HBMonitor(next,networkStatus);
    }

    private void setupNetworkStatus(ConcurrentHashMap<Integer,Integer> map) {
        for (int i = 2; i < 6; i++) 
            map.put(i, 0);
    }

    /**
	 * server received a message!
	 * responseObserver used to send results
	 * can send acknowledgement of request was accepted and another of the results of the request
	 */
    @Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {
        System.out.println("Received request with ID: " + request.getId());
        (new Put(que, networkStatus, request, responseObserver)).start();
        (new Take(que, next, networkStatus)).start();
    }

    private static final route.Route constructMessage(int mID, String path, String payload, int type, int destination ,int origin) {
		route.Route.Builder bld = route.Route.newBuilder();
		bld.setId(mID);
		bld.setOrigin(origin); 
		bld.setPath(path);
        bld.setWorkType(type);
        bld.setDestination(destination);
		bld.setPayload(ByteString.copyFrom(payload.getBytes()));
		return bld.build();
	}

    private void start() throws Exception {
		svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteLeaderServer())
				.build();
		System.out.println("-- starting Leader server on port " + RouteServer.getInstance().getServerPort());
		svr.start();
        hb.start(); // START OF HEARTBEAT REQUEST CYCLE (MIGHT NEED TO MOVE THIS)
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
