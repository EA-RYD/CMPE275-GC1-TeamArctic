package gash.grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.xds.shaded.io.envoyproxy.envoy.api.v2.route.Route;
import route.RouteServiceGrpc;
import route.RouteServiceGrpc.RouteServiceImplBase;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class RouteLeaderServer extends RouteServiceImplBase {
    /**
     * monitors work performed
     * sends heartbeat requests and accepts them
     * similar to take in that is sends requests, just doesnt depend on que for requests, does on own
     * inf loop constantly sending requests every n seconds
    */
    public static class HBMonitor extends Thread { 
    
        private boolean _isRunning = true;
		private int workId;
        private RouteServiceGrpc.RouteServiceStub comm;
        private ArrayList<Integer> workerServers;

        
        public HBMonitor(RouteServiceGrpc.RouteServiceStub comm) {
            this.workId = 0;
            this.comm = comm;
            workerServers = new ArrayList<>();
            // TODO CHANGE THESE TO VALID PORTS
            workerServers.add(1);
            workerServers.add(2);
            workerServers.add(3);
            workerServers.add(4);
        }

        // makes request to every worker server, every 2 sec for ever
        @Override
        public void run() {
            while(_isRunning) {
                try {
                    for (Integer port : workerServers) {
                        route.Route msg = constructMessage(workId++, "null", "Send HB to Leader", "HeartBeat", port);
                        comm.request(msg, null);
                    }
                    Thread.sleep(3000); //sleeps for 3 seconds
                } catch (Exception e) {
                    System.err.println("Exception in HBMonitor:\n" + e.getMessage());
                }
                
            }
        }

        public void stopHB() {
            _isRunning = false;
        }
    }

    /**
     * Puts work into queue
     * determines where to send request depending on request info
    */
    public static class Put extends Thread {
        private boolean _verbose = false;
        private LinkedBlockingDeque<QPair> que;
        private ConcurrentHashMap<Integer,HeartBeatServer> map;
        private QPair msg;

        public Put(LinkedBlockingDeque<QPair> que, ConcurrentHashMap<Integer,HeartBeatServer> map, route.Route msg, StreamObserver<route.Route> obs) { 
            this.que = que;
            this.map = map;
            this.msg = new QPair(obs, msg);
        }

        @Override
        public void run() {
            route.Route newMsg =  msg.getRoute();
            if (verify(newMsg)) {
                sendAcknowledgement(msg.getObserver());
                if (newMsg.getType().equals("HeartBeat"))  // TODO iterate heartbeat detector so that heartbeats arrived increases 
                    map.put((int) newMsg.getOrigin(), new HeartBeatServer(new String(newMsg.getPayload().toByteArray()))); 
                else 
                    que.offer(msg);   // destination needs to be determined later by Take thread
            }
        }   

        // Message has valid type
        private boolean verify(route.Route request) {
            String[] validTypes = {"HeartBeat","Work"};
		    return Arrays.asList(validTypes).contains(request.getType());
	    }

        // sends and ack response back to client
        private void sendAcknowledgement(StreamObserver<route.Route> responseObserver) {
            String ackStr = "Request has been received!";
            route.Route ack = constructMessage(0, "null",ackStr,"Acknowledgement", 0);
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
        private ConcurrentHashMap<Integer,HeartBeatServer> map;

        private LinkedBlockingDeque<QPair> que;

        public Take(LinkedBlockingDeque<QPair> que, RouteServiceGrpc.RouteServiceStub commStub, ConcurrentHashMap<Integer,HeartBeatServer> map) {
            this.que = que;
            this.commStub = commStub;
            this.map = map;
        }

        @Override
        public void run() { 
            // MIGHT NEED TO ADD ADDITIONAL FIELD FOR KNOWING WHAT WORK SERVER PERFORMED WORK IF WE WANT TO KEEP TRACK OF THAT
            while (_isRunning && !que.isEmpty()) { //keeps going until que is empty
                // types of messages: Server Reply, Client Work, Unknown
				try {
                    QPair qp = que.poll();
					route.Route msg = qp.getRoute();
                    if (msg.getType().equals("Reply")) { // send reply to client (this prob has to be done by Worker instead)
                        qp.getObserver().onNext(msg); //might need to change message slightly
                        qp.getObserver().onCompleted();         
                    } else if (msg.getType().equals("Work")) { // send work to chosen work server
                        // Maybe if hashmap of heartbeats has not been updated for a bit then sleep this thread for a few sec
                        //could also use prio que and not take until size is 4
                        String payloadStr = new String(msg.getPayload().toByteArray());
                        route.Route newReq = constructMessage((int) msg.getId(), msg.getPath(),payloadStr, msg.getType(), findLeastBusyServer());
                        commStub.request(newReq, qp.getObserver());
                    } else { //does it make sense that leader would pass it forward or should it just throw out?

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
            int minHB = -1, minHBServer = -1; //MIGHT NEED TO CHANGE DEPENDING ON INDIVIDUAL SERVER HB
            for (Map.Entry<Integer,HeartBeatServer> entry : map.entrySet()) 
                minHBServer = (entry.getValue().getHB() > minHB) ? entry.getKey() : minHBServer;
            return minHBServer;
        }
        
    }

    //might need to make some of these static
    protected final static int port = 1; // should come from conf file eventually
    protected LinkedBlockingDeque<QPair> que; // que of requests
    protected ConcurrentHashMap<Integer,HeartBeatServer> networkStatus; //Key: ID of server, Value: Heartbeat status
    protected int heartbeatsUpdated = 0; //needs to be 4 for leader to distribute a server (might use later)
    private Server svr; // actual server
    protected RouteServiceGrpc.RouteServiceStub next; //stub to request to next server in cycle
    private HBMonitor hb;

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
        ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", RouteLeaderServer.port).usePlaintext().build();
		next = RouteServiceGrpc.newStub(ch);
        hb = new HBMonitor(next);
    }

    /**
	 * server received a message!
	 * responseObserver used to send results
	 * can send acknowledgement of request was accepted and another of the results of the request
	 */
    @Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {
        (new Put(que, networkStatus, request, responseObserver)).start();
        // maybe add some sleep here????
        (new Take(que, next, networkStatus)).start();
    }

    private static final route.Route constructMessage(int mID, String path, String payload, String type, int destination) {
		route.Route.Builder bld = route.Route.newBuilder();
		bld.setId(mID);
		bld.setOrigin(RouteLeaderServer.port); // MIGHT NEED TO CHANGE SO I DONT OVERWRITE
		bld.setPath(path);
        bld.setType(type);
        bld.setDestination(destination);
		bld.setPayload(ByteString.copyFrom(payload.getBytes()));
		return bld.build();
	}

    private void start() throws Exception {
        // TODO INTIALIZE VARS AND ADD LOOP FOR CHECKING HBs
		svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteLeaderServer())
				.build();

		System.out.println("-- starting Leader server");
		svr.start();
        hb.start(); // START OF HEARTBEAT REQUEST CYCLE
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
