package gash.grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc.RouteServiceImplBase;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class RouteLeaderServer extends RouteServiceImplBase {

    private class HeartBeat { // dummy temp
    }
    
    // route.Route

    private LinkedBlockingDeque<route.Route> que; 
    private ConcurrentHashMap<Integer,HeartBeat> networkStatus;
    private Server svr;
    // maybe there should we worker threads that take and put from que?

    public static void main(String[] args) throws Exception {
        //TODO
    }

    public void systemCheck() { //sends out requests for heartbeats
        // TODO
    }

    protected ByteString process(route.Route msg) { //process route object
		// TODO 

		return null;
	}

    private void sendAcknowledgement(StreamObserver<route.Route> responseObserver) {
        // TODO
    }

    //configure server communication setup, maybe hashmap setup goes here?
    private static Properties getConfiguration(final File path) throws IOException { 
        //someone else writing config file
        return null;
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

    private boolean verify(route.Route request) {
        // TODO
		return true;
	}

    private void start() throws Exception {
		// TODO
	}

    protected void stop() {
        // TODO
	}

    private void blockUntilShutdown() throws Exception {
		/* TODO what clean up is required? */
	}


}
