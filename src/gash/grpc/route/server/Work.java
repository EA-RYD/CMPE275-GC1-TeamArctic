package gash.grpc.route.server;

import io.grpc.stub.StreamObserver;
import route.Route;

public class Work {
    private StreamObserver<route.Route> responseObserver;
    private Route request;

    private int stats;
    private int someOtherStuff;

    public Work(StreamObserver<route.Route> rs, Route ro) {
        this.responseObserver = rs;
        this.request = ro;
    }
}
