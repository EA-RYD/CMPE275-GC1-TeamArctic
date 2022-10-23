package gash.grpc.route.server;

import io.grpc.stub.StreamObserver;

public class QPair {
    private StreamObserver<route.Route> obs;
    private route.Route msg;

    public QPair(StreamObserver<route.Route> obs, route.Route msg) {
        this.obs = obs;
        this.msg = msg;
    }

    public StreamObserver<route.Route> getObserver() {
        return obs;
    }

    public route.Route getRoute() {
        return msg;
    }
}
