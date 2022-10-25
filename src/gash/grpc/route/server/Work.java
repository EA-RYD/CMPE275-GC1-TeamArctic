package gash.grpc.route.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.stub.StreamObserver;
import route.Route;

public class Work {
    protected StreamObserver<route.Route> responseObserver;
    private route.Route request;
    private static AtomicInteger sIdGen;

    public final int workId;
    public long fromId;
    public long toId;
    public WorkType isA;
    //Simulating computational time
    private int sleepTime;

    // a placeholder
    public byte[] payload;
    public enum WorkType {
        POST, GET, PUT, DELETE, HeartBeat, Unknown
    }
    private Map<Integer, WorkType> routeMap = new HashMap<>() {{
        put(1, WorkType.POST);
        put(2, WorkType.GET);
        put(3, WorkType.PUT);
        put(4, WorkType.DELETE);
        put(5, WorkType.HeartBeat);
    }};

    static {
        sIdGen = new AtomicInteger();
    }

    public Work(StreamObserver<route.Route> rs, route.Route ro) {
        this.responseObserver = rs;
        this.request = ro;
        // this.workId = sIdGen.incrementAndGet();
        this.workId = (int)ro.getId();
        this.fromId = ro.getOrigin();
        this.toId = ro.getDestination();
        this.isA = routeMap.containsKey(ro.getWorkType()) ? routeMap.get(ro.getWorkType()) : WorkType.Unknown;
    }

    //Constructor for heartbeat work
    //For heartbeat at the worker thread level, heartbeat at server level is handled at other constructor
    public Work(byte[] payload) {
        this.workId = sIdGen.incrementAndGet();
        this.isA = WorkType.HeartBeat;
        this.payload = payload;
    }

    // get the sleep time for different work types
    public int getSleepTime() {
        if (this.isA == null) {
            throw new IllegalArgumentException("WorkType is not set");
        }
        switch (this.isA) {
            case POST:
                this.sleepTime = 1000;
                break;
            case GET:
                this.sleepTime = 500;
                break;
            case PUT:
                this.sleepTime = 1000;
                break;
            case DELETE:
                this.sleepTime = 500;
                break;
            default:
                break;
        }
        return this.sleepTime;
    }
}