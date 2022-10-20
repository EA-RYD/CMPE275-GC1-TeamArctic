package gash.grpc.route.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.stub.StreamObserver;
import route.Route;

public class Work {
    private StreamObserver<route.Route> responseObserver;
    private Route request;
    private static AtomicInteger sIdGen;

    public final int workId;
    public long fromId;
    public long toId;
    public WorkType isA;

    // a placeholder
    public byte payload;
    public enum WorkType {
        POST, GET, PUT, DELETE, Response, HeartBeat, Unknown
    }
    private Map<Integer, WorkType> routeMap = new HashMap<>() {{
        put(1, WorkType.POST);
        put(2, WorkType.GET);
        put(3, WorkType.PUT);
        put(4, WorkType.DELETE);
        put(5, WorkType.Response);
        put(6, WorkType.HeartBeat);
    }};

    static {
        sIdGen = new AtomicInteger();
    }

    public Work(StreamObserver<route.Route> rs, Route ro) {
        this.responseObserver = rs;
        this.request = ro;
        this.workId = sIdGen.incrementAndGet();
        this.fromId = ro.getOrigin();
        this.toId = ro.getDestination();
        this.isA = routeMap.containsKey(ro.getWorkType()) ? routeMap.get(ro.getWorkType()) : WorkType.Unknown;
    }

    public boolean needProcess() {
        return this.isA != WorkType.Response && this.isA != WorkType.HeartBeat && this.isA != WorkType.Unknown;
    }
}
