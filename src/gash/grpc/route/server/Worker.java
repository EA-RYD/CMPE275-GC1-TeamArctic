package gash.grpc.route.server;

import com.google.protobuf.ByteString;

public class Worker extends Thread {
    private WorkerServer _server;

    public Worker(WorkerServer server) {
        this._server = server;
    }

    @Override
    public void run() {
        //TODO not a good idea to spin on work --> wastes CPU cycles
        while (true) {
            // TODO implement a queue/container to hold work
        }
    }

    private void doWork(Work w) {
        if (w != null) {
            // initialize the response routing/header information
            route.Route.Builder builder = route.Route.newBuilder();
            builder.setId(RouteServer.getInstance().getNextMessageID());
            builder.setOrigin(w.fromId);
            builder.setDestination(w.toId);

            // do the work
            if (w.isA == Work.WorkType.HeartBeat) {
                builder.setWorkType(5);

                // do the work and reply
                builder.setPayload(processHB_Worker(w));
                _server.comm.request(builder.build(), null);
            } else {
                builder.setPayload(process(w));
                // send back the response to client
                w.responseObserver.onNext(builder.build());
                w.responseObserver.onCompleted();
            }
        }
    }

    // TODO: how do we calculate the heartbeat?
    protected ByteString processHB_Worker(Work w) {
		// TODO placeholder
		String content = "";
		byte[] raw = content.getBytes();

		return ByteString.copyFrom(raw);
	}
    
    protected ByteString process(Work w) {
        // TODO placeholder
        String content = "";
        byte[] raw = content.getBytes();

        return ByteString.copyFrom(raw);
    }
}
