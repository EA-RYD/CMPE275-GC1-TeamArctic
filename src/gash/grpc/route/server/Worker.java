package gash.grpc.route.server;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

public class Worker extends Thread {
    private WorkerServer _server;
    public enum WorkerType {
        Worker, HBManager
    }

    public static final int maxWorkSize = 50;

    //List of work for worker threads, list of heartbeats for HBManager
    private LinkedBlockingDeque<Work> works;

    //Type of worker (regular worker or heartbeat manager)
    WorkerType type;

    //Heartbeat manager needs reference to list of workers?
    private List<Worker> workers;

    private int sleepTimeAllWorkers;

    private static AtomicInteger idGen;
    private int id;

    static {
        idGen = new AtomicInteger();
    }
    
    public Worker(WorkerServer server, WorkerType type) {
        this._server = server;
        this.type = type;
        this.id = idGen.incrementAndGet();
        this.sleepTimeAllWorkers = 0;

        works = new LinkedBlockingDeque<>();
    }

    @Override
    public void run() {
        //TODO not a good idea to spin on work --> wastes CPU cycles
        while (true) {
            if (type == WorkerType.Worker) {
                var work = works.poll();
                doWork(work);
            }
            else {
                updateHBQueue();

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void updateHBQueue() {
        sleepTimeAllWorkers = 0;
        works.clear();

        for (Worker worker : workers) {
            int cumulativeSleepPerWorker = 0;

            for (Work w : worker.getWorks()) {
                cumulativeSleepPerWorker += w.getSleepTime();
            }

            sleepTimeAllWorkers += cumulativeSleepPerWorker;

            //Get string representation of each worker's heartbeat status
            String hbStatus = id + " " + worker.getWorks().size() + " " + cumulativeSleepPerWorker;

            works.add(new Work(hbStatus.getBytes()));
        }
    }

    private void doWork(Work w) {
        if (w != null) {
            System.out.println("Worker " + id + " is working on work " + w.workId);
            // initialize the response routing/header information
            route.Route.Builder builder = route.Route.newBuilder();
            builder.setId(RouteServer.getInstance().getNextMessageID());
            builder.setOrigin(_server.serverID);
            builder.setDestination(w.toId);

            // do the work
            builder.setPayload(process(w));
            // send back the response to client
            w.responseObserver.onNext(builder.build());
            w.responseObserver.onCompleted();
            try {
                Thread.sleep(w.getSleepTime());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    protected ByteString process(Work w) {
        // TODO placeholder
        String content = "";
        byte[] raw = content.getBytes();

        return ByteString.copyFrom(raw);
    }

    public void addWork(Work w) {
        works.add(w);
    }

    public LinkedBlockingDeque<Work> getWorks() {
        return works;
    }

    public void setWorkers(List<Worker> workers) {
        this.workers = workers;
    }

    public int getWorkerId() {
        return this.id;
    }

    public int getSleepTimeAllWorkers() {
        return this.sleepTimeAllWorkers;
    }
}