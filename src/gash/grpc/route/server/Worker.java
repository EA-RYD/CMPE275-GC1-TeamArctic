package gash.grpc.route.server;

public class Worker extends Thread {
    private RouteServerImpl _server;

    public Worker(RouteServerImpl server) {
        this._server = server;
    }

    @Override
    public void run() {
        //TODO not a good idea to spin on work --> wastes CPU cycles
        while (true) {
            var work = _server.checkForWork();
            doWork(work);
        }
    }

    private void doWork(Work w) {
        if (w != null) {
            //TODO do work
        }
    }
}
