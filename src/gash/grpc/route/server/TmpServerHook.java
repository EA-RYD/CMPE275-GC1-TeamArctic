package gash.grpc.route.server;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

import org.json.JSONObject;

/**
 * 
 * for testing purposes
 *
 */

public class TmpServerHook {
	private Properties setup;
	private ServerSocket socket;
	
	private LinkedBlockingDeque<JSONObject> que;
	
	public TmpServerHook(Properties setup) {
		this.setup = setup;
	}
	
	// original client uses this 
    //maybe just adds request to queue?
    // put thread places request in server
    public void request(String json) {
        // TODO
        
        
    }
    
    public void start() {
    	if (setup == null)
			throw new RuntimeException("Missing configuration properties");

		try {
			int port = Integer.parseInt(setup.getProperty("port"));
			socket = new ServerSocket(port);

			while (true) {
				Socket s = socket.accept();

				System.out.println("--> server got a client connection");
				System.out.flush();
				
				// pass client connection to a connection handler
				ConnectionHandler ch = new ConnectionHandler(s);	// maybe pass this class to ConnectionHandler?
				ch.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
	public static void main(String[] args) {
		Properties p = new Properties();
		p.setProperty("port", "2100");

		TmpServerHook serverHook = new TmpServerHook(p);
		serverHook.start();
	}
}
