package gash.grpc.route.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ConnectionHandler extends Thread {
	private Socket connection;
	private InputStream in = null;
	private PrintWriter out = null;
	private BufferedReader reader = null;

	public ConnectionHandler(Socket connection) {
		this.connection = connection;
	}
	
	@Override
	public void run() {
		try {
			in = connection.getInputStream();
			out = new PrintWriter(connection.getOutputStream(), true);
			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			
			if (in == null || out == null)
				throw new RuntimeException("Unable to get in/out streams");
			
			while (true) {
				String message;
				if ((message = reader.readLine()) != null) {
				    //out.println(message);
					System.out.println("Received: " + message);
				    System.out.flush();
				    
				    // convert message to JSONObject and pass it back to ServerHook?
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
