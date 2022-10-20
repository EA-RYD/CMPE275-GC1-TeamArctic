package gash.grpc.route.client;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.json.JSONObject;

import com.google.protobuf.ByteString;

public class Client {
	private static long clientID = 501;		// need to find out what this is for
	private Properties setup;
	private Socket socket;
	private OutputStreamWriter out;
	
	public Client(Properties setup) {
		this.setup = setup;
		setUp();
	}
	
	public void setUp() {
		if (socket != null)
			return;
		
		String host = setup.getProperty("host");
		String port = setup.getProperty("port");
		if (host == null || port == null)
			throw new RuntimeException("Missing port and/or host");
		
		try {
			socket = new Socket(host, Integer.parseInt(port));
			out = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {	
		int times = 500;
		for (int i = 0; i < times; i++) {
			JSONObject json = new JSONObject();
			json.put("id", i);
			json.put("origin", Client.clientID);
			json.put("destination", "somewhere");
			json.put("path", "/to/somewhere");
			json.put("workType" , 1);
			json.put("payload", "Hello");	// how to store it so that it is compatible with .proto type bytes?
			
			try {
				out.write(json.toString());
				out.write('\n');
				out.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		while (true) {
			// TODO wait for replies from server and ends once it has received all replies
		}
	}
	
	public static void main(String[] args) {
		Properties p = new Properties();
		p.setProperty("host", "127.0.0.1");
		p.setProperty("port", "2100");

		Client client = new Client(p);
		client.run();
	}
}
