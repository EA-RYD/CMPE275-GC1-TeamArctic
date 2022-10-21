package gash.grpc.route.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.json.JSONObject;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

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
			
			ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", 2345).usePlaintext().build();
			RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
			
			while (true) {
				String message;
				if ((message = reader.readLine()) != null) {
				    // out.println(message);
					System.out.println("Received: " + message);
				    System.out.flush();
				    
				    // convert message to JSONObject and pass it back to ServerHook?
				    JSONObject json = new JSONObject(message);
				    Route.Builder bld = Route.newBuilder();
				    
				    // let Client know ServerHook received message
				    out.write("ServerHook received message: " + json.getLong("id"));
				    out.write('\n');
					out.flush();
					
				    // JsonFormat.parser().merge(message, bld);
		
				    // not ideal, need to figure out how to encode payload as byte in JSONObject and use JSONFormat.parser()
					bld.setId(json.getLong("id"));
					bld.setOrigin(json.getLong("origin"));
					bld.setPath(json.getString("path"));
					bld.setWorkType(json.getInt("workType"));
					byte[] payload = json.getString("payload").getBytes();
					bld.setPayload(ByteString.copyFrom(payload));
					
				    // send to grpc server
					Route r = stub.request(bld.build());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
