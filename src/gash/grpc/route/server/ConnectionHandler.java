package gash.grpc.route.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.json.JSONObject;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import route.Route;
import route.RouteServiceGrpc;

public class ConnectionHandler extends Thread {
	private long id;
	private Socket connection;
	private int destination;	// grpc leader server port
	private InputStream in = null;
	private PrintWriter out = null;
	private BufferedReader reader = null;
	protected static Logger logger = Logger.getLogger("connection");

	public ConnectionHandler(Socket connection, int destination, long id) {
		this.connection = connection;
		this.destination = destination;
		this.id = id;

		// configuring logger
			logger.setUseParentHandlers(false);
			Path p = Paths.get("logs", "connection" + id + ".log");
			if (!Files.exists(p.getParent())) {
				try {
					Files.createDirectory(p.getParent());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				FileHandler fh = new FileHandler("logs/connection" + id + ".log");
				logger.addHandler(fh);
	        	SimpleFormatter formatter = new SimpleFormatter();  
	        	fh.setFormatter(formatter);
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
	}

	@Override
	public void run() {
		try {
			in = connection.getInputStream();
			out = new PrintWriter(connection.getOutputStream(), true);
			reader = new BufferedReader(new InputStreamReader(in));

			if (in == null || out == null)
				throw new RuntimeException("Unable to get in/out streams");
			
			

			ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", destination).usePlaintext().build();
			RouteServiceGrpc.RouteServiceStub stub = RouteServiceGrpc.newStub(ch);
			final CountDownLatch finishLatch = new CountDownLatch(1);
			StreamObserver<route.Route> responseObserver = new StreamObserver<route.Route>() {
				@Override
				public void onNext(route.Route msg) {
					System.out.println(msg.getId());
					var payload = new String(msg.getPayload().toByteArray());
					int messageId = (int) msg.getId();
					long serverId = msg.getOrigin();
					String message = "Received response for message " + messageId + " from server " + serverId + ": " + payload;
					logger.info(message);
					ConnectionHandler.this.notify(message);
				}

				@Override
				public void onError(Throwable t) {
					finishLatch.countDown();
				}

				@Override
				public void onCompleted() {
					finishLatch.countDown();
				}
			};
		
			String message;
			while ((message = reader.readLine()) != null) {
				System.out.println("Received: " + message);
				System.out.flush();

				// convert message to JSONObject
				JSONObject json = new JSONObject(message);
				Route.Builder bld = Route.newBuilder();

				// let Client know ServerHook received message
				// out.write("ServerHook received message: " + json.getLong("id"));
				// out.write('\n');
				// out.flush();

				// JsonFormat.parser().merge(message, bld);

				// not ideal, need to figure out how to encode payload as byte in JSONObject and
				// use JSONFormat.parser()
				bld.setId(json.getLong("id"));
				bld.setOrigin(json.getLong("origin"));
				bld.setPath(json.getString("path"));
				bld.setWorkType(json.getInt("workType"));
				byte[] payload = json.getString("payload").getBytes();
				bld.setPayload(ByteString.copyFrom(payload));
				
				logger.info("Sent message " + json.getLong("id") + "from Client " + json.getLong("origin"));
				
				// send to grpc server
				stub.request(bld.build(), responseObserver);
			}
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void notify(String reply) {
		out.write(reply);
		out.write('\n');
		out.flush();
	}
}
