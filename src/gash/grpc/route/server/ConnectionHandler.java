package gash.grpc.route.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

import org.json.JSONObject;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import route.Route;
import route.RouteServiceGrpc;

public class ConnectionHandler extends Thread {
	private Socket connection;
	private int destination;	// grpc leader server port
	private InputStream in = null;
	private PrintWriter out = null;
	private BufferedReader reader = null;

	public ConnectionHandler(Socket connection, int destination) {
		this.connection = connection;
		this.destination = destination;
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
					var payload = new String(msg.getPayload().toByteArray());
					long serverId = msg.getOrigin();
					ConnectionHandler.this.notify("Received response from server " + serverId + ": " + payload);
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
				out.write("ServerHook received message: " + json.getLong("id"));
				out.write('\n');
				out.flush();

				// JsonFormat.parser().merge(message, bld);

				// not ideal, need to figure out how to encode payload as byte in JSONObject and
				// use JSONFormat.parser()
				bld.setId(json.getLong("id"));
				bld.setOrigin(json.getLong("origin"));
				bld.setPath(json.getString("path"));
				bld.setWorkType(json.getInt("workType"));
				byte[] payload = json.getString("payload").getBytes();
				bld.setPayload(ByteString.copyFrom(payload));

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
		out.write("Response from GRPC server: " + reply);
		out.write('\n');
		out.flush();
	}
}
