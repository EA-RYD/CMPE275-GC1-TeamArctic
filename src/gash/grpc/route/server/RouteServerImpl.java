package gash.grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc.RouteServiceImplBase;

/**
 * copyright 2021, gash
 *
 * Gash licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class RouteServerImpl extends RouteServiceImplBase {
	private Server svr;

	// queue/container is just a placeholder
	// an array is not MT-safe
	private ArrayList<Work> queue = new ArrayList<>();
	//private ArrayList<Heartbeat> hb = new ArrayList<>();
	private Thread worker;

	/**
	* Configuration of the server's identity, port, and role
	*/
	private static Properties getConfiguration(final File path) throws IOException {
		if (!path.exists())
			throw new IOException("missing file");

		Properties rtn = new Properties();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(path);
			rtn.load(fis);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}

		return rtn;
	}


	public Work checkForWork() {
		return queue.get(0);
	}


	protected ByteString process(route.Route msg) {
		// TODO placeholder
		String content = new String(msg.getPayload().toByteArray());
		System.out.println("-- got: " + msg.getOrigin() + ", worktype: " + msg.getWorkType() + ", with: " + content);

		// TODO complete processing
		final String blank = "blank";
		byte[] raw = blank.getBytes();

		return ByteString.copyFrom(raw);
	}

	public static void main(String[] args) throws Exception {
		// TODO check args!

		String path = args[0];
		try {
			Properties conf = RouteServerImpl.getConfiguration(new File(path));
			RouteServer.configure(conf);

			/* Similar to the socket, waiting for a connection */
			final RouteServerImpl impl = new RouteServerImpl();
			impl.start();
			impl.blockUntilShutdown();

		} catch (IOException e) {
			// TODO better error message
			e.printStackTrace();
		}
	}

	private void start() throws Exception {
		svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteServerImpl())
				.build();

		System.out.println("-- starting server");
		svr.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				RouteServerImpl.this.stop();
			}
		});
	}

	protected void stop() {
		svr.shutdown();
	}

	private void blockUntilShutdown() throws Exception {
		/* TODO what clean up is required? */
		svr.awaitTermination();
	}

	/**
	 * server received a message!
	 * transform this to a delay response (delay processing?)
	 * responseObserver used to send results
	 * can send acknowledgement of request was accepted and another of the results of the request
	 */
	@Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {

		// TODO refactor to use RouteServer to isolate implementation from
		// transportation


		/*
		//route.Route.Builder ack = null;
		route.Route.Builder builder = route.Route.newBuilder();


		//if verification is successful, ack = route.Route.newBuilder(), if block lasts until setting payload

		//delay work
		//var w= new Work(responseObserver, request);
		//queue.add(w);

		// routing/header information
		builder.setId(RouteServer.getInstance().getNextMessageID());
		builder.setOrigin(RouteServer.getInstance().getServerID());
		builder.setDestination(request.getOrigin());
		builder.setPath(request.getPath());

		// do the work and reply
		builder.setPayload(process(request));
		*/
		route.Route.Builder ack = null;
		if (verify(request)) {
			ack = route.Route.newBuilder();

			var w = new Work(responseObserver, request);
			queue.add(w);

			// routing/header information
			//remove in place of non-blocking?
			ack.setId(RouteServer.getInstance().getNextMessageID());
			ack.setOrigin(RouteServer.getInstance().getServerID());

			ack.setWorkType(request.getWorkType());
			ack.setDestination(request.getOrigin());
			ack.setPath(request.getPath());

			//non-blocking handling of messages
			//if (request.type == work) enqueueAsWork(request)
			//else if (Request.type == heartbeat) enqueueAsMgmt(request);
			//else error(request);
			//remove routing/header info?
			//remove setPayload
			//ack = acceptedWork(request, response)

			ack.setPayload(process(request));
		}
		else {
			//send acknowledgement that the server is not going to process the request
			//buildRejection(ack, request)
		}


		route.Route rtn = ack.build();
		//route.Route rtn = builder.build();

		responseObserver.onNext(rtn);
		responseObserver.onCompleted();

	}

	private boolean verify(route.Route request) {
		return true;
	}
}
