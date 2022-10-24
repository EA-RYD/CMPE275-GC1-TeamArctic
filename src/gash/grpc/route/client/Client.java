package gash.grpc.route.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.json.JSONObject;

public class Client {
	private static long clientID = 501;
	private Properties setup;
	private Socket socket;
	private InputStreamReader in;
	private OutputStreamWriter out;
	private BufferedReader reader;
	protected static Logger logger = Logger.getLogger("client");
	
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
			in = new InputStreamReader(socket.getInputStream());
			out = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
			reader = new BufferedReader(in);
			
			// configuring logger
			logger.setUseParentHandlers(false);
	        FileHandler fh = new FileHandler("logs/client" + clientID + ".log");  
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);  
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		CountDownLatch latch = new CountDownLatch(1);
		Listener listener = new Listener(in, reader, logger, latch);
		listener.start();
		
		int times = 500;
		for (int i = 0; i < times; i++) {
			JSONObject json = new JSONObject();
			json.put("id", i);
			json.put("origin", Client.clientID);
			json.put("destination", "somewhere");
			json.put("path", "/to/somewhere");
			json.put("workType" , i % 4 + 1);
			json.put("payload", "Hello");	// how to store it so that it is compatible with .proto type bytes?
			
			try {
				out.write(json.toString());
				out.write('\n');
				out.flush();
				logger.info("Sent: " + json.toString());
				System.out.println("Sent request: " + i);
				Thread.sleep(50);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public class Listener extends Thread {
		private InputStreamReader in;
		private BufferedReader reader;
		private Logger logger;
		private CountDownLatch latch;
		
		public Listener(InputStreamReader in, BufferedReader reader, Logger logger, CountDownLatch latch) {
			this.in = in;
			this.logger = logger;
			this.reader = reader;
			this.latch = latch;
		}
		
		@Override
		public void run() {
			String response;
			try {
				while ((response = reader.readLine()) != null) {
					logger.info(response);
					System.out.println(response);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			latch.countDown();
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
