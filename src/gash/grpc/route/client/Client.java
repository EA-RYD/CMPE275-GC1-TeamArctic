package gash.grpc.route.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.json.JSONObject;

import com.google.protobuf.ByteString;

public class Client {
	private long clientID;
	private Properties setup;
	private Socket socket;
	private InputStreamReader in;
	private OutputStreamWriter out;
	private BufferedReader reader;
	protected static Logger logger = Logger.getLogger("client");
	
	public Client(Properties setup) {
		this.setup = setup;
		this.clientID = Long.parseLong(setup.getProperty("id"));
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
			Path p = Paths.get("logs", "client" + clientID + ".log");
			if (!Files.exists(p.getParent())) {
				Files.createDirectory(p.getParent());
			}
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
		int times = 100;
		for (int i = 0; i < times; i++) {
			JSONObject json = new JSONObject();
			json.put("id", i);
			json.put("origin", clientID);
			json.put("destination", "somewhere");
			json.put("path", "/to/somewhere");
			json.put("workType" , i % 4 + 1);
			json.put("payload", "Hello");	// how to store it so that it is compatible with .proto type bytes?
			
			try {
				out.write(json.toString());
				out.write('\n');
				out.flush();
				System.out.println("Sent request: " + i);
				Thread.sleep(50);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		while (true) {
			// TODO wait for replies from server
			try {
				String response = reader.readLine();
				if (response != null) {
					logger.info(response);
					System.out.println(response);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		String path = args[0];
		
		// getting client configurations from conf file
		FileInputStream fis = null;
		Properties conf = new Properties();
		try {
			File file = new File(path);
			if (!file.exists())
				throw new IOException("missing config file for client");
			fis = new FileInputStream(path);
			conf.load(fis);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
		
		Client client = new Client(conf);
		client.run();
	}
}
