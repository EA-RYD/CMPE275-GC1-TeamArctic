package gash.grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

public class TmpServerHook {
	protected static int port;
	protected static int destination;	// grpc leader server port
	private ServerSocket socket;
	private long idGenerator = 1;
	
    /**
	* Configuration of the server hook's port
	*/
	private static Properties getConfiguration(final File path) throws IOException {
		if (!path.exists())
			throw new IOException("missing config file for server hook");

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
	
	private static void configure(Properties prop) {
        String tmp = prop.getProperty("server.port");
        port = Integer.parseInt(tmp);
        String dest = prop.getProperty("server.destination");
        destination = Integer.parseInt(dest);
	}
    
    public void start() {
    	if (port == 0 && destination == 0)
			throw new RuntimeException("Missing configuration properties");

		try {
			System.out.println("-- starting server hook on port " + port);
			socket = new ServerSocket(port);

			while (true) {
				Socket s = socket.accept();

				System.out.println("--> server hook got a client connection");
				System.out.flush();
				
				// pass client connection to a connection handler
				ConnectionHandler ch = new ConnectionHandler(s, destination, idGenerator++);
				ch.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
	public static void main(String[] args) {
		String path = args[0];
		try {
			Properties conf = TmpServerHook.getConfiguration(new File(path));
			TmpServerHook.configure(conf);

			final TmpServerHook serverHook = new TmpServerHook();
			serverHook.start();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
