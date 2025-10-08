import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    int port = 6379;
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      serverSocket.setReuseAddress(true);
      System.out.println("Listening on: " + port);
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start(); //lambda function
      }
    } catch (IOException e) {
      System.out.println("Error " + e.getMessage());
    }
  }

  static void handleClient(Socket client) {
    try (Socket c = client;
         InputStream in = c.getInputStream();
         OutputStream out = c.getOutputStream()) {
      byte[] buf = new byte[1024];
      int n;
      while ((n = in.read(buf)) != 1) {
        out.write("+PONG\r\n".getBytes());
        out.flush();
      }
    } catch (IOException e) {
      System.out.println("Error" + e.getMessage());
    }
  }
}
