import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;

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
    try (BufferedReader clientInput = new BufferedReader(new InputStreamReader(client.getInputStream()));
         BufferedWriter clientOutput = new BufferedWriter(
                 new OutputStreamWriter(client.getOutputStream()));) {
      Map<String, String> commandsStore = new HashMap<>(); // for SET and get
      Map<String, Long> expiries = new HashMap<>();
      String content;
      while ((content = clientInput.readLine()) != null) {
        if(content.equalsIgnoreCase("PING")) {
          clientOutput.write("+PONG\r\n");
          clientOutput.flush();
        } else if (content.equalsIgnoreCase("echo")) {
          String numBytes = clientInput.readLine();
          clientOutput.write(numBytes + "\r\n" + clientInput.readLine() +
                  "\r\n");
          clientOutput.flush();
        } else if (content.equalsIgnoreCase("set")) {
          String contextExpire;
          clientInput.readLine(); // $ceva
          String key = clientInput.readLine(); // $cheia cautata
          clientInput.readLine(); // $skip
          String value = clientInput.readLine(); // $valoarea cautata
          clientInput.readLine(); // $skip
          Long expireAt = null;
          clientInput.mark(1024);
          String maybeLen = clientInput.readLine();
          if(maybeLen != null && maybeLen.startsWith("$")) {
            String opt = clientInput.readLine();
            clientInput.readLine();
            String ttlStr = clientInput.readLine();
            try {
              long ttl = Long.parseLong(ttlStr);
              long now = System.currentTimeMillis();
              if (opt.equalsIgnoreCase("ex")) {
                expireAt = now + ttl * 1000L;
              } else if (opt.equalsIgnoreCase("px")) {
                expireAt = now + ttl;
              }
            } catch (NumberFormatException e) {
              System.out.println("Error" + e);
            }
          }
          commandsStore.put(key,value);
          if (expireAt != null) expiries.put(key, expireAt);
          else expiries.remove(key);
          clientOutput.write("+OK\r\n");
          clientOutput.flush();
        } else if (content.equalsIgnoreCase("get")) {
          clientInput.readLine(); // $skip
          String key = clientInput.readLine();
          Long exp = expiries.get(key);
          if (exp != null && System.currentTimeMillis() >= exp) {
            expiries.remove(key);
            commandsStore.remove(key);
            clientOutput.write("$-1\r\n");
            clientOutput.flush();
          }
          String value = commandsStore.get(key);
          if(value != null) {
            clientOutput.write("$" + value.length()+ "\r\n" + value + "\r\n");
            clientOutput.flush();
          } else {
            clientOutput.write("$-1\r\n");
            clientOutput.flush();
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Error" + e.getMessage());
    }
  }
}
