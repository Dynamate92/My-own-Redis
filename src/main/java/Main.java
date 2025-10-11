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
      Map<String, List<String>> listsStore = new HashMap<>();
      String content;
      int currentArrayCount = -1;
      while ((content = clientInput.readLine()) != null) {
        if (content.startsWith("*")) {
          try {
            currentArrayCount = Integer.parseInt(content.substring(1).trim());
            continue; // mergi la următoarea linie ($len sau comanda)
          } catch (NumberFormatException ignored) {}
        }
        if(content.equalsIgnoreCase("PING")) {
          clientOutput.write("+PONG\r\n");
          clientOutput.flush();
        } else if (content.equalsIgnoreCase("echo")) {
          String numBytes = clientInput.readLine();
          clientOutput.write(numBytes + "\r\n" + clientInput.readLine() +
                  "\r\n");
          clientOutput.flush();
        } else if (content.equalsIgnoreCase("set")) {
          clientInput.readLine();              // $len key
          String key = clientInput.readLine(); // key
          clientInput.readLine();              // $len val
          String value = clientInput.readLine();// val

          Long expireAt = null;

          // lookahead doar daca mai sunt caractere gata de citit (altfel nu are optiuni)
          if (clientInput.ready()) {
            clientInput.mark(1024);
            String maybeLen = clientInput.readLine();
            if (maybeLen != null && maybeLen.startsWith("$")) {
              String opt = clientInput.readLine();     // EX / PX
              String ttlLen = clientInput.readLine();  // $<len>
              String ttlStr = clientInput.readLine();  // "100" etc.
              try {
                long ttl = Long.parseLong(ttlStr);
                long now = System.currentTimeMillis();
                if ("ex".equalsIgnoreCase(opt)) expireAt = now + ttl * 1000L;
                else if ("px".equalsIgnoreCase(opt)) expireAt = now + ttl;
              } catch (NumberFormatException e) {
                System.out.println("Error " + e);
              }
            } else {
              clientInput.reset(); // nu era o optiune, revenim
            }
          }

          commandsStore.put(key, value);
          if (expireAt != null) expiries.put(key, expireAt); else expiries.remove(key);

          clientOutput.write("+OK\r\n");
          clientOutput.flush();
        } else if (content.equalsIgnoreCase("get")) {
          clientInput.readLine(); // $skip
          String key = clientInput.readLine();
          Long exp = expiries.get(key);
          if (exp != null && System.currentTimeMillis() >= exp) {
            expiries.remove(key);
            commandsStore.remove(key);

          }
          String value = commandsStore.get(key);
          if(value != null) {
            clientOutput.write("$" + value.length()+ "\r\n" + value + "\r\n");
            clientOutput.flush();
          } else {
            clientOutput.write("$-1\r\n");
            clientOutput.flush();
          }
        } else if (content.equalsIgnoreCase("rpush")) {
          // stim exact cate argumente urmeaza in aceasta comanda:
          // currentArrayCount = 1 (comanda) + 1 (key) + k (valori)
          // deci k = currentArrayCount - 2
          int valuesToRead = Math.max(0, currentArrayCount - 2);

          // key
          clientInput.readLine();               // $<len> key
          String key = clientInput.readLine();  // key

          // asigura lista
          List<String> list = listsStore.getOrDefault(key, new ArrayList<>());

          // citeste exact k valori (fara blocaj)
          for (int i = 0; i < valuesToRead; i++) {
            clientInput.readLine();                 // $<len> value
            String value = clientInput.readLine();  // value
            list.add(value);
          }

          listsStore.put(key, list);

          // raspuns integer cu noua lungime
          clientOutput.write(":" + list.size() + "\r\n");
          clientOutput.flush();

          // reset pentru urmatoarea comanda
          currentArrayCount = -1;
        }
      }
    } catch (IOException e) {
      System.out.println("Error" + e.getMessage());
    }
  }
}
