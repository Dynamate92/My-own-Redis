import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;



public class Main {
  static final Map<String , List<String>> SHARED_LIST = Collections.synchronizedMap(new HashMap<>());
  static final Map<String, Deque<WaitingClient>> WAITERS = new ConcurrentHashMap<>();

  static class WaitingClient {
    final Socket socket;
    final BufferedWriter out;
    final String key;
    WaitingClient(Socket socket, BufferedWriter out, String key) {
      this.socket = socket;
      this.out = out;
      this.key = key;
    }
  }
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
      listsStore = SHARED_LIST;
      Set<String> streamKeys = new HashSet<>();
      Map<String, String> lastStreamId = new HashMap<>();
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
        } else if (content.equalsIgnoreCase("lrange")) {
          // $len + key
          clientInput.readLine();
          String key = clientInput.readLine();

          // $len + start
          clientInput.readLine();
          String startStr = clientInput.readLine();

          // $len + stop
          clientInput.readLine();
          String stopStr = clientInput.readLine();

          int start = Integer.parseInt(startStr);
          int stop = Integer.parseInt(stopStr);

          List<String> list = listsStore.get(key);
          if (list == null) {
            // lista nu exista -> raspuns gol
            clientOutput.write("*0\r\n");
            clientOutput.flush();
            continue;
          }

          int size = list.size();

          // normalizeaza indicii
          if (start < 0) start = size + start;
          if (stop < 0) stop = size + stop;
          if (start < 0) start = 0;
          if (stop >= size) stop = size - 1;

          // daca range invalid → lista goala
          if (start > stop || start >= size) {
            clientOutput.write("*0\r\n");
            clientOutput.flush();
            continue;
          }

          // trimite raspunsul ca array RESP
          int count = stop - start + 1;
          clientOutput.write("*" + count + "\r\n");
          for (int i = start; i <= stop; i++) {
            String val = list.get(i);
            clientOutput.write("$" + val.length() + "\r\n" + val + "\r\n");
          }
          clientOutput.flush();
        } else if (content.equalsIgnoreCase("lpush")) {
          // total elemente in array = 1 (comanda) + 1 (key) + k valori
          int valuesToRead = Math.max(0, currentArrayCount - 2);

          // key
          clientInput.readLine();               // $<len> pentru key
          String key = clientInput.readLine();  // key

          // lista existenta sau noua
          List<String> list = listsStore.getOrDefault(key, new ArrayList<>());

          // citim FIX valuesToRead valori
          List<String> vals = new ArrayList<>(valuesToRead);
          for (int i = 0; i < valuesToRead; i++) {
            clientInput.readLine();                // $<len> pentru value
            String v = clientInput.readLine();     // value
            vals.add(v);
          }

          // lpush: insereaza la inceput pastrand ordinea LPUSH (ultimul citit ajunge cel mai stang)
          for (int i = 0; i < vals.size(); i++) {
            // varianta corecta pt LPUSH: primul argument dupa key trebuie sa ajunga cel mai la stanga
            // deci inseram in ORDINE inversa in lista
            // ex: LPUSH key a b c => lista: c, b, a
            // ca sa obtinem asta cu add(0,...), iteram de la 0 la n-1 si facem add(0, vals.get(i))? asta ar pune a,b,c -> c,b,a (corect)
            list.add(0, vals.get(i));
          }

          listsStore.put(key, list);

          // raspuns integer cu noua lungime
          clientOutput.write(":" + list.size() + "\r\n");
          clientOutput.flush();

          // reset pt urmatoarea comanda
          currentArrayCount = -1;
        } else if (content.equalsIgnoreCase("llen")) {
          clientInput.readLine();
          String key = clientInput.readLine();

          List<String> list = listsStore.get(key);

          int len;
          len = (list == null) ? 0 : list.size();

          clientOutput.write(":" + len + "\r\n");
          clientOutput.flush();
        } else if (content.equalsIgnoreCase("lpop")) {
          // *2 => LPOP key
          // *3 => LPOP key count
          boolean hasCount = (currentArrayCount == 3);

          // key
          clientInput.readLine();               // $len
          String key = clientInput.readLine();  // key

          int count = 1;
          if (hasCount) {
            clientInput.readLine();             // $len
            String countStr = clientInput.readLine();
            try { count = Integer.parseInt(countStr); } catch (NumberFormatException ignore) { count = 1; }
            if (count < 0) count = 0; // defensiv
          }

          List<String> list = listsStore.get(key);

          if (list == null || list.isEmpty() || count == 0) {
            // raspuns diferit in functie de prezenta lui count (conform Redis 6.2):
            if (hasCount) {
              clientOutput.write("*0\r\n");        // array gol
            } else {
              clientOutput.write("$-1\r\n");       // nil bulk
            }
            clientOutput.flush();
            currentArrayCount = -1;
            continue;
          }

          int toPop = Math.min(count, list.size());
          List<String> popped = new ArrayList<>(toPop);
          for (int i = 0; i < toPop; i++) {
            popped.add(list.remove(0));
          }
          if (list.isEmpty()) listsStore.remove(key);

          // raspuns:
          if (hasCount) {
            // cand exista 'count', raspunsul este INTOTDEAUNA un array
            clientOutput.write("*" + popped.size() + "\r\n");
            for (String v : popped) {
              clientOutput.write("$" + v.length() + "\r\n" + v + "\r\n");
            }
          } else {
            // fara 'count' -> bulk string sau nil (deja tratat mai sus)
            String v = popped.get(0);
            clientOutput.write("$" + v.length() + "\r\n" + v + "\r\n");
          }
          clientOutput.flush();
          currentArrayCount = -1;
        } else if (content.equalsIgnoreCase("blpop")) {
          // BLPOP key timeoutSec (timeout poate fi 0 sau non-zero)
          clientInput.readLine();              // $len key
          String key = clientInput.readLine(); // key
          clientInput.readLine();              // $len timeout
          String timeoutStr = clientInput.readLine();

          double timeoutSec;
          try { timeoutSec = Double.parseDouble(timeoutStr); } catch (NumberFormatException e) { timeoutSec = 0.0; }

          // incercare imediata (ca lpop)
          List<String> listNow = listsStore.get(key);
          if (listNow != null) {
            synchronized (listNow) {
              if (!listNow.isEmpty()) {
                String val = listNow.remove(0);
                if (listNow.isEmpty()) listsStore.remove(key);
                clientOutput.write("*2\r\n");
                clientOutput.write("$" + key.length() + "\r\n" + key + "\r\n");
                clientOutput.write("$" + val.length() + "\r\n" + val + "\r\n");
                clientOutput.flush();
                currentArrayCount = -1;
                continue;
              }
            }
          }

          // pune clientul in coada fifo pt key
          WAITERS.putIfAbsent(key, new ArrayDeque<>());
          Deque<WaitingClient> q = WAITERS.get(key);
          WaitingClient me = new WaitingClient(client, clientOutput, key);
          synchronized (q) { q.addLast(me); }

          // calculeaza deadline doar daca timeoutSec > 0
          final boolean hasDeadline = timeoutSec > 0.0;
          final long deadlineNs = hasDeadline ? (System.nanoTime() + (long)(timeoutSec * 1_000_000_000L)) : Long.MAX_VALUE;

          // asteptare prin polling scurt (nu modificam rpush)
          while (true) {
            boolean iAmHead;
            synchronized (q) { iAmHead = (q.peekFirst() == me); }

            if (iAmHead) {
              List<String> l = listsStore.get(key);
              String val = null;
              if (l != null) {
                synchronized (l) {
                  if (!l.isEmpty()) {
                    val = l.remove(0); // lpop
                    if (l.isEmpty()) listsStore.remove(key);
                  }
                }
              }
              if (val != null) {
                synchronized (q) { q.pollFirst(); } // ies din coada (fifo)
                try {
                  clientOutput.write("*2\r\n");
                  clientOutput.write("$" + key.length() + "\r\n" + key + "\r\n");
                  clientOutput.write("$" + val.length() + "\r\n" + val + "\r\n");
                  clientOutput.flush();
                } catch (IOException ignore) {}
                currentArrayCount = -1;
                break;
              }
            }

            // daca avem timeout nenul si a expirat, intoarcem null array si iesim
            if (hasDeadline && System.nanoTime() >= deadlineNs) {
              synchronized (q) { q.remove(me); }
              try { clientOutput.write("*-1\r\n"); clientOutput.flush(); } catch (IOException ignore) {}
              currentArrayCount = -1;
              break;
            }

            // sleep mic ca sa nu mancam cpu
            try { Thread.sleep(10); } catch (InterruptedException ignore) {}

            // daca socketul s-a inchis, curata si iesi
            if (client.isClosed()) {
              synchronized (q) { q.remove(me); }
              currentArrayCount = -1;
              break;
            }
          }

          continue;
        } else if (content.equalsIgnoreCase("type")) {
          // *2\r\n
          // $4\r\n
          // TYPE\r\n
          // $<len>\r\n
          // <key>\r\n

          clientInput.readLine();
          String key = clientInput.readLine();

          String type;

          if(commandsStore.containsKey(key)) {
            type = "string";
          } else if (listsStore.containsKey(key)) {
            type = "list";
          } else if (streamKeys.contains(key)){
            type = "stream";
          } else {
            type = "none";
          }
          clientOutput.write("+" + type + "\r\n");
          clientOutput.flush();
          currentArrayCount = -1;
        } else if (content.equalsIgnoreCase("xadd")) {
          // XADD key id field value [field value ...]
          // Acceptă:  "ms-seq" (explicit)  sau  "ms-*" (auto seq) în acest stage.

          // key
          clientInput.readLine();              // $len key
          String key = clientInput.readLine(); // key

          // id (poate fi "123-*", sau "123-4")
          clientInput.readLine();              // $len id
          String id = clientInput.readLine();

          // consumă perechile field/value (nu le folosim în acest stage)
          int remaining = Math.max(0, currentArrayCount - 3);
          for (int i = 0; i < remaining; i++) {
            clientInput.readLine(); // $len
            clientInput.readLine(); // field OR value
          }

          String newId;

          // === cazul: auto-seq, format "ms-*" ===
          if (id.endsWith("-*")) {
            String msStr = id.substring(0, id.length() - 2); // tot ce e inainte de "-*"
            long ms;
            try {
              ms = Long.parseLong(msStr);
            } catch (NumberFormatException nfe) {
              clientOutput.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
              clientOutput.flush();
              currentArrayCount = -1;
              continue;
            }

            // determină secvența conform regulilor
            long seq;
            String last = lastStreamId.get(key);
            if (last == null) {
              // stream gol
              seq = (ms == 0) ? 1 : 0;
            } else {
              int d = last.indexOf('-');
              long lastMs = Long.parseLong(last.substring(0, d));
              long lastSeq = Long.parseLong(last.substring(d + 1));

              if (ms < lastMs) {
                clientOutput.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                clientOutput.flush();
                currentArrayCount = -1;
                continue;
              } else if (ms == lastMs) {
                seq = lastSeq + 1;
              } else {
                // ms > lastMs, stream "gol" pentru ms-ul ăsta
                seq = (ms == 0) ? 1 : 0;
              }
            }

            newId = ms + "-" + seq;

          } else {
            // === cazul: explicit "ms-seq" (validarea de la etapa precedentă) ===
            if ("0-0".equals(id)) {
              clientOutput.write("-ERR The ID specified in XADD must be greater than 0-0\r\n");
              clientOutput.flush();
              currentArrayCount = -1;
              continue;
            }

            long curMs, curSeq;
            try {
              int dash = id.indexOf('-');
              if (dash <= 0 || dash == id.length() - 1) throw new NumberFormatException();
              curMs = Long.parseLong(id.substring(0, dash));
              curSeq = Long.parseLong(id.substring(dash + 1));
            } catch (NumberFormatException nfe) {
              clientOutput.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
              clientOutput.flush();
              currentArrayCount = -1;
              continue;
            }

            String last = lastStreamId.get(key);
            if (last != null) {
              int d = last.indexOf('-');
              long lastMs = Long.parseLong(last.substring(0, d));
              long lastSeq = Long.parseLong(last.substring(d + 1));

              boolean smallerOrEqual =
                      (curMs < lastMs) ||
                              (curMs == lastMs && curSeq <= lastSeq);

              if (smallerOrEqual) {
                clientOutput.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                clientOutput.flush();
                currentArrayCount = -1;
                continue;
              }
            }
            newId = id;
          }

          // marchează tipul și salvează ultimul id
          streamKeys.add(key);
          lastStreamId.put(key, newId);

          // răspuns: ID ca bulk string
          clientOutput.write("$" + newId.length() + "\r\n" + newId + "\r\n");
          clientOutput.flush();

          currentArrayCount = -1;
        }

      }
    } catch (IOException e) {
      System.out.println("Error" + e.getMessage());
    }
  }
}
