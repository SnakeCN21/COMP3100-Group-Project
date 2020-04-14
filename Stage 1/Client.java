import java.net.*;
import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.*;
import java.util.*;

public class Client {

  private Socket socket = null;
  private BufferedReader input = null;
  private DataOutputStream output = null;
  private String largestServer = null;

  // system commands
  public static final String HELO = "HELO";
  public static final String AUTH = "AUTH ";
  public static final String REDY = "REDY";
  public static final String SCHD = "SCHD";
  public static final String OK = "OK";
  public static final String QUIT = "QUIT";
  public static final String JOBN = "JOBN";

  public Client(String address, int port) throws Exception {
    socket = new Socket(address, port);
    System.out.println("Connected");
    // reads input from the socket input stream
    input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

    // sends output to the socket output stream
    output = new DataOutputStream(socket.getOutputStream());

  }

  void message(String msg) throws IOException {
    byte[] message = msg.getBytes();
    output.write(message, 0, message.length);
    output.flush();
  }

  // receive message from server
  String getFromServer() throws Exception {
    StringBuilder msg = new StringBuilder();
    while (msg.length() < 1) {
      while (input.ready()) {
        msg.append((char) input.read());
      }
    }
    String inputMsg = msg.toString();
    return inputMsg;
  }

  void getLargest() {
    String largestType = null;
    try {
      File inputFile = new File("system.xml");
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(inputFile);
      doc.getDocumentElement().normalize();
      NodeList serverList = doc.getElementsByTagName("server");
      Node tempNode = serverList.item(0);
      Element tempElement = (Element) tempNode;
      int largestCore = Integer.parseInt(tempElement.getAttribute("coreCount"));

      // looping through the list

      for (int temp = 0; temp < serverList.getLength(); temp++) {
        Node currentNode = serverList.item(temp);
        if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
          Element currentElement = (Element) currentNode;
          int coreCount = Integer.parseInt(currentElement.getAttribute("coreCount"));
          if (coreCount > largestCore) {
            largestCore = coreCount;
            largestType = currentElement.getAttribute("type");
          }
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    largestServer = largestType;
  }

  public static void main(String[] args) {
    try {

      String name = System.getProperty("user.name");

      Client client = new Client("127.0.0.1", 50000);

      client.message(HELO);
      client.getFromServer();

      client.message(AUTH + name);
      client.getFromServer();

      client.message(REDY);
      String msg = client.getFromServer();
      client.getLargest();

      // Requesting for all server information
      while (!msg.equals("NONE")) {
        String[] line = msg.split(" ");
        String request = ("RESC All " + line[4] + " " + line[5] + " " + line[6]);
        client.message(request);

        if (client.getFromServer().equals("DATA")) {
          String temp = "";
          client.message(OK);

          while (!temp.equals(".")) {
            if (!temp.equals(".")) {
              client.message(OK);
              temp = client.getFromServer();
            }
          }
          if (client.largestServer != null) {
            client.message(SCHD + " " + line[2] + " " + client.largestServer + " " + "0");
          }
        }
        client.getFromServer();
        client.message(REDY);
        msg = client.getFromServer();
      }
      client.message(QUIT);
      client.input.close();
      client.output.close();
      client.socket.close();

    } catch (Exception e) {
      // System.out.println(e);
    }
  }
}