package com.company;

import java.net.*;
import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.*;

public class Stage1Client {
    private Socket socket = null;
    private BufferedReader input = null;
    private DataOutputStream output = null;
    //private String largestServer = null;

    public static final String HELO = "HELO";
    public static final String AUTH = "AUTH ";
    public static final String REDY = "REDY";
    public static final String SCHD = "SCHD";
    public static final String LARGE_0 = " large 0";
    public static final String OK = "OK";
    public static final String QUIT = "QUIT";

    public static final String SPLIT = " ";

    public static final String JOBN = "JOBN";


    public Stage1Client(String address, int port) throws Exception {
        socket = new Socket(address, port);
        //System.out.println("Connected");

        // receive buffer from server
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // sends output to the socket
        output = new DataOutputStream(socket.getOutputStream());
    }

    void sendMsg(String msg) throws IOException {
        byte[] message = msg.getBytes();
        output.write(message, 0, message.length);
        output.flush();
    }

//    private void writeIntoTheStream(Socket socket, String msg) throws IOException {
//        OutputStream out = socket.getOutputStream();
//        out.write(msg.getBytes());// string, byte[]
//        out.flush();
//    }

    //receive message from server
    String getMsg() throws Exception {
        StringBuilder msg = new StringBuilder();
        while (msg.length() < 1) {
            while (input.ready()) {
                msg.append((char) input.read());
            }
        }

        String inputMsg = msg.toString();
        return inputMsg;
    }

    //byte[] readMsg = new byte[1024];
//    private String readFromTheStream(Socket socket) throws IOException {
//        DataInputStream in = new DataInputStream(socket.getInputStream());
//        //the string sent by the server
//        //byte[] readMsg// String readMsg
//        //in.read(readMsg);
//        String msg = in.readUTF();
//
//        return msg;
//    }

//    void getLargest() {
//        String largestType = null;
//
//        try {
//            File inputFile = new File("./system.xml");
//            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//            Document doc = dBuilder.parse(inputFile);
//            doc.getDocumentElement().normalize();
//
//            //NodeList serverList = doc.getElementsByTagName("server");
//            NodeList serverList = doc.getElementsByTagName("config");
//            Node tempNode = serverList.item(0);
//            Element tempElement = (Element) tempNode;
//            int largestCore = Integer.parseInt(tempElement.getAttribute("coreCount"));
//
//            // looping through the list
//
//            for (int temp = 0; temp < serverList.getLength(); temp++) {
//                Node currentNode = serverList.item(temp);
//                if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
//                    Element currentElement = (Element) currentNode;
//                    int coreCount = Integer.parseInt(currentElement.getAttribute("coreCount"));
//                    if (coreCount > largestCore) {
//                        largestCore = coreCount;
//                        largestType = currentElement.getAttribute("type");
//                    }
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        largestServer = largestType;
//    }

    public static void main(String[] args) {
        try {
            String serverName = InetAddress.getLocalHost().getHostName();
            String name = System.getProperty("user.name");

            //Stage1Client client = new Stage1Client("127.0.0.1", 50000);
            Stage1Client client = new Stage1Client(serverName, 50000);

            client.sendMsg(HELO);
            client.getMsg();

            client.sendMsg(AUTH + name);
            client.getMsg();

            client.sendMsg(REDY);
            String msg = client.getMsg();

            //client.getLargest();

            // Requesting for all server information
            //while (!msg.equals("NONE")) {
            while (msg.equals(JOBN)) {
                String[] line = msg.split(SPLIT);

                if (line[0].equals(JOBN)) {
                    client.sendMsg(SCHD + line[2] + LARGE_0);
                    client.getMsg();

                    client.sendMsg(OK);

                    client.sendMsg(REDY);
                    msg = client.getMsg();

                    continue;
                } else {
                    client.sendMsg(QUIT);
                    client.getMsg();

                    break;
                }
//                String request = ("RESC All " + line[4] + " " + line[5] + " " + line[6]);
//                client.sendMsg(request);
//
//                if (client.getMsg().equals("DATA")) {
//                    String temp = "";
//                    client.sendMsg("OK");
//
//
//                    while (!temp.equals(".")) {
//                        if (!temp.equals(".")) {
//                            client.sendMsg("OK");
//                            temp = client.getMsg();
//                        }
//                    }
//                    if (client.largestServer != null) {
//                        client.sendMsg("SCHD" + " " + line[2] + " " + client.largestServer + " " + "0");
//                    }
//                }
//                client.getMsg();
//                client.sendMsg("REDY");
//                msg = client.getMsg();
            }

            //client.sendMsg("QUIT");
            client.input.close();
            client.output.close();
            client.socket.close();

        } catch (Exception e) {
            //System.out.println(e);
        }
    }
}