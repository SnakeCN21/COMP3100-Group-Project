import java.net.*;
import java.io.*;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class Stage1Client {
    private Socket socket = null;
    private BufferedReader input = null;
    private DataOutputStream output = null;

    private static String submitTime = "";
    private static String jobID = "";
    private static String estimatedRuntime = "";
    private static String CPUCores = "";
    private static String memory = "";
    private static String disk = "";

    private static ArrayList<HashMap<String, String>> serverList = new ArrayList<HashMap<String, String>>();
    private static HashMap<String, String> largestServer = new HashMap<String, String>();

    private static final String SENT = "SENT";
    private static final String RCVD = "RCVD";

    private static final String HELO = "HELO";
    private static final String AUTH = "AUTH";
    private static final String REDY = "REDY";
    private static final String SCHD = "SCHD";
    private static final String RESC_ALL = "RESC All";
    private static final String QUIT = "QUIT";

    private static final String SPLIT = " ";

    private static final String OK = "OK";
    private static final String JOBN = "JOBN";
    private static final String RESF = "RESF";
    private static final String RESR = "RESR";
    private static final String NONE = "NONE";
    private static final String DOT = ".";

    private static final String SERVER_TYPE = "serverType";
    private static final String SERVER_ID = "serverID";
    private static final String SERVER_STATE = "serverState";
    private static final String AVAILABLE_TIME = "availableTime";
    private static final String CPU_CORES = "CPUCores";
    private static final String MEMORY = "memory";
    private static final String DISK_SPACE = "diskSpace";


    public Stage1Client(String address, int port) throws Exception {
        socket = new Socket(address, port);

        // receive buffer from server
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // sends output to the socket
        output = new DataOutputStream(socket.getOutputStream());
    }

    private void sendMsg(String msg) throws IOException {
        byte[] message = msg.getBytes();
        //output.write(message, 0, message.length);
        output.write(message);
        output.flush();
    }

    // receive message from server
    private String getMsg() throws Exception {
        StringBuilder msg = new StringBuilder();
        while (msg.length() < 1) {
            while (input.ready()) {
                msg.append((char) input.read());
            }
        }

        String inputMsg = msg.toString();
        return inputMsg;
    }

    private static String resc(Stage1Client client, String inMsg) throws Exception {
        String msg = "";
        String[] line = inMsg.split(SPLIT);

        submitTime = line[1];
        jobID = line[2];
        estimatedRuntime = line[3];
        CPUCores = line[4];
        memory = line[5];
        disk = line[6];

        client.sendMsg(RESC_ALL + SPLIT + CPUCores + SPLIT + memory + SPLIT + disk);
        //System.out.println(RCVD + SPLIT + RESC_ALL + SPLIT + CPUCores + SPLIT + memory + SPLIT + disk);
        msg = client.getMsg();
        //System.out.println(SENT + SPLIT + msg);

        serverList = new ArrayList<HashMap<String, String>>();

        while (!msg.equals(DOT)) {
            client.sendMsg(OK);
            //System.out.println(RCVD + SPLIT + OK);
            msg = client.getMsg();
            //System.out.println(SENT + SPLIT + msg);

            if (!msg.equals(DOT)) {
                serverList = assembleServerList(serverList, msg);
            }
        }

        return msg;
    }

    private static String schd(Stage1Client client) throws Exception {
        client.sendMsg(SCHD + SPLIT + jobID + SPLIT + largestServer.get(SERVER_TYPE) + SPLIT + largestServer.get(SERVER_ID));
        //System.out.println(RCVD + SPLIT + SCHD + SPLIT + jobID + SPLIT + largestServer.get(SERVER_TYPE) + SPLIT + largestServer.get(SERVER_ID));
        String msg = client.getMsg();
        //System.out.println(SENT + SPLIT + msg);

        return msg;
    }

    private static ArrayList<HashMap<String, String>> assembleServerList(ArrayList<HashMap<String, String>> serverList, String serverMsg) {
        String[] line = serverMsg.split(SPLIT);

        String serverType = line[0];
        String serverID = line[2];
        String serverState = line[2];
        String availableTime = line[3];
        String CPUCores = line[4];
        String memory = line[5];
        String diskSpace = line[6];

        HashMap<String, String> serverMap = new HashMap<String, String>();

        serverMap.put(SERVER_TYPE, serverType);
        serverMap.put(SERVER_ID, serverID);
        serverMap.put(SERVER_STATE, serverState);
        serverMap.put(AVAILABLE_TIME, availableTime);
        serverMap.put(CPU_CORES, CPUCores);
        serverMap.put(MEMORY, memory);
        serverMap.put(DISK_SPACE, diskSpace);

        serverList.add(serverMap);

        return serverList;
    }

    private static HashMap<String, String> allToLargest(ArrayList<HashMap<String, String>> serverList) {
        HashMap<String, String> largestServer = new HashMap<String, String>();
        largestServer = serverList.get(0);

        for (int i=1; i<serverList.size(); i++) {
            int currentLargest = Integer.parseInt(largestServer.get(CPU_CORES));
            int currentCPUCores = Integer.parseInt(serverList.get(i).get(CPU_CORES));

            if (currentLargest < currentCPUCores) {
                largestServer = serverList.get(i);
            }
        }

        return largestServer;
    }

    public static void main(String[] args) {
        try {
            System.out.println("System time: " + DateFormat.getInstance().format(System.currentTimeMillis()));

            String serverName = InetAddress.getLocalHost().getHostName();
            String name = System.getProperty("user.name");

            Stage1Client client = new Stage1Client(serverName, 50000);

            String msg = "";

            client.sendMsg(HELO);
            //System.out.println(RCVD + SPLIT + HELO);
            msg = client.getMsg();
            //System.out.println(SENT + SPLIT + msg);

            client.sendMsg(AUTH + SPLIT + name);
            //System.out.println(RCVD + SPLIT + AUTH + SPLIT + name);
            msg = client.getMsg();
            //System.out.println(SENT + SPLIT + msg);

            // read system.xml


            while (msg.equals(OK)) {
                client.sendMsg(REDY);
                //System.out.println(RCVD + SPLIT + REDY);
                msg = client.getMsg();
                //System.out.println(SENT + SPLIT + msg);

                if (msg.startsWith(JOBN)) {
                    msg = resc(client,msg);

                    largestServer = allToLargest(serverList);

                    msg = schd(client);
                } else if (msg.startsWith(RESF)) {
                    msg = schd(client);
                } else if (msg.startsWith(RESR)) {
                    //largestServer = allToLargest(serverList);

                    msg = schd(client);
                }
            }

            client.sendMsg(QUIT);
            //System.out.println(RCVD + SPLIT + QUIT);
            msg = client.getMsg();
            //System.out.println(SENT + SPLIT + msg);

            client.input.close();
            client.output.close();
            client.socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
