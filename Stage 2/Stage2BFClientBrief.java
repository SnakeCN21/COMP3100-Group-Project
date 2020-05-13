import java.net.*;
import java.io.*;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class Stage2BFClientBrief {
    private Socket socket = null;
    private BufferedReader input = null;
    private DataOutputStream output = null;

    private static XMLReader xml;

    private static String submitTime = "";
    private static String jobID = "";
    private static String estimatedRuntime = "";
    private static String CPUCores = "";
    private static String memory = "";
    private static String disk = "";

    //private static String serverID = "";
    private static int bestFit = Integer.MAX_VALUE;
    private static int minAvail = Integer.MAX_VALUE;

    private static ArrayList<HashMap<String, String>> serverList = new ArrayList<HashMap<String, String>>();
    private static HashMap<String, ArrayList<HashMap<String, String>>> serverListByType = new HashMap<String, ArrayList<HashMap<String, String>>>();
    private static HashMap<String, String> schdServer = new HashMap<String, String>();

    public Stage2BFClientBrief(String address, int port) throws Exception {
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

    private static void parsingJob(String msg) {
        String[] line = msg.split(Constant.SPLIT);

        submitTime = line[1];
        jobID = line[2];
        estimatedRuntime = line[3];
        CPUCores = line[4];
        memory = line[5];
        disk = line[6];
    }

    private static String resc(Stage2BFClientBrief client, String rescMode) throws Exception {
        String msg = "";

        client.sendMsg(rescMode + Constant.SPLIT + CPUCores + Constant.SPLIT + memory + Constant.SPLIT + disk);
        //System.out.println(Constant.RCVD + Constant.SPLIT + rescMode + Constant.SPLIT + CPUCores + Constant.SPLIT + memory + Constant.SPLIT + disk);
        msg = client.getMsg();
        //System.out.println(Constant.SENT + Constant.SPLIT + msg);

        serverList = new ArrayList<HashMap<String, String>>();
        serverListByType = new HashMap<String, ArrayList<HashMap<String, String>>>();

        while (!msg.equals(Constant.DOT)) {
            client.sendMsg(Constant.OK);
            //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.OK);
            msg = client.getMsg();
            //System.out.println(Constant.SENT + Constant.SPLIT + msg);

            if (!msg.equals(Constant.DOT)) {
                assembleServerList(msg);
            }
        }

        assembleServerListByType();

        return msg;
    }

    private static void assembleServerList(String serverMsg) {
        String[] line = serverMsg.split(Constant.SPLIT);

        String serverType = line[0];
        String serverID = line[1];
        String serverState = line[2];
        String availableTime = line[3];
        String CPUCores = line[4];
        String memory = line[5];
        String diskSpace = line[6];

        HashMap<String, String> serverMap = new HashMap<String, String>();

        serverMap.put(Constant.SERVER_TYPE, serverType);
        serverMap.put(Constant.SERVER_ID, serverID);
        serverMap.put(Constant.SERVER_STATE, serverState);
        serverMap.put(Constant.AVAILABLE_TIME, availableTime);
        serverMap.put(Constant.CPU_CORES, CPUCores);
        serverMap.put(Constant.MEMORY, memory);
        serverMap.put(Constant.DISK_SPACE, diskSpace);

        serverList.add(serverMap);
    }

    private static void assembleServerListByType() {
        for (int i=0; i<serverList.size(); i++) {
            String currentServerType = serverList.get(i).get(Constant.SERVER_TYPE);

            ArrayList<HashMap<String, String>> savedServerList = serverListByType.get(currentServerType);

            if (savedServerList == null) {
                ArrayList<HashMap<String, String>> tempServerList = new ArrayList<HashMap<String, String>>();
                tempServerList.add(serverList.get(i));
                serverListByType.put(currentServerType, tempServerList);
            } else {
                savedServerList.add(serverList.get(i));

                serverListByType.put(currentServerType, savedServerList);
            }
        }
    }

    private static void bestFit() {
        schdServer = new HashMap<String, String>();

        for (int i=0; i<xml.serverList.size(); i++) {
            HashMap<String, String> serverMap = xml.serverList.get(i);
            String currentServerType = serverMap.get(Constant.TYPE);

            ArrayList<HashMap<String, String>> typeServerList = serverListByType.get(currentServerType);

            if (typeServerList == null) {
                continue;
            }

            for (int j=0; j<typeServerList.size(); j++) {
                HashMap<String, String> server = typeServerList.get(j);

                String serverType = server.get(Constant.SERVER_TYPE);
                String serverID = server.get(Constant.SERVER_ID);
                String serverState = server.get(Constant.SERVER_STATE);
                int serverAvailableTime = Integer.parseInt(server.get(Constant.AVAILABLE_TIME));
                int serverCPUCores = Integer.parseInt(server.get(Constant.CPU_CORES));
                int serverMemory = Integer.parseInt(server.get(Constant.MEMORY));
                int serverDiskSpace = Integer.parseInt(server.get(Constant.DISK_SPACE));

                if (serverState.equals(Constant.SERVER_UNAVAILABLE)) {
                    continue;
                } else {
                    int fitness = serverCPUCores - Integer.parseInt(CPUCores);

                    if (fitness < 0) {
                        continue;
                    }

                    if (fitness < bestFit || (fitness == bestFit && serverAvailableTime < minAvail)) {
                        bestFit = fitness;
                        minAvail = serverAvailableTime;

                        schdServer = server;
                    }
                }
            }
        }

        if (schdServer.isEmpty()) {
            bestFit = Integer.MAX_VALUE;
            minAvail = Integer.MAX_VALUE;

            for (int i=0; i<serverList.size(); i++) {
                HashMap<String, String> server = serverList.get(i);

                //System.out.println("server = " + server.toString());

                String serverType = server.get(Constant.SERVER_TYPE);
                String serverID = server.get(Constant.SERVER_ID);
                String serverState = server.get(Constant.SERVER_STATE);
                int serverAvailableTime = Integer.parseInt(server.get(Constant.AVAILABLE_TIME));
                int serverCPUCores = Integer.parseInt(server.get(Constant.CPU_CORES));
                int serverMemory = Integer.parseInt(server.get(Constant.MEMORY));
                int serverDiskSpace = Integer.parseInt(server.get(Constant.DISK_SPACE));

                if (!serverState.equals(Constant.SERVER_UNAVAILABLE)) {
                    int fitness = serverCPUCores - Integer.parseInt(CPUCores);

                    //System.out.println("fitness = " + serverCPUCores + " - " + Integer.parseInt(CPUCores));

                    if (fitness < 0) {
                        continue;
                    }

                    if (fitness < bestFit || (fitness == bestFit && serverAvailableTime < minAvail)) {
                        bestFit = fitness;
                        minAvail = serverAvailableTime;

                        schdServer = server;
                    }
                }
            }
        }

        if (schdServer.isEmpty()) {
            schdServer = serverList.get(0);
        }

        //System.out.println("===================\n" + schdServer.toString());
    }

    private static HashMap<String, String> allToLargest() {
        String largestServerType = xml.largestServer.get(Constant.TYPE);

        //schdServer = new HashMap<String, String>();
        //schdServer = serverList.get(0);

        //for (int i=1; i<serverList.size(); i++) {
        for (int i=0; i<serverList.size(); i++) {
//            int currentLargest = Integer.parseInt(schdServer.get(CPU_CORES));
//            int currentCPUCores = Integer.parseInt(serverList.get(i).get(CPU_CORES));
//
//            if (currentLargest < currentCPUCores) {
//                schdServer = serverList.get(i);
//            }

            String currentServerType = serverList.get(i).get(Constant.SERVER_TYPE);

            if (largestServerType.equals(currentServerType)) {
                schdServer = serverList.get(i);

                break;
            }
        }

        return schdServer;
    }

    private static String schd(Stage2BFClientBrief client) throws Exception {
        client.sendMsg(Constant.SCHD + Constant.SPLIT + jobID + Constant.SPLIT + schdServer.get(Constant.SERVER_TYPE) + Constant.SPLIT + schdServer.get(Constant.SERVER_ID));
        //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.SCHD + Constant.SPLIT + jobID + Constant.SPLIT + schdServer.get(Constant.SERVER_TYPE) + Constant.SPLIT + schdServer.get(Constant.SERVER_ID));
        String msg = client.getMsg();
        //System.out.println(Constant.SENT + Constant.SPLIT + msg);

        return msg;
    }

    public static void main(String[] args) {
        try {
            //System.out.println("System time: " + DateFormat.getInstance().format(System.currentTimeMillis()));

            String serverName = InetAddress.getLocalHost().getHostName();
            String name = System.getProperty("user.name");

            Stage2BFClientBrief client = new Stage2BFClientBrief(serverName, 50000);

            String msg = "";

            client.sendMsg(Constant.HELO);
            //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.HELO);
            msg = client.getMsg();
            //System.out.println(Constant.SENT + Constant.SPLIT + msg);

            client.sendMsg(Constant.AUTH + Constant.SPLIT + name);
            //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.AUTH + Constant.SPLIT + name);
            msg = client.getMsg();
            //System.out.println(Constant.SENT + Constant.SPLIT + msg);

            // read system.xml
            xml = new XMLReader();

            while (msg.equals(Constant.OK)) {
                client.sendMsg(Constant.REDY);
                //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.REDY);
                msg = client.getMsg();
                //System.out.println(Constant.SENT + Constant.SPLIT + msg);

                if (msg.startsWith(Constant.JOBN)) { 
                    parsingJob(msg);

                    msg = resc(client, Constant.RESC_CAPABLE);

                    bestFit();

                    //schdServer = allToLargest();

                    msg = schd(client);
                }
//                else if (msg.startsWith(Constant.RESF)) {
//                    msg = schd(client);
//                } else if (msg.startsWith(Constant.RESR)) {
//                    //schdServer = allToLargest(serverList);
//
//                    msg = schd(client);
//                }
            }

            client.sendMsg(Constant.QUIT);
            //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.QUIT);
            msg = client.getMsg();
            //System.out.println(Constant.SENT + Constant.SPLIT + msg);

            client.input.close();
            client.output.close();
            client.socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
