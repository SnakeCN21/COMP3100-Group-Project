import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Stage3ClientBrief {
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

//    private static String serverID = "";
//    private static int bestFit = Integer.MAX_VALUE;
//    private static int minAvail = Integer.MAX_VALUE;

    private static ArrayList<HashMap<String, String>> serverList = new ArrayList<HashMap<String, String>>();
    private static HashMap<String, ArrayList<HashMap<String, String>>> serverListByType = new HashMap<String, ArrayList<HashMap<String, String>>>();
    private static HashMap<String, String> schdServer = new HashMap<String, String>();

    private static HashMap<String, HashMap<String, String>> championServerMap = new HashMap<String, HashMap<String, String>>();
//    private static HashMap<String, Integer> serverFitnessCores = new HashMap<String, Integer>();
//    private static HashMap<String, Integer> serverFitnessMemory = new HashMap<String, Integer>();
//    private static HashMap<String, Integer> serverFitnessDisk = new HashMap<String, Integer>();
//    private static Double tempServerTotalWeights = 1d;

    public Stage3ClientBrief(String address, int port) throws Exception {
        socket = new Socket(address, port);

        // Receive buffer from server
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Sends output to the socket
        output = new DataOutputStream(socket.getOutputStream());
    }

    // Send message to server
    private void sendMsg(String msg) throws IOException {
        byte[] message = msg.getBytes();
        //output.write(message, 0, message.length);
        output.write(message);
        output.flush();
    }

    // Receive message from server
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

    // Parsing job details from server
    private static void parsingJob(String msg) {
        String[] line = msg.split(Constant.SPLIT);

        submitTime = line[1];
        jobID = line[2];
        estimatedRuntime = line[3];
        CPUCores = line[4];
        memory = line[5];
        disk = line[6];
    }

    // Resource information request
    private static String resc(Stage3ClientBrief client, String rescMode) throws Exception {
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
//                if (jobID.equals("619")) {
//                    System.out.println(msg);
//                }
//
//                if (jobID.equals("620")) {
//                    return "";
//                }

                assembleServerList(msg);
            }
        }

        //assembleServerListByType();

        return msg;
    }

    /* Parsing every server's message, and put in an ArrayListList<HashMap>
     *  Map's key is server's attr,
     *  Map's value is server's attr value.
     */
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

    /* Reassemble server list by server type, and put in an HashMap<String, ArrayList>
     *  Map's key is server's type,
     *  Map's value is server list of this type.
     */
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

    // Algorithms of Champion
    private static void champion() {
        //schdServer = new HashMap<String, String>();

        //restoreFitnessValue();
        restoreChampionParameter();

        HashMap<String, Integer> serverFitnessCores = new HashMap<String, Integer>();
        HashMap<String, Integer> serverFitnessMemory = new HashMap<String, Integer>();
        HashMap<String, Integer> serverFitnessDisk = new HashMap<String, Integer>();

        // For each server type i, s i , in the order appear in system.xml
//        for (int i=0; i<xml.serverList.size(); i++) {
//            HashMap<String, String> serverMap = xml.serverList.get(i);
//            String currentServerType = serverMap.get(Constant.TYPE);
//
//            ArrayList<HashMap<String, String>> typeServerList = serverListByType.get(currentServerType);
//
//            if (typeServerList == null) {
//                continue;
//            }
//
//            // For each server j, s i,j of server type s i , from 0 to limit - 1
//            for (int j=0; j<typeServerList.size(); j++) {
//                HashMap<String, String> server = typeServerList.get(j);
//
//                String serverType = server.get(Constant.SERVER_TYPE);
//                String serverID = server.get(Constant.SERVER_ID);
//                String serverState = server.get(Constant.SERVER_STATE);
//                int serverAvailableTime = Integer.parseInt(server.get(Constant.AVAILABLE_TIME));
//                int serverCPUCores = Integer.parseInt(server.get(Constant.CPU_CORES));
//                int serverMemory = Integer.parseInt(server.get(Constant.MEMORY));
//                int serverDiskSpace = Integer.parseInt(server.get(Constant.DISK_SPACE));
//
//                if (serverState.equals(Constant.SERVER_UNAVAILABLE)) {
//                    continue;
//                } else {
//                    // Calculate the fitness value fs i,j ,j i
//                    int fitnessCores = serverCPUCores - Integer.parseInt(CPUCores);
//                    int fitnessMemory = serverMemory - Integer.parseInt(memory);
//                    int fitnessDiskSpace = serverDiskSpace - Integer.parseInt(disk);
//
////                    if (jobID.equals("619")) {
////                        System.out.println(serverType + " # " + serverID);
////                        System.out.println("fitnessCores = " + fitnessCores);
////                        System.out.println("fitnessMemory = " + fitnessMemory);
////                        System.out.println("fitnessDiskSpace = " + fitnessDiskSpace);
////
////                        System.out.println("===============");
////                    }
////
////                    if (fitnessCores<0 || fitnessMemory<0 || fitnessDiskSpace<0) {
////                        System.out.println(serverType + " # " + serverID + " not passed");
////                        continue;
////                    } else {
////                        System.out.println(serverType + " # " + serverID + " passed");
////                    }
//
//                    if (fitnessCores<0 || fitnessMemory<0 || fitnessDiskSpace<0) {
//                        continue;
//                    }
//
//                    String SID = serverType + serverID;
//                    championServerMap.put(SID, server);
//
//                    serverFitnessCores.put(SID, fitnessCores);
//                    serverFitnessMemory.put(SID, fitnessMemory);
//                    serverFitnessDisk.put(SID, fitnessDiskSpace);
//
//                    ArrayList<HashMap<String, Integer>> serverCoresRank = sortServerMap(serverFitnessCores);
//                    ArrayList<HashMap<String, Integer>> serverMemoryRank = sortServerMap(serverFitnessMemory);
//                    ArrayList<HashMap<String, Integer>> serverDiskRank = sortServerMap(serverFitnessDisk);
//
//                    ArrayList<HashMap<String, Double>> serverCoresWeights = weightsCalculation(serverCoresRank);
//                    ArrayList<HashMap<String, Double>> serverMemoryWeights = weightsCalculation(serverMemoryRank);
//                    ArrayList<HashMap<String, Double>> serverDiskWeights = weightsCalculation(serverDiskRank);
//
//                    ArrayList<HashMap<String, Double>> serverTotalWeights = totalWeightsCalculation(serverCoresWeights, serverMemoryWeights, serverDiskWeights);
//
//                    serverTotalWeights = sortServerTotalWeights(serverTotalWeights);
//
//                    setChampionServer(serverTotalWeights);
//                }
//            }
//        }

        for (int i=0; i<serverList.size(); i++) {
            HashMap<String, String> server = serverList.get(i);

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
                // Calculate the fitness value fs i,j ,j i
                int fitnessCores = serverCPUCores - Integer.parseInt(CPUCores);
                int fitnessMemory = serverMemory - Integer.parseInt(memory);
                int fitnessDiskSpace = serverDiskSpace - Integer.parseInt(disk);

//                    if (jobID.equals("619")) {
//                        System.out.println(serverType + " # " + serverID);
//                        System.out.println("fitnessCores = " + fitnessCores);
//                        System.out.println("fitnessMemory = " + fitnessMemory);
//                        System.out.println("fitnessDiskSpace = " + fitnessDiskSpace);
//
//                        System.out.println("===============");
//                    }
//
//                    if (fitnessCores<0 || fitnessMemory<0 || fitnessDiskSpace<0) {
//                        System.out.println(serverType + " # " + serverID + " not passed");
//                        continue;
//                    } else {
//                        System.out.println(serverType + " # " + serverID + " passed");
//                    }

                if (fitnessCores < 0 || fitnessMemory < 0 || fitnessDiskSpace < 0) {
                    continue;
                }

                String SID = serverType + serverID;
                championServerMap.put(SID, server);

                serverFitnessCores.put(SID, fitnessCores);
                serverFitnessMemory.put(SID, fitnessMemory);
                serverFitnessDisk.put(SID, fitnessDiskSpace);
            }
        }

        if (serverFitnessCores.isEmpty() || serverFitnessMemory.isEmpty() || serverFitnessDisk.isEmpty()) {
            schdServer = serverList.get(0);
        } else {
            ArrayList<HashMap<String, Integer>> serverCoresRank = sortServerMap(serverFitnessCores);
            ArrayList<HashMap<String, Integer>> serverMemoryRank = sortServerMap(serverFitnessMemory);
            ArrayList<HashMap<String, Integer>> serverDiskRank = sortServerMap(serverFitnessDisk);

            ArrayList<HashMap<String, Double>> serverCoresWeights = weightsCalculation(serverCoresRank);
            ArrayList<HashMap<String, Double>> serverMemoryWeights = weightsCalculation(serverMemoryRank);
            ArrayList<HashMap<String, Double>> serverDiskWeights = weightsCalculation(serverDiskRank);

            ArrayList<HashMap<String, Double>> serverTotalWeights = totalWeightsCalculation(serverCoresWeights, serverMemoryWeights, serverDiskWeights);

            serverTotalWeights = sortServerTotalWeights(serverTotalWeights);

            setChampionServer(serverTotalWeights);
        }

        /* If best is still not found, because all servers are busy, then,
         *  Return first server of server list
         *  First appears in config_simple3.xml job 69
         */
//        if (schdServer.isEmpty()) {
//            schdServer = serverList.get(0);
//        }

        //System.out.println("Condition 3, schdServer = " + schdServer.toString());
    }

//    private static void restoreFitnessValue() {
//        bestFit = Integer.MAX_VALUE;
//        minAvail = Integer.MAX_VALUE;
//    }

    private static void restoreChampionParameter() {
        schdServer = new HashMap<String, String>();
        championServerMap = new HashMap<String, HashMap<String, String>>();
        //tempServerTotalWeights = 1d;
    }

    private static ArrayList<HashMap<String, Integer>> sortServerMap(HashMap<String, Integer> map) {
        ArrayList<HashMap<String,Integer>> result = new ArrayList<HashMap<String,Integer>>();

        for (HashMap.Entry<String, Integer> currentEntry : map.entrySet()) {
            HashMap<String, Integer> currentMap = new HashMap<String, Integer>();
            currentMap.put(currentEntry.getKey(), currentEntry.getValue());

            result.add(currentMap);
        }

        for (int i=0; i<result.size(); i++) {
            for (int j=0; j<result.size()-1; j++) {
                HashMap<String, Integer> currentMap = result.get(i);
                int currentValue = 0;

                for (HashMap.Entry<String, Integer> currentEntry : currentMap.entrySet()) {
                    currentValue = currentEntry.getValue();
                }

                HashMap<String, Integer> compareMap = result.get(j);
                int compareValue = 0;

                for (HashMap.Entry<String, Integer> compareEntry : compareMap.entrySet()) {
                    compareValue = compareEntry.getValue();
                }

                if (currentValue < compareValue) {
                    result.set(i, compareMap);
                    result.set(j, currentMap);
                }
            }
        }

        return result;
    }

    private static ArrayList<HashMap<String, Double>> weightsCalculation(ArrayList<HashMap<String, Integer>> list) {
        ArrayList<HashMap<String,Double>> result = new ArrayList<HashMap<String,Double>>();

        for (int i=0; i<list.size(); i++) {
            HashMap<String, Integer> currentMap = list.get(i);
            String SID = "";
            //int currentValue = 0;

            for (HashMap.Entry<String, Integer> currentEntry : currentMap.entrySet()) {
                SID = currentEntry.getKey();
                //currentValue = currentEntry.getValue();
            }

            //BigDecimal b1 = new BigDecimal(Double.toString(currentValue));
            BigDecimal b1 = new BigDecimal(Double.toString(i+1));
            BigDecimal b2 = new BigDecimal(Double.toString(list.size()));

            Double weights = b1.divide(b2, Constant.WEIGHTS_SCALE, RoundingMode.HALF_UP).doubleValue();

            HashMap<String, Double> tempMap = new HashMap<String, Double>();
            tempMap.put(SID, weights);

            result.add(tempMap);
        }

        return result;
    }

    private static ArrayList<HashMap<String, Double>> totalWeightsCalculation(ArrayList<HashMap<String, Double>> coresList, ArrayList<HashMap<String, Double>> memoryList, ArrayList<HashMap<String, Double>> diskList) {
        ArrayList<HashMap<String,Double>> result = new ArrayList<HashMap<String,Double>>();

        for (int i=0; i<coresList.size(); i++) {
            double totalWeights = 0;

            HashMap<String, Double> currentCoresMap = coresList.get(i);
            String SID = "";
            double currentCoresWeights = 0d;
            double currentMemoryWeights = 0d;
            double currentDiskWeights = 0d;

            for (HashMap.Entry<String, Double> currentCoresEntry : currentCoresMap.entrySet()) {
                SID = currentCoresEntry.getKey();
                currentCoresWeights = currentCoresEntry.getValue();
            }

            for (int j=0; j<memoryList.size(); j++) {
                HashMap<String, Double> currentMemoryMap = memoryList.get(j);
                String memorySID = "";

                for (HashMap.Entry<String, Double> currentMemoryEntry : currentMemoryMap.entrySet()) {
                    memorySID = currentMemoryEntry.getKey();
                    currentMemoryWeights = currentMemoryEntry.getValue();
                }

                if (SID.equals(memorySID)) {
                    BigDecimal b1 = new BigDecimal(Double.toString(currentCoresWeights));
                    BigDecimal b2 = new BigDecimal(Double.toString(currentMemoryWeights));

                    totalWeights = b1.add(b2).doubleValue();

                    break;
                }
            }

            for (int j=0; j<diskList.size(); j++) {
                HashMap<String, Double> currentDiskMap = diskList.get(j);
                String diskSID = "";

                for (HashMap.Entry<String, Double> currentDiskEntry : currentDiskMap.entrySet()) {
                    diskSID = currentDiskEntry.getKey();
                    currentDiskWeights = currentDiskEntry.getValue();
                }

                if (SID.equals(diskSID)) {
                    BigDecimal b1 = new BigDecimal(Double.toString(totalWeights));
                    BigDecimal b2 = new BigDecimal(Double.toString(currentDiskWeights));

                    totalWeights = b1.add(b2).doubleValue();

                    break;
                }
            }

            HashMap<String, Double> tempMap = new HashMap<String, Double>();
            tempMap.put(SID, totalWeights);

            result.add(tempMap);
        }

        return result;
    }

    private static ArrayList<HashMap<String, Double>> sortServerTotalWeights(ArrayList<HashMap<String, Double>> list) {
        for (int i=0; i<list.size(); i++) {
            for (int j=0; j<list.size()-1; j++) {
                HashMap<String, Double> currentMap = list.get(i);
                double currentValue = 0d;

                for (HashMap.Entry<String, Double> currentEntry : currentMap.entrySet()) {
                    currentValue = currentEntry.getValue();
                }

                HashMap<String, Double> compareMap = list.get(j);
                double compareValue = 0d;

                for (HashMap.Entry<String, Double> compareEntry : compareMap.entrySet()) {
                    compareValue = compareEntry.getValue();
                }

                if (currentValue < compareValue) {
                    list.set(i, compareMap);
                    list.set(j, currentMap);
                }
            }
        }

        return list;
    }

    private static void setChampionServer(ArrayList<HashMap<String, Double>> list) {
        HashMap<String, Double> currentMap = list.get(0);
        String SID = "";
        //Double currentServerTotalWeightsValue = 1d;

        for (HashMap.Entry<String, Double> currentEntry : currentMap.entrySet()) {
            SID = currentEntry.getKey();
            //currentServerTotalWeightsValue = currentEntry.getValue();
        }

//        if (currentServerTotalWeightsValue < tempServerTotalWeights) {
//            tempServerTotalWeights = currentServerTotalWeightsValue;
//            schdServer = championServerMap.get(SID);
//        }

        schdServer = championServerMap.get(SID);
    }

    // Find the largest server id's hashmap of largest server type
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

    // Scheduling decision
    private static String schd(Stage3ClientBrief client) throws Exception {
        client.sendMsg(Constant.SCHD + Constant.SPLIT + jobID + Constant.SPLIT + schdServer.get(Constant.SERVER_TYPE) + Constant.SPLIT + schdServer.get(Constant.SERVER_ID));
        //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.SCHD + Constant.SPLIT + jobID + Constant.SPLIT + schdServer.get(Constant.SERVER_TYPE) + Constant.SPLIT + schdServer.get(Constant.SERVER_ID));
        String msg = client.getMsg();
        //System.out.println(Constant.SENT + Constant.SPLIT + msg);

        return msg;
    }

    public static void main(String[] args) {
        try {
            String serverName = InetAddress.getLocalHost().getHostName();
            String name = System.getProperty("user.name");

            // Connect server
            Stage3ClientBrief client = new Stage3ClientBrief(serverName, 50000);

            String msg = "";

            client.sendMsg(Constant.HELO);
            //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.HELO);
            msg = client.getMsg();
            //System.out.println(Constant.SENT + Constant.SPLIT + msg);

            client.sendMsg(Constant.AUTH + Constant.SPLIT + name);
            //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.AUTH + Constant.SPLIT + name);
            msg = client.getMsg();
            //System.out.println(Constant.SENT + Constant.SPLIT + msg);

            // Read system.xml
            xml = new XMLReader();

            // Schedule job
            while (msg.equals(Constant.OK)) {
                client.sendMsg(Constant.REDY);
                //System.out.println(Constant.RCVD + Constant.SPLIT + Constant.REDY);
                msg = client.getMsg();
                //System.out.println(Constant.SENT + Constant.SPLIT + msg);

                if (msg.startsWith(Constant.JOBN)) {
                    parsingJob(msg);

                    msg = resc(client, Constant.RESC_CAPABLE);

                    champion();

                    msg = schd(client);
                }
            }

            // QUIT
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
