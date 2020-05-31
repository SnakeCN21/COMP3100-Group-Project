import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Stage3Client {
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

    private static ArrayList<HashMap<String, String>> serverList = new ArrayList<HashMap<String, String>>();
    private static HashMap<String, String> schdServer = new HashMap<String, String>();

    private static HashMap<String, HashMap<String, String>> championServerMap = new HashMap<String, HashMap<String, String>>();

    private static HashMap<String, Integer> serverFitnessCores = new HashMap<String, Integer>();
    private static HashMap<String, Integer> serverFitnessMemory = new HashMap<String, Integer>();
    private static HashMap<String, Integer> serverFitnessDisk = new HashMap<String, Integer>();

    private static ArrayList<HashMap<String, Integer>> serverCoresRank = new ArrayList<HashMap<String, Integer>>();
    private static ArrayList<HashMap<String, Integer>> serverMemoryRank = new ArrayList<HashMap<String, Integer>>();
    private static ArrayList<HashMap<String, Integer>> serverDiskRank = new ArrayList<HashMap<String, Integer>>();

    private static ArrayList<HashMap<String, Double>> serverCoresWeight = new ArrayList<HashMap<String, Double>>();
    private static ArrayList<HashMap<String, Double>> serverMemoryWeight = new ArrayList<HashMap<String, Double>>();
    private static ArrayList<HashMap<String, Double>> serverDiskWeight = new ArrayList<HashMap<String, Double>>();

    private static ArrayList<HashMap<String, Double>> serverTotalWeight = new ArrayList<HashMap<String, Double>>();

    public Stage3Client(String address, int port) throws Exception {
        socket = new Socket(address, port);

        // Receive buffer from server
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Sends output to the socket
        output = new DataOutputStream(socket.getOutputStream());
    }

    // Send message to server
    private void sendMsg(String msg) throws IOException {
        byte[] message = msg.getBytes();

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
    private static String resc(Stage3Client client, String rescMode) throws Exception {
        String msg = "";

        client.sendMsg(rescMode + Constant.SPLIT + CPUCores + Constant.SPLIT + memory + Constant.SPLIT + disk);
        System.out.println(Constant.RCVD + Constant.SPLIT + rescMode + Constant.SPLIT + CPUCores + Constant.SPLIT + memory + Constant.SPLIT + disk);
        msg = client.getMsg();
        System.out.println(Constant.SENT + Constant.SPLIT + msg);

        serverList = new ArrayList<HashMap<String, String>>();

        while (!msg.equals(Constant.DOT)) {
            client.sendMsg(Constant.OK);
            System.out.println(Constant.RCVD + Constant.SPLIT + Constant.OK);
            msg = client.getMsg();
            System.out.println(Constant.SENT + Constant.SPLIT + msg);

            if (!msg.equals(Constant.DOT)) {
                assembleServerList(msg);
            }
        }

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

    /* Algorithms of Champion
     *  This algorithm focus on finding the best-fitness server for each job,
     *  by sequentially looking at different server states.
     *  This algorithm can adjust the Avg Turnaround Time and Avg Exec Time to improve work efficiency.
     */
    private static void champion() {
        restoreChampionParameter();

        for (int i=0; i<serverList.size(); i++) {
            HashMap<String, String> server = serverList.get(i);

            String currentServerType = server.get(Constant.SERVER_TYPE);
            String currentServerID = server.get(Constant.SERVER_ID);
            String currentServerState = server.get(Constant.SERVER_STATE);
            int currentServerAvailableTime = Integer.parseInt(server.get(Constant.AVAILABLE_TIME));
            int currentServerCPUCores = Integer.parseInt(server.get(Constant.CPU_CORES));
            int currentServerMemory = Integer.parseInt(server.get(Constant.MEMORY));
            int currentServerDiskSpace = Integer.parseInt(server.get(Constant.DISK_SPACE));

            if (currentServerState.equals(Constant.SERVER_UNAVAILABLE)) {
                continue;
            } else {
                // Calculate the fitness value
                int fitnessCores = currentServerCPUCores - Integer.parseInt(CPUCores);
                int fitnessMemory = currentServerMemory - Integer.parseInt(memory);
                int fitnessDiskSpace = currentServerDiskSpace - Integer.parseInt(disk);

                // If any resource configuration doesn't meet the job requirements, then check the next server.
                if (fitnessCores < 0 || fitnessMemory < 0 || fitnessDiskSpace < 0) {
                    continue;
                }

                String SID = currentServerType + currentServerID;
                championServerMap.put(SID, server);

                serverFitnessCores.put(SID, fitnessCores);
                serverFitnessMemory.put(SID, fitnessMemory);
                serverFitnessDisk.put(SID, fitnessDiskSpace);
            }
        }

        // If all fitness map doesn't meet the job requirements, then set the default server.
        if (serverFitnessCores.isEmpty() || serverFitnessMemory.isEmpty() || serverFitnessDisk.isEmpty()) {
            schdServer = serverList.get(0);

            return;
        } else {
            // Sort the fitness values of the 3 resources in ascending order.
            serverCoresRank = sortServerMap(serverFitnessCores);
            serverMemoryRank = sortServerMap(serverFitnessMemory);
            serverDiskRank = sortServerMap(serverFitnessDisk);

            // Calculate the respective weights of the 3 resources
            // Weight = index / list.size ()
            serverCoresWeight = weightCalculation(serverCoresRank);
            serverMemoryWeight = weightCalculation(serverMemoryRank);
            serverDiskWeight = weightCalculation(serverDiskRank);

            // Calculate all 3 resources of each server.
            serverTotalWeight = totalWeightCalculation(serverCoresWeight, serverMemoryWeight, serverDiskWeight);

            // Sort all servers in ascending order of total weight.
            serverTotalWeight = sortServerTotalWeight(serverTotalWeight);

            // Find the server whose performance is closest to the job requirements according to the total weight.
            setChampionServer(serverTotalWeight);
        }
    }

    // Reset all the variables that will be used in order to find the champion server.
    private static void restoreChampionParameter() {
        championServerMap = new HashMap<String, HashMap<String, String>>();

        serverFitnessCores = new HashMap<String, Integer>();
        serverFitnessMemory = new HashMap<String, Integer>();
        serverFitnessDisk = new HashMap<String, Integer>();

        serverCoresRank = new ArrayList<HashMap<String, Integer>>();
        serverMemoryRank = new ArrayList<HashMap<String, Integer>>();
        serverDiskRank = new ArrayList<HashMap<String, Integer>>();

        serverCoresWeight = new ArrayList<HashMap<String, Double>>();
        serverMemoryWeight = new ArrayList<HashMap<String, Double>>();
        serverDiskWeight = new ArrayList<HashMap<String, Double>>();

        serverTotalWeight = new ArrayList<HashMap<String, Double>>();

        schdServer = new HashMap<String, String>();
    }

    /* Sort the servers according to the fitness value of each resource from small to large.
     *  Map's key is SID, which is ServerType + ServerID
     *  Map's value is fitness value of specific resource.
     */
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

    /* Calculate servers respective weights of each resource
     *  The smaller the weight, the higher the priority,
     *  which means that the current resources of this server are closer to the job requirements
     *  Weight = index / list.size ()
     */
    private static ArrayList<HashMap<String, Double>> weightCalculation(ArrayList<HashMap<String, Integer>> list) {
        ArrayList<HashMap<String,Double>> result = new ArrayList<HashMap<String,Double>>();

        for (int i=0; i<list.size(); i++) {
            HashMap<String, Integer> currentMap = list.get(i);
            String SID = "";

            for (HashMap.Entry<String, Integer> currentEntry : currentMap.entrySet()) {
                SID = currentEntry.getKey();
            }

            BigDecimal b1 = new BigDecimal(Double.toString(i+1));
            BigDecimal b2 = new BigDecimal(Double.toString(list.size()));

            Double weight = b1.divide(b2, Constant.WEIGHT_SCALE, RoundingMode.HALF_UP).doubleValue();

            HashMap<String, Double> tempMap = new HashMap<String, Double>();
            tempMap.put(SID, weight);

            result.add(tempMap);
        }

        return result;
    }

    /* Calculate all 3 fitness value of each server.
     *  Add up the weights of the 3 fitness value of each server,
     *  to get the total weight of each server.
     */
    private static ArrayList<HashMap<String, Double>> totalWeightCalculation(ArrayList<HashMap<String, Double>> coresList, ArrayList<HashMap<String, Double>> memoryList, ArrayList<HashMap<String, Double>> diskList) {
        ArrayList<HashMap<String,Double>> result = new ArrayList<HashMap<String,Double>>();

        for (int i=0; i<coresList.size(); i++) {
            double totalWeight = 0;

            HashMap<String, Double> currentCoresMap = coresList.get(i);
            String SID = "";
            double currentCoresWeight = 0d;
            double currentMemoryWeight = 0d;
            double currentDiskWeight = 0d;

            for (HashMap.Entry<String, Double> currentCoresEntry : currentCoresMap.entrySet()) {
                SID = currentCoresEntry.getKey();
                currentCoresWeight = currentCoresEntry.getValue();
            }

            for (int j=0; j<memoryList.size(); j++) {
                HashMap<String, Double> currentMemoryMap = memoryList.get(j);
                String memorySID = "";

                for (HashMap.Entry<String, Double> currentMemoryEntry : currentMemoryMap.entrySet()) {
                    memorySID = currentMemoryEntry.getKey();
                    currentMemoryWeight = currentMemoryEntry.getValue();
                }

                if (SID.equals(memorySID)) {
                    BigDecimal b1 = new BigDecimal(Double.toString(currentCoresWeight));
                    BigDecimal b2 = new BigDecimal(Double.toString(currentMemoryWeight));

                    totalWeight = b1.add(b2).doubleValue();

                    break;
                }
            }

            for (int j=0; j<diskList.size(); j++) {
                HashMap<String, Double> currentDiskMap = diskList.get(j);
                String diskSID = "";

                for (HashMap.Entry<String, Double> currentDiskEntry : currentDiskMap.entrySet()) {
                    diskSID = currentDiskEntry.getKey();
                    currentDiskWeight = currentDiskEntry.getValue();
                }

                if (SID.equals(diskSID)) {
                    BigDecimal b1 = new BigDecimal(Double.toString(totalWeight));
                    BigDecimal b2 = new BigDecimal(Double.toString(currentDiskWeight));

                    totalWeight = b1.add(b2).doubleValue();

                    break;
                }
            }

            HashMap<String, Double> tempMap = new HashMap<String, Double>();
            tempMap.put(SID, totalWeight);

            result.add(tempMap);
        }

        return result;
    }

    /* Sort all servers in ascending order of total weight.
     *  Map's key is SID,
     *  Map's value is total weight.
     */
    private static ArrayList<HashMap<String, Double>> sortServerTotalWeight(ArrayList<HashMap<String, Double>> list) {
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

    /* According to the sorted list, select the server with index = 0,
     * This server is configured closest to the job requirements.
     */
    private static void setChampionServer(ArrayList<HashMap<String, Double>> list) {
        HashMap<String, Double> currentMap = list.get(0);
        String SID = "";

        for (HashMap.Entry<String, Double> currentEntry : currentMap.entrySet()) {
            SID = currentEntry.getKey();
        }

        schdServer = championServerMap.get(SID);
    }

    // Scheduling decision
    private static String schd(Stage3Client client) throws Exception {
        client.sendMsg(Constant.SCHD + Constant.SPLIT + jobID + Constant.SPLIT + schdServer.get(Constant.SERVER_TYPE) + Constant.SPLIT + schdServer.get(Constant.SERVER_ID));
        System.out.println(Constant.RCVD + Constant.SPLIT + Constant.SCHD + Constant.SPLIT + jobID + Constant.SPLIT + schdServer.get(Constant.SERVER_TYPE) + Constant.SPLIT + schdServer.get(Constant.SERVER_ID));
        String msg = client.getMsg();
        System.out.println(Constant.SENT + Constant.SPLIT + msg);

        return msg;
    }

    public static void main(String[] args) {
        try {
            String serverName = InetAddress.getLocalHost().getHostName();
            String name = System.getProperty("user.name");

            // Connect server
            Stage3Client client = new Stage3Client(serverName, 50000);

            String msg = "";

            client.sendMsg(Constant.HELO);
            System.out.println(Constant.RCVD + Constant.SPLIT + Constant.HELO);
            msg = client.getMsg();
            System.out.println(Constant.SENT + Constant.SPLIT + msg);

            client.sendMsg(Constant.AUTH + Constant.SPLIT + name);
            System.out.println(Constant.RCVD + Constant.SPLIT + Constant.AUTH + Constant.SPLIT + name);
            msg = client.getMsg();
            System.out.println(Constant.SENT + Constant.SPLIT + msg);

            // Read system.xml
            xml = new XMLReader();

            // Schedule job
            while (msg.equals(Constant.OK)) {
                client.sendMsg(Constant.REDY);
                System.out.println(Constant.RCVD + Constant.SPLIT + Constant.REDY);
                msg = client.getMsg();
                System.out.println(Constant.SENT + Constant.SPLIT + msg);

                if (msg.startsWith(Constant.JOBN)) {
                    parsingJob(msg);

                    msg = resc(client, Constant.RESC_CAPABLE);

                    champion();

                    msg = schd(client);
                }
            }

            // QUIT
            client.sendMsg(Constant.QUIT);
            System.out.println(Constant.RCVD + Constant.SPLIT + Constant.QUIT);
            msg = client.getMsg();
            System.out.println(Constant.SENT + Constant.SPLIT + msg);

            client.input.close();
            client.output.close();
            client.socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
