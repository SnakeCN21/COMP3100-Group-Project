// Stage 2 First Fit Algorithm
// Student Name : Jiahui Lin
// Student No : 45141916
import java.net.*;
import java.io.*;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Scanner;

public class Stage3Client {
    private Socket socket = null;
    private BufferedReader input = null;
    private DataOutputStream output = null;

    private static String submitTime = "";
    private static String jobID = "";
    private static String estimatedRuntime = "";
    private static String CPUCores = "";
    private static String memory = "";
    private static String disk = "";
    private static String serverState = "";
    
    private static ArrayList < HashMap < String, String >> serverList = new ArrayList < HashMap < String, String >> ();
    private static HashMap < String, String > largestServer = new HashMap < String, String > ();
    private static HashMap < String, String > availableFitServer = new HashMap < String, String > ();
    private static final String SENT = "SENT";
    private static final String RCVD = "RCVD";

    private static final String HELO = "HELO";
    private static final String AUTH = "AUTH";
    private static final String REDY = "REDY";
    private static final String SCHD = "SCHD";
    private static final String RESC_ALL = "RESC Capable";
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

    // Resource information request
    private static String resc(Stage3Client client, String inMsg) throws Exception {
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

        serverList = new ArrayList < HashMap < String, String >> ();

        while (!msg.equals(DOT)) {
            client.sendMsg(OK);
         
            msg = client.getMsg();
           // store the initial server list
           
            if (!msg.equals(DOT)) {
                serverList = assembleServerList(serverList, msg);
            }
        }

        return msg;
    }

    /* Parse every server's message, and put in an ArrayListList<HashMap>
     *  Map's key is server's attr,
     *  Map's value is server's attr value.
     */
    private static ArrayList < HashMap < String, String >> assembleServerList(ArrayList < HashMap < String, String >> serverList, String serverMsg) {
        String[] line = serverMsg.split(SPLIT);

        String serverType = line[0];
        String serverID = line[1];
        String serverState = line[2];
        String availableTime = line[3];
        String CPUCores = line[4];
        String memory = line[5];
        String diskSpace = line[6];

        HashMap < String, String > serverMap = new HashMap < String, String > ();

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

    // Find the largest server id's hashmap of largest server type
    private static HashMap < String, String > allToLargest(ArrayList < HashMap < String, String >> serverList, XMLReader xml) {
        String largestServerType = xml.largestServer.get(xml.TYPE);

        largestServer = new HashMap < String, String > ();
        //largestServer = serverList.get(0);

        //for (int i=1; i<serverList.size(); i++) {
        for (int i = 0; i < serverList.size(); i++) {
            String currentServerType = serverList.get(i).get(SERVER_TYPE);
            if (largestServerType.equals(currentServerType)) {
                largestServer = serverList.get(i);

                break;
            }
        }

        return largestServer;
    }
    private static HashMap<String, String> availableFit(ArrayList<HashMap<String, String>> serverList,Stage3Client client, XMLReader xml,int cpuCores,int memory,int disk)throws Exception {
        // sort the server into three different groups
        HashMap < String, String > availableFitServer = new HashMap < String, String > ();
        HashMap < String, String > activeServer = new HashMap < String, String > ();
        HashMap < String, String > idleServer = new HashMap < String, String > ();
        ArrayList<HashMap<String, String>> alternativeServers = new ArrayList<HashMap<String, String>>();
        
        int bootingTime = Integer.MAX_VALUE;
        

        
        for (int i = 0; i< serverList.size();i++) {
             int serverCoreSize = Integer.parseInt(serverList.get(i).get(CPU_CORES));
              int serverDisk = Integer.parseInt(serverList.get(i).get(DISK_SPACE));
              int serverMemory = Integer.parseInt(serverList.get(i).get(MEMORY));
              int serverState = Integer.parseInt(serverList.get(i).get(SERVER_STATE));
              int tempTime = Integer.parseInt(serverList.get(i).get(AVAILABLE_TIME));
             
             // looping through the list trying to find idle server with least waiting time
              if(serverState !=3 && isFitted(serverList.get(i),cpuCores,memory,disk)&& (tempTime< bootingTime ||(tempTime== bootingTime && idleServer!=null && isGreater(idleServer,serverList.get(i))))) {
                  idleServer = serverList.get(i);
                  bootingTime=tempTime;
             
                  
              }else if(serverState ==3  &&activeServer.get(MEMORY)==null&&isFitted(serverList.get(i),cpuCores,memory,disk)) {
     
                 activeServer = serverList.get(i);
                 
              }else if(!isFitted(serverList.get(i),cpuCores,memory,disk)) {
        
                alternativeServers.add(serverList.get(i));
              }
          }
       
        
        if(activeServer.get(MEMORY)!=null){
            availableFitServer = activeServer;
        }else if(idleServer.get(MEMORY)!=null){
             availableFitServer = idleServer;
        }else{
            availableFitServer= getLeastWaitingServer(client,alternativeServers);
    }
    
    return  availableFitServer;
    }          
    
 private static HashMap<String, String> getLeastWaitingServer(Stage3Client client,ArrayList<HashMap<String, String>> serverList) throws Exception{
       
       
       //HashMap < String, String > serverMap = new HashMap < String, String > ();
         int minExecutionTime = Integer.MAX_VALUE;
         HashMap<String, String> leastWaitingServer = new HashMap<String, String>();
    
        for(int i = 0; i< serverList.size();i++) {
    
            int temp = 0;
            client.sendMsg("LSTJ "+serverList.get(i).get(SERVER_TYPE)+" "+serverList.get(i).get(SERVER_ID));
             String msg = client.getMsg();
                 while(!msg.equals(DOT)){
                    
                     client.sendMsg(OK);
                     msg = client.getMsg();
                     temp += getExecutionTime(msg);     
                 }
                if(temp!=0 && temp<minExecutionTime) {
                    minExecutionTime = temp;
                    leastWaitingServer = serverList.get(i);
                }         
        }
    
        return leastWaitingServer;
    }

 private static int getExecutionTime(String serverMsg) {
        HashMap < String, String > serverMap = new HashMap < String, String > ();
        int executionTime = Integer.MAX_VALUE;
        String line[] = serverMsg.split(SPLIT);
        if(line.length == 7){
            String jobState = line[1];
            String estimatedRunTime = line[3];
            executionTime = Integer.parseInt(estimatedRunTime);
        }
        return executionTime;
    }


    
    private static boolean isFitted(HashMap<String, String> server,int cpuCore,int memory, int disk) {
        int serverCpu = Integer.parseInt(server.get(CPU_CORES));
        int serverMemory = Integer.parseInt(server.get(MEMORY));
        int serverDisk = Integer.parseInt(server.get(DISK_SPACE));
        
        if(serverCpu>=cpuCore && serverMemory >= memory && serverDisk >=disk ) {
            return true;
        }else {
            return false;
        }
    }
    private static boolean isGreater(HashMap<String, String> server,HashMap<String, String> currentServer) {
        int serverCpu = Integer.parseInt(server.get(CPU_CORES));
        int serverMemory = Integer.parseInt(server.get(MEMORY));
        int serverDisk = Integer.parseInt(server.get(DISK_SPACE));
        
        int currentServerCpu = Integer.parseInt(server.get(CPU_CORES));
        int currentServerMemory = Integer.parseInt(server.get(MEMORY));
        int currentServerDisk = Integer.parseInt(server.get(DISK_SPACE));
        
        if(serverCpu< currentServerCpu && serverMemory < currentServerMemory && serverDisk<currentServerDisk ) {
            return true;
        }else {
            return false;
        }
    }
   

    // Scheduling decision
    private static String schd(Stage3Client client) throws Exception {
        client.sendMsg(SCHD + SPLIT + jobID + SPLIT + largestServer.get(SERVER_TYPE) + SPLIT + largestServer.get(SERVER_ID));
        //System.out.println(RCVD + SPLIT + SCHD + SPLIT + jobID + SPLIT + largestServer.get(SERVER_TYPE) + SPLIT + largestServer.get(SERVER_ID));
        String msg = client.getMsg();
        //System.out.println(SENT + SPLIT + msg);

        return msg;
    }
    private static String schd_af(Stage3Client client) throws Exception {
        client.sendMsg(SCHD + SPLIT + jobID + SPLIT + availableFitServer.get(SERVER_TYPE) + SPLIT + availableFitServer.get(SERVER_ID));
        //System.out.println(RCVD + SPLIT + SCHD + SPLIT + jobID + SPLIT + largestServer.get(SERVER_TYPE) + SPLIT + largestServer.get(SERVER_ID));
        String msg = client.getMsg();
        //System.out.println(SENT + SPLIT + msg);

        return msg;
    }

    public static void main(String[] args) {
        try {
            System.out.println("System time: " + DateFormat.getInstance().format(System.currentTimeMillis()));

            String serverName = InetAddress.getLocalHost().getHostName();
            String name = System.getProperty("user.name");
            Scanner in = new Scanner(System.in);
            String algorithm = "";
             if (args.length != 0) {
        if(args[0].equals("-a")){
          System.out.println(args[1]);
                      algorithm = args[1];
        }
            }
        
            // Connect server
            Stage3Client client = new Stage3Client(serverName, 50000);

            String msg = "";

            client.sendMsg(HELO);
            //System.out.println(RCVD + SPLIT + HELO);
            msg = client.getMsg();
            //System.out.println(SENT + SPLIT + msg);

            client.sendMsg(AUTH + SPLIT + name);
            //System.out.println(RCVD + SPLIT + AUTH + SPLIT + name);
            msg = client.getMsg();
            //System.out.println(SENT + SPLIT + msg);

            // Read system.xml
            XMLReader xml = new XMLReader();

            // Schedule job


    while (msg.equals(OK)) {

        client.sendMsg(REDY);
        //System.out.println(RCVD + SPLIT + REDY);
        msg = client.getMsg();
        //System.out.println(SENT + SPLIT + msg);
        if (msg.startsWith(JOBN)) {
            String line[] = msg.split(SPLIT);
            CPUCores = line[4];
            memory = line[5];
            disk = line[6];
            
            // Resc All
            msg = resc(client, msg);
            
            if (algorithm.equals("af")) {
               availableFitServer = availableFit(serverList,client,xml, Integer.parseInt(CPUCores),Integer.parseInt(memory),Integer.parseInt(disk));
                msg = schd_af(client);

            } else {
                largestServer = allToLargest(serverList, xml);
                msg = schd(client);
            }
        }
    }

            // QUIT
        
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







