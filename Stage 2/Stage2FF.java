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

public class Stage2FF {
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
    private static ArrayList < HashMap < String, String >> initialServerList = new ArrayList < HashMap < String, String >> ();
    private static HashMap < String, String > largestServer = new HashMap < String, String > ();
    private static HashMap < String, String > firstFitServer = new HashMap < String, String > ();
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
    private static boolean isInitial = true;


    public Stage2FF(String address, int port) throws Exception {
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
    private static String resc(Stage2FF client, String inMsg) throws Exception {
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
            if (!msg.equals(DOT)&& isInitial == true) {
                initialServerList = assembleServerList(initialServerList, msg);
            }
            if (!msg.equals(DOT)) {
                serverList = assembleServerList(serverList, msg);
            }
        }
	isInitial = false;
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
    
    private static HashMap < String, String > allToFirstFit(ArrayList < HashMap < String, String >> initialServerList, ArrayList < HashMap < String, String >> serverList, int cpuCores,int memory,int disk) {
      	ArrayList < HashMap < String, String >> sortedList = new ArrayList < HashMap < String, String >>();
    	
	// sort the initial server list
	initialServerList.sort(Comparator.comparing(m ->Integer.parseInt(m.get(CPU_CORES)),
						    Comparator.nullsLast(Comparator.naturalOrder())));
	
	// sort the current serverList according to orginial List
	for(int i = 0; i < initialServerList.size();i++){
	    String initialServerType = initialServerList.get(i).get(SERVER_TYPE);
	    int initialServerId = Integer.parseInt(initialServerList.get(i).get(SERVER_ID));
	    for(int j = 0; j < serverList.size(); j++){
		String serverType = serverList.get(j).get(SERVER_TYPE);
	        int serverId = Integer.parseInt(serverList.get(j).get(SERVER_ID));
		if(serverType.equals(initialServerType) && initialServerId==serverId){
			sortedList.add(serverList.get(j));
			break;
		}		
	   } 	
	}


	firstFitServer =new HashMap < String, String > ();
	
	// find a server of current serverlist with capable resources
	for(int i = 0; i < sortedList.size(); i++){

	  int serverCoreSize = Integer.parseInt(sortedList.get(i).get(CPU_CORES));
	  int serverDisk = Integer.parseInt(sortedList.get(i).get(DISK_SPACE));
	  int serverMemory = Integer.parseInt(sortedList.get(i).get(MEMORY));
          int serverState = Integer.parseInt(sortedList.get(i).get(SERVER_STATE));

	  if(cpuCores <=serverCoreSize && memory <=serverMemory && disk <= serverDisk && serverState!=4 ){
		firstFitServer = sortedList.get(i);
		break;
	   }
	}
	// if all the current servers are not capable, find the server that is initially capable
	if(firstFitServer.get(CPU_CORES)== null){
	 	outerLoop:
		for(int i = 0; i < sortedList.size(); i++){
		  
		     int serverState = Integer.parseInt(sortedList.get(i).get(SERVER_STATE));
		      if(serverState ==1 || serverState ==3){
			String serverType = sortedList.get(i).get(SERVER_TYPE);
			int serverId = Integer.parseInt(sortedList.get(i).get(SERVER_ID));
			
			for(int j = 0; j < initialServerList.size(); j++){
			
			   String initialServerType = initialServerList.get(j).get(SERVER_TYPE);
			   int initialServerId = Integer.parseInt(initialServerList.get(j).get(SERVER_ID));
			   int initialCoreSize = Integer.parseInt(initialServerList.get(j).get(CPU_CORES));

			   int initialDisk = Integer.parseInt(initialServerList.get(j).get(DISK_SPACE));
			   int initialMemory = Integer.parseInt(initialServerList.get(j).get(MEMORY));

			    if(serverType.equals(initialServerType) && initialServerId==serverId && initialCoreSize>= cpuCores && initialDisk >= disk && initialMemory >= memory ){
				firstFitServer = sortedList.get(i);	
				 break outerLoop;
			    }
			}
		     }
		}
	}

        return firstFitServer;
    }
    // Scheduling decision
    private static String schd(Stage2FF client) throws Exception {
        client.sendMsg(SCHD + SPLIT + jobID + SPLIT + largestServer.get(SERVER_TYPE) + SPLIT + largestServer.get(SERVER_ID));
        //System.out.println(RCVD + SPLIT + SCHD + SPLIT + jobID + SPLIT + largestServer.get(SERVER_TYPE) + SPLIT + largestServer.get(SERVER_ID));
        String msg = client.getMsg();
        //System.out.println(SENT + SPLIT + msg);

        return msg;
    }
    private static String schd_ff(Stage2FF client) throws Exception {
        client.sendMsg(SCHD + SPLIT + jobID + SPLIT + firstFitServer.get(SERVER_TYPE) + SPLIT + firstFitServer.get(SERVER_ID));
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
            Stage2FF client = new Stage2FF(serverName, 50000);

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
                    msg = resc(client, msg);
                    if (algorithm.equals("ff")) {
			
                        firstFitServer = allToFirstFit(initialServerList, serverList, Integer.parseInt(CPUCores),Integer.parseInt(memory),Integer.parseInt(disk));
                        msg = schd_ff(client);

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


