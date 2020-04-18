import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class XMLReader {
    private static final String FILE = "./system.xml";
    private static final String TAG_NAME = "system";

    protected static final String TYPE = "type";
    protected static final String LIMIT = "limit";
    protected static final String BOOTUP_TIME = "bootupTime";
    protected static final String RATE = "rate";
    protected static final String CORE_COUNT = "coreCount";
    protected static final String MEMORY = "memory";
    protected static final String DISK = "disk";

    protected static ArrayList<HashMap<String, String>> serverList;
    protected static HashMap<String, String> largestServer;

    public XMLReader() throws Exception {
        serverList = new ArrayList<HashMap<String, String>>();

        largestServer = new HashMap<String, String>();

        readXML();
    }

    private static void readXML() {
        try {
            //creating a constructor of file class and parsing an XML file
            File file = new File(FILE);

            //an instance of factory that gives a document builder
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            //an instance of builder to parse the specified xml file
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(file);
            doc.getDocumentElement().normalize();

            NodeList nodeList = doc.getElementsByTagName(TAG_NAME);

            for (int x=0; x<nodeList.getLength(); x++) {
                Node system = nodeList.item(x);

                if (system.getNodeType() == Node.ELEMENT_NODE) {
                    NodeList serversList = system.getChildNodes();

                    for (int y=0; y<serversList.getLength(); y++) {
                        Node servers = serversList.item(y);

                        if (servers.getNodeType() == Node.ELEMENT_NODE) {
                            NodeList serverList = servers.getChildNodes();

                            for (int z=0; z<serverList.getLength(); z++) {
                                Node server = serverList.item(z);

                                if (server.getNodeType() == Node.ELEMENT_NODE) {
                                    assemblyServerList(server);
                                }
                            }
                        }
                    }
                }
            }

            assemblyLargestServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String argv[]) throws Exception {
//        XMLReader xml = new XMLReader();
//        System.out.println(xml.largestServer.toString());
    }

    /* Assembly all servers to an ArrayListList<HashMap>.
    *  Map's key is server's attr,
    *  Map's value is server's attr value.
    */
    private static void assemblyServerList(Node server) {
        HashMap<String, String> serverMap = new HashMap<String, String>();

        NamedNodeMap nodeMap = server.getAttributes();

        for (int i=0; i<nodeMap.getLength(); i++) {
            Node item = nodeMap.item(i);

            serverMap.put(item.getNodeName(), item.getNodeValue());
        }

        serverList.add(serverMap);
    }

    /* Find the largest core server
    *  Return the server's map.
    */
    private static void assemblyLargestServer() {
        int largestCore = 0;

        for (int i=0; i<serverList.size(); i++) {
            HashMap<String, String> serverMap = serverList.get(i);
            int currentCore = Integer.parseInt(serverMap.get(CORE_COUNT));;

            if (currentCore > largestCore) {
                largestCore = currentCore;
                largestServer = serverMap;
            }
        }
    }
    
}
