package com.company;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class XMLReader {
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
            File file = new File(Constant.FILE);

            //an instance of factory that gives a document builder
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            //an instance of builder to parse the specified xml file
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(file);
            doc.getDocumentElement().normalize();

            NodeList nodeList = doc.getElementsByTagName(Constant.TAG_NAME);

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

            //sortServerList();
            assemblyLargestServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String argv[]) throws Exception {
//        XMLReader xml = new XMLReader();
//
//        for (int i=0; i<serverList.size(); i++) {
//            HashMap<String, String> serverMap = serverList.get(i);
//
//            System.out.println(serverMap.get(Constant.CORE_COUNT));
//        }
//
//        System.out.println(xml.largestServer.toString());

        HashMap<String, ArrayList> map = new HashMap<String, ArrayList>();

        ArrayList list = map.get("dsd");

        if (list == null || list.isEmpty()) {
            System.out.println(0);
        } else {
            System.out.println(1);
        }
    }

    private static void assemblyServerList(Node server) {
        HashMap<String, String> serverMap = new HashMap<String, String>();

        NamedNodeMap nodeMap = server.getAttributes();

        for (int i=0; i<nodeMap.getLength(); i++) {
            Node item = nodeMap.item(i);

            serverMap.put(item.getNodeName(), item.getNodeValue());
        }

        serverList.add(serverMap);
    }

    private static void sortServerList() {
        for (int i=0; i <serverList.size(); i++) {
            for (int j=0; j<serverList.size()-i-1; j++) {
                if (Integer.parseInt(serverList.get(j).get(Constant.CORE_COUNT)) > Integer.parseInt(serverList.get(j+1).get(Constant.CORE_COUNT))) {
                    serverList.add(j, serverList.get(j+1));
                    serverList.remove(j+2);
                }
            }
        }
    }

    private static void assemblyLargestServer() {
        int largestCore = 0;

        for (int i=0; i<serverList.size(); i++) {
            HashMap<String, String> serverMap = serverList.get(i);
            int currentCore = Integer.parseInt(serverMap.get(Constant.CORE_COUNT));;

            if (currentCore > largestCore) {
                largestCore = currentCore;
                largestServer = serverMap;
            }
        }

        //largestServer = serverList.get(serverList.size()-1);
    }

}