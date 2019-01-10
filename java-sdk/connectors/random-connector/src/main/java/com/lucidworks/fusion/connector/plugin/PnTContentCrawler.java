package com.lucidworks.fusion.connector.plugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.Session;

public class PnTContentCrawler{
    private String[] DO_NOT_CRAWL = new String[]{"_trash","Archive","Training and Certification"};

    private String startFolder;

    public PnTContentCrawler(String startFolder){
        this.startFolder = startFolder;
    }

    public List<String> getAllDocuments(Session session){

        

        return crawl(session.getObjectByPath(startFolder));
    }

    List<String> crawl(CmisObject object){

        
        List<String> objectIds = new ArrayList<>();

        System.out.println(object.getName());

        if(object.getBaseType().getDisplayName().equalsIgnoreCase("Folder")){

            Folder f = (Folder)object;

            if(!Arrays.asList(DO_NOT_CRAWL).contains(f.getName())) {
                Iterator<CmisObject> iterator = f.getChildren().iterator();
                while (iterator.hasNext()) {
                    objectIds.addAll(crawl(iterator.next()));
                }
            }

        }
        else if(object.getBaseType().getDisplayName().equalsIgnoreCase("Document")){
            objectIds.add(object.getId());
        }

        return objectIds;
    }
}