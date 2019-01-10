package com.lucidworks.fusion.connector.plugin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.chemistry.opencmis.client.api.Repository;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.commons.io.IOUtils;

public class PnTConnectorClient{
    private SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
    private Session session;
    private String CMIS_BROWSER_BINDING_URL = "https://alfpnt.intranet.dev.int.devlab.redhat.com/alfresco/api/-default-/public/cmis/versions/1.1/browser";
    File keyStore;

    private static PnTConnectorClient connectorClient = null;

    private PnTConnectorClient( String username, String password){

        System.out.println("GETTING TRUST STORE");

        try {
           keyStore = stream2file(getClass().getResourceAsStream("/pnt-portal.jks"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.setProperty("javax.net.ssl.trustStore",keyStore.getPath());
        System.setProperty("javax.net.ssl.trustStorePassword ","changeit");
        
        Map<String, String> parameter = new HashMap<String, String>();
        parameter.put(SessionParameter.USER,username);
        parameter.put(SessionParameter.PASSWORD,password);
        parameter.put(SessionParameter.BROWSER_URL,CMIS_BROWSER_BINDING_URL);
        parameter.put(SessionParameter.BINDING_TYPE,BindingType.BROWSER.value());

        List<Repository> repositories = this.sessionFactory.getRepositories(parameter);

        if (repositories.size()>0) {
            Repository repository = repositories.get(0);
            this.session = repository.createSession();

        }

    }

    public static PnTConnectorClient getConnectorClient( String username, String password){
        if(connectorClient==null){
            connectorClient = new PnTConnectorClient(username,password);
        }

        return connectorClient;
    }

    public Session getSession() {
        return session;
    }

    public File stream2file(InputStream inputStream) throws IOException{
        final File tempFile = File.createTempFile("stream2file",".tmp");
        tempFile.deleteOnExit();
        try(FileOutputStream out = new FileOutputStream(tempFile)){
            IOUtils.copy(inputStream,out);
        }
        return tempFile;
    }

}