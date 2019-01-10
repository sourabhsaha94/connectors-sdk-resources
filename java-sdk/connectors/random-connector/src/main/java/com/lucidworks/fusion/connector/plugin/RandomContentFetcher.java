package com.lucidworks.fusion.connector.plugin;

import com.lucidworks.fusion.connector.plugin.api.fetcher.type.content.FetchInput;

import com.lucidworks.fusion.connector.plugin.api.fetcher.result.FetchResult;
import com.lucidworks.fusion.connector.plugin.api.fetcher.result.PreFetchResult;
import com.lucidworks.fusion.connector.plugin.api.fetcher.result.StartResult;
import com.lucidworks.fusion.connector.plugin.api.fetcher.result.StopResult;
import com.lucidworks.fusion.connector.plugin.api.fetcher.type.content.ContentFetcher;
import com.lucidworks.fusion.connector.plugin.api.fetcher.type.content.MessageHelper;

import org.apache.chemistry.opencmis.client.api.Document;
import org.apache.chemistry.opencmis.client.api.Property;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import javax.inject.Inject;
import java.net.InetAddress;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class RandomContentFetcher implements ContentFetcher {

  private final static String ERROR_ID = "no-number-this-should-fail";

  private static final Random rnd = new Random();
  private static final Logger logger = LogManager.getLogger(RandomContentFetcher.class);
  private final RandomContentConfig randomContentConfig;
  private final RandomContentGenerator generator;

  Session session;
  List<String> documentIdList;
  @Inject
  public RandomContentFetcher(
      RandomContentConfig randomContentConfig,
      RandomContentGenerator generator
  ) {
    this.randomContentConfig = randomContentConfig;
    this.generator = generator;
  }

  @Override
  public StartResult start(StartContext context) {

    //Session session = PnTConnectorClient.getConnectorClient("Crawler", "crawler").getSession();

    //System.out.println(session.getObjectByPath("/Sites/pnt-portal/documentLibrary/Customer success").getName());

    return ContentFetcher.super.start(context);
  }

  @Override
  public PreFetchResult preFetch(PreFetchContext preFetchContext) {

    session = PnTConnectorClient.getConnectorClient("Crawler", "crawler").getSession();

    PnTContentCrawler contentCrawler = new PnTContentCrawler("/Sites/pnt-portal/documentLibrary/Customer success");

    documentIdList = contentCrawler.getAllDocuments(session);
    
    for(Integer i=0;i<documentIdList.size();i++){
      Map<String, Object> data = Collections.singletonMap("number", i);
      preFetchContext.emitCandidate(MessageHelper.candidate(i.toString(), Collections.emptyMap(), data).build());
    }


    /*IntStream.range(0, documentIdList.size()).asLongStream().forEach(i -> {
      logger.info("Emitting candidate -> number {}", i);
      Map<String, Object> data = Collections.singletonMap("number", count);
      preFetchContext.emitCandidate(MessageHelper.candidate(UUID.randomUUID().toString(), Collections.emptyMap(), data).build());
    });*/
    // Simulating an error item here... because we're emitting an item without a "number",
    // the fetch() call will attempt to convert the number into a long and throw an exception.
    // The item should be recorded as an error in the ConnectorJobStatus.
    //preFetchContext.emitCandidate(MessageHelper.candidate(ERROR_ID).build());
    return preFetchContext.newResult();
  }

  @Override
  public FetchResult fetch(FetchContext fetchContext) {
    FetchInput input = fetchContext.getFetchInput();
    Map<String,Object> contentMap = new HashMap<>();
    Integer num = (Integer) input.getMetadata().get("number");
    /*try {
      long num = (Long) input.getMetadata().get("number");

      String headline = generator.makeSentence(true);
      int numSentences = getRandomNumberInRange(10, 255);
      String txt = generator.makeText(numSentences);
      //logger.info("Emitting Document -> number {}", num);

      Map<String, Object> fields = new HashMap();
      fields.put("number_i", num);
      fields.put("timestamp_l", Instant.now().toEpochMilli());
      fields.put("headline_s", headline);
      fields.put("hostname_s", hostname);
      fields.put("text_t", txt);
      fetchContext.emitDocument(fields);
    } catch (NullPointerException npe) {
      if (ERROR_ID.equals(input.getId())) {
        logger.info("The following error is expected, as means to demonstrate how errors are emitted");
      }

      throw npe;
    }*/

    Document document = (Document)session.getObject(documentIdList.get((int)num));
    //logger.info("Emitting Document -> number {}", num);

    for(Property p : document.getProperties()){
      contentMap.put(p.getId()+"_s",p.getValueAsString());
    }

    fetchContext.emitDocument(contentMap);
    return fetchContext.newResult();
  }

  @Override
  public StopResult stop(StopContext context) {
    return ContentFetcher.super.stop(context);
  }
}