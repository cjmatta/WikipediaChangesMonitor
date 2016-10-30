package org.cmatta.kafka.streams.wikipedia;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chris on 10/17/16.
 */
public class WikipediaMessageParser {
  private static Logger log = LoggerFactory.getLogger(WikipediaMessageParser.class);
  public static KeyValue<String, String> getRawMessage(JsonNode key, JsonNode value) {
    try {
      String channel = key.get("payload").get("channel").asText();
      String message = value.get("payload").get("message").asText();
      return new KeyValue<>(channel, message);
    } catch (IllegalGenerationException e) {
      log.error(e.getMessage());
      return new KeyValue<>(null, null);
    }
  }

  public static KeyValue<String, WikipediaChange> parseMessage(JsonNode key, JsonNode value) {
    String rawMessage = value.get("payload").get("message").asText();
    Date createdAt = new Date(Long.parseLong(value.get("payload").get("createdat").asText()));

    String pattern ="\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)";
    Pattern wikiPattern = Pattern.compile(pattern);
    Matcher matcher = wikiPattern.matcher(rawMessage);

    String wikiPage = matcher.group(1);
    String flags = matcher.group(2);
    String diffUrl = matcher.group(3);
    String userName = matcher.group(4);
    int byteChange = Integer.parseInt(matcher.group(5));
    String commitMessage = matcher.group(6);

    WikipediaChange change = new WikipediaChange(createdAt, wikiPage, flags, diffUrl, userName, byteChange, commitMessage);

    return new KeyValue<String, WikipediaChange>(wikiPage, change);
  }
}
