package org.cmatta.kafka.streams.wikipedia;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

/**
 * Created by chris on 10/24/16.
 */
public class WikipediaChange extends Struct {
  private static final String CREATED_AT = "createdat";
  private static final String WIKI_PAGE = "wikipage";
  private static final String IS_NEW = "isnew";
  private static final String IS_MINOR = "isminor";
  private static final String IS_UNPATROLLED = "isunpatrolled";
  private static final String IS_BOT_EDIT = "isbotedit";
  private static final String DIFF_URL = "diffurl";
  private static final String USER_NAME = "username";
  private static final String BYTE_CHANGE = "bytechange";
  private static final String COMMIT_MESSAGE = "commitmessage";

  public static final Schema SCHEMA = SchemaBuilder.struct()
      .name("org.cmatta.kafka.streams.wikipedia.wikipediachange")
      .doc("Message containing change information from Wikipedia")
      .field(CREATED_AT, Timestamp.builder().required().doc("The time the event was received.").build())
      .field(WIKI_PAGE, SchemaBuilder.string().required().doc("The page changed.").build())
      .field(IS_NEW, SchemaBuilder.bool().required().defaultValue(false).doc("A new entry.").build())
      .field(IS_MINOR, SchemaBuilder.bool().required().defaultValue(false).doc("A minor edit.").build())
      .field(IS_UNPATROLLED, SchemaBuilder.bool().required().defaultValue(false).doc("Unpatrolled").build())
      .field(IS_BOT_EDIT, SchemaBuilder.bool().required().defaultValue(false).doc("Did a bot edit?").build())
      .field(DIFF_URL, SchemaBuilder.string().required().doc("The URL containing the change.").build())
      .field(USER_NAME, SchemaBuilder.string().required().doc("The username of the editor").build())
      .field(BYTE_CHANGE, SchemaBuilder.int32().required().doc("The bytes changed").build())
      .field(COMMIT_MESSAGE, SchemaBuilder.string().required().doc("The commit message of the change."));


  public WikipediaChange(Date createdAt, String wikiPage, String flag, String diffUrl, String userName, int byteChange, String commitMessage){
    super(SCHEMA);
    this
        .put(CREATED_AT, createdAt)
        .put(WIKI_PAGE, wikiPage)
        .put(IS_NEW, flag.contains("N"))
        .put(IS_MINOR, flag.contains("M"))
        .put(IS_UNPATROLLED, flag.contains("!"))
        .put(IS_BOT_EDIT, flag.contains("B"))
        .put(DIFF_URL, diffUrl)
        .put(USER_NAME, userName)
        .put(BYTE_CHANGE, byteChange)
        .put(COMMIT_MESSAGE, commitMessage);
  }

  @Override
  public String toString() {
    String returnVal = String.format(
        "Created At: %s\nPage Edited: %s\nNew Page: %s\n" +
            "Minor Edit: %s\nUnpatrolled: %s\nBot Edited: %s\n" +
            "URL: %s\nUser: %s\nBytes: %s\nMessage: %s",
        this.get(CREATED_AT),
        this.get(WIKI_PAGE),
        this.get(IS_NEW),
        this.get(DIFF_URL),
        this.get(USER_NAME),
        this.get(BYTE_CHANGE),
        this.get(COMMIT_MESSAGE));

    return returnVal;

  }
}
