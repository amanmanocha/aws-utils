package com.github.am.aws.dynamodb;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

public class EventDeserializer {
  private static final String BINARY_TYPE = "\\{\"B\":";
  private static final String NUMBER_TYPE = "\\{\"N\":";
  private static final String STRING_TYPE = "\\{\"S\":";

  private static final String RECORDS_NODE = "Records";
  private static final String DYNAMODB_NODE = "dynamodb";
  private static final String NEW_IMAGE_NODE = "NewImage";

  private Gson gson = new Gson();

  public <T> List<T> deserialize(String jsonString, Class<T> clazz) throws IOException {
    JsonNode json = new ObjectMapper().readTree(jsonString);
    JsonNode recordNodes = json.get(RECORDS_NODE);

    if (recordNodes == null)
      return emptyList();

    return deserialize(clazz, recordNodes);
  }

  private <T> List<T> deserialize(Class<T> clazz, JsonNode recordNodes) {
    List<T> models = new ArrayList<>();
    for (int i = 0; i < recordNodes.size(); i++) {
      JsonNode recordNode = recordNodes.get(i);
      JsonNode dynamodbNode = recordNode.get(DYNAMODB_NODE);
      JsonNode newImageNode = dynamodbNode.get(NEW_IMAGE_NODE);

      models.add((T) deserializeNewImage("" + newImageNode, clazz));
    }
    return models;
  }

  private <T> T deserializeNewImage(String json, Class<T> clazz) {
    String parts[] = json.split("}");

    String jsonWithoutTypes = "";
    for (String part : parts) {
      jsonWithoutTypes += part.replaceAll(STRING_TYPE, "");
      jsonWithoutTypes = jsonWithoutTypes.replaceAll(NUMBER_TYPE, "");
      jsonWithoutTypes = jsonWithoutTypes.replaceAll(BINARY_TYPE, "");
    }
    jsonWithoutTypes = jsonWithoutTypes + "}";

    return gson.fromJson(jsonWithoutTypes, clazz);
  }

}
