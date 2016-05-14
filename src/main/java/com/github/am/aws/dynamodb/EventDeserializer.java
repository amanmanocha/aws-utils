package com.github.am.aws.dynamodb;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

public class EventDeserializer {
	private Gson gson = new Gson();

	public <T> List<T> deserialize(String jsonString, Class<T> clazz) throws IOException {
		JsonNode json = new ObjectMapper().readTree(jsonString);
		JsonNode recordNodes = json.get("Records");

		if (recordNodes == null)
			return emptyList();

		List<T> models = new ArrayList<>();
		for (int i = 0; i < recordNodes.size(); i++) {

			JsonNode recordNode = recordNodes.get(i);
			JsonNode dynamodbNode = recordNode.get("dynamodb");
			JsonNode newImageNode = dynamodbNode.get("NewImage");

			models.add((T) deserializeNewImage("" + newImageNode, clazz));
		}
		return models;
	}

	private <T> T deserializeNewImage(String json, Class<T> clazz) {
		String parts[] = json.split("}");

		String jsonWithoutTypes = "";
		for (String part : parts) {
			jsonWithoutTypes += part.replaceAll("\\{\"S\":", "");
			jsonWithoutTypes = jsonWithoutTypes.replaceAll("\\{\"N\":", "");
			jsonWithoutTypes = jsonWithoutTypes.replaceAll("\\{\"B\":", "");
		}
		jsonWithoutTypes = jsonWithoutTypes + "}";

		return gson.fromJson(jsonWithoutTypes, clazz);
	}

}
