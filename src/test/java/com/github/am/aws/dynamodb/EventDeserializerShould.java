package com.github.am.aws.dynamodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.junit.Test;

public class EventDeserializerShould {
	private EventDeserializer eventDeserializer = new EventDeserializer();

	//@formatter:off
	@Test public void
	deserialize_dynamodb_new_image_request() throws Exception {
    List<TestType> deserializedModels = eventDeserializer.deserialize(sampleDynamoDbEvent(), TestType.class);
    
    assertThat(deserializedModels).hasSize(1);
    TestType advice = (TestType)deserializedModels.get(0);
    
    assertThat(advice.getId()).isEqualTo(1);
    assertThat(advice.getToken()).isEqualTo("DUMMY_TOKEN");
	}
	//@formatter:on

	private String sampleDynamoDbEvent() throws UnsupportedEncodingException {
		return "{\"Records\":[{\"eventID\":\"491eea1491aab476cd1071927e53bca4\",\"eventName\":\"INSERT\",\"eventVersion\":\"1.0\",\"eventSource\":\"aws:dynamodb\",\"awsRegion\":\"ap-northeast-1\",\"dynamodb\":{\"Keys\":{\"Id\":{\"S\":\"1\"}},\"NewImage\":{\"token\":{\"S\":\"DUMMY_TOKEN\"},\"id\":{\"N\":\"1\"}},\"SequenceNumber\":\"73200000000001986199874\",\"SizeBytes\":13,\"StreamViewType\":\"NEW_AND_OLD_IMAGES\"},\"eventSourceARN\":\"arn:aws:dynamodb:ap-northeast-1:049428796662:table/advice/stream/2016-05-11T07:24:20.363\"}]}";
	}

	class TestType {
		int id;
		String token;

		public int getId() {
			return id;
		}

		public String getToken() {
			return token;
		}

	}

}
