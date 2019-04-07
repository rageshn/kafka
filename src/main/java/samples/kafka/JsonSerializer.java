package samples.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 * @author ragesh
 * This class is for serializing json objects while sending it to kafka topics
 *
 */
public class JsonSerializer implements Serializer {

	@Override
	public void configure(Map configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String topic, Object data) {
		// TODO Auto-generated method stub
		byte[] returnValue = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			returnValue = objectMapper.writeValueAsBytes(data);
		}
		catch(Exception ex) {
			System.out.println("Exception while serializing json message: " + ex.getMessage());
		}
		return returnValue;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
