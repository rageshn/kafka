package samples.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer {
	
	private Class <T> type;
	
	public JsonDeserializer(Class type) {
		this.type = type;
	}

	@Override
	public void configure(Map configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object deserialize(String topic, byte[] data) {
		// TODO Auto-generated method stub
		ObjectMapper mapper = new ObjectMapper();
		T obj = null;
		try {
			obj = mapper.readValue(data, type);
		}
		catch(Exception ex) {
			System.out.println("Exception while deserializing data: " + ex.getMessage());
		}
		return obj;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
