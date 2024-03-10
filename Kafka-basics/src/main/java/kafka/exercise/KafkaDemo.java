package kafka.exercise;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaDemo {

    private static final Logger log = LoggerFactory.getLogger(KafkaDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("The Kafka Producer");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-kafka", "hello world");

        // send data
        // 비동기식
        producer.send(producerRecord);

        // flush and close the producer
        // tell the producer to send all data and block until done -- 동기식
        producer.flush();
        producer.close();

        // 데이터를 보내는 방식은 비동기 방식이기 때문에 flush()와 close()가 없다면
        // producer가 kafka에게 데이터를 전송할 기회를 주지 않고 프로그램이 종료되었을 것이다.
        // 하지만 flush()를 호출해 kafka로 데이터를 전송한 다음 producer가 닫히게 함으로써, 모든 작업을 마친 뒤에 프로그램이 종료되도록 할 수 있다.

        // 실제 프로그램에서 flush()를 직접 호출하는 일은 매우 드물고 프로그램 종료 전에 close()를 호출할 것이다.
        // 하지만 그 이후에도 send()로 보낸 데이터는 전송된다. producer가 게속해서 동작하기 때문이다.
    }

}
