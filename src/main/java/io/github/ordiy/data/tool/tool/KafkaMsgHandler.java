package io.github.ordiy.data.tool.tool;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class KafkaMsgHandler {

    KafkaProducer<String, String> kafkaProducer = null;
    String topic = null;
    public KafkaMsgHandler(String kafkaBootstrap, String topic){

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 每个批次16KB（默认值是 16KB）
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10); // 等待 10 毫秒后发送批次（即使未满）
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"); // 启用 zstd 压缩消息，减少传输带宽
        this.topic = topic;
        this.kafkaProducer = new KafkaProducer<>(props);
    }


    public void sendMsg(String key, String value){
      kafkaProducer.send(new ProducerRecord<>(topic, key, value));
    }

    public void flush(){
        kafkaProducer.flush();
    }

}
