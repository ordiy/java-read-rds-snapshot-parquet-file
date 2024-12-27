package io.github.ordiy.data.tool.tool;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class KafkaConsumerAndConvertMsg {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerAndConvertMsg.class);
    private static final ObjectMapper mapper = new ObjectMapper();


    public static void main(String[] args) {

        if (args.length < 2) {
            logger.error("args is null");
            return;
        }
        //config mapper
        mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        mapper.configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, true);

        //java cp ./rds-parquet-demo-1.0.1-SNAPSHOT.jar com.example.demo.too.KafkaConsumerAndConvertMsg
        //10.10.147.214:9092,10.10.155.75:9092,10.10.146.87:9092  cdc_src_dynamodb_pwa_webpush_log_history webpush_log_1101_convert earliest cdc-src-kafka-convert-pwa-webpush-log
        String bootstrap = args[0];
        String srcTopic = args[1];
        String groupName = args[2];
        String offsetConfig =args[3]; //earliest

        String targetTopic = args[4];

        //kafka producer
        KafkaMsgHandler kafkaMsgHandler = new KafkaMsgHandler(bootstrap, targetTopic);

        //kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);  // Kafka 服务器地址
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);  // 消费者组 ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");  // Key 反序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");  // Value 反序列化类
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig );  // 从最早的消息开始消费

        // 创建消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅指定的主题
        consumer.subscribe(Collections.singletonList(srcTopic));

        // 持续监听消息
        try {
            while (true) {
                // 拉取消息，设置轮询超时时间
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // 处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        logger.info("consumer msg_offset:{}", record.offset());

                        Map<String, Object> map = convertMsg(record.value());
                        Object id = map.get("unique_id");
                        Map<String, String> uniqueId = Collections.singletonMap("unique_id", Objects.toString(id));

                        String key = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(uniqueId);
                        String msg =  mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);

                        kafkaMsgHandler.sendMsg(key, msg);
                    }catch (JsonProcessingException e){
                        logger.error(" convert msg:{} failed.cause:{}", record.value(), e);
                    }
                }
            }
        } finally {
            kafkaMsgHandler.flush();
            // 关闭消费者
            consumer.close();
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> convertMsg( String recordValue) throws JsonProcessingException {
        // 将 JSON 字符串转换为 Map
        return mapper.readValue(recordValue, Map.class);
        // 输出结果

    }

}
