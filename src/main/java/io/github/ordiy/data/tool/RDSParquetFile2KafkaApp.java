package io.github.ordiy.data.tool;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.github.ordiy.data.tool.tool.KafkaMsgHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.htrace.shaded.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Hello world!
 */
public class RDSParquetFile2KafkaApp {
    private static final Logger logger = LogManager.getLogger(RDSParquetFile2KafkaApp.class);
    public static  String KeyName = "id";
    public static ObjectMapper objectMapper = new ObjectMapper(); // Jackson 用于将 Map 转换为 JSON
    private long total_red_lines = 0L ;

    public static void main(String[] args) {
//        System.setProperty("config.file.path","src/main/resources/config.properties");
        new RDSParquetFile2KafkaApp().setup();
    }

    public void setup() {
        String configPath = System.getProperty("config.file.path", "");
        if (StringUtils.isEmpty(configPath)) {
            logger.error(" config file is null");
            return;
        }
        //读取配置文件
        // 创建一个 Properties 对象
        Properties properties = new Properties();

        // 尝试读取文件
        try (InputStream input = new FileInputStream(configPath)) {
            // 加载 properties 文件
            properties.load(input);

            // 访问文件中的属性
            String kafkaBootstrap = properties.getProperty("kafka.bootstrap");
            String kafkaTopicName = properties.getProperty("kafka.topic");
            String rdsFilePath = properties.getProperty("rds.parquet.path");
            String rdsTableName = properties.getProperty("rds.parquet.table");
            KeyName = properties.getProperty("rds-table-filed-map-as-topic-msg-key","id");
            logger.info("config kafka:{} ,topic:{} , rds path:{}, rds_table:{}", kafkaBootstrap, kafkaTopicName, rdsTableName, rdsFilePath);

            KafkaMsgHandler kafkaMsgHandler = new KafkaMsgHandler(kafkaBootstrap, kafkaTopicName);
            List<java.nio.file.Path> fileList = null;
            //递归目录下parquet 文件，并进行读取和发送
            try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(rdsFilePath), FileVisitOption.FOLLOW_LINKS)) {
                // 过滤出 .parquet 文件并处理
                fileList = paths.filter(path -> path.toString().endsWith(".parquet"))
                        .collect(Collectors.toList());
            }
            if (CollectionUtils.isNotEmpty(fileList)) {
                logger.info("----> parquet_file_dis:{} total files num:{}", rdsFilePath, fileList.size());
                for (java.nio.file.Path parquetFile : fileList) {
                    readFileAndSendMsg(kafkaMsgHandler, parquetFile);
                }
            }
            logger.info("----END--- application. file:{},topic:{}, total_line:{}", rdsFilePath, kafkaTopicName, total_red_lines);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * @param kafkaMsgHandler
     * @param filePath
     */
    public void readFileAndSendMsg(KafkaMsgHandler kafkaMsgHandler, java.nio.file.Path parquetFile) throws IOException {
        String filePath = parquetFile.toAbsolutePath().toString();
        logger.info("readFileAndSendMsg file:{}", filePath);

        // 配置 Hadoop 文件系统，处理本地文件
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);
        MessageType schema = metadata.getFileMetaData().getSchema();
        // 创建 Parquet 文件阅读器
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));

        PageReadStore pages;
        while (null != (pages = reader.readNextRowGroup())) {
            final long rows = pages.getRowCount();
            total_red_lines += rows;
            logger.info("file:{} , total-colum row: {}", filePath, rows);

            // 读取每一列的数据
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

            for (int i = 0; i < rows; i++) {
                Group group = recordReader.read();
//                printGroup(group, schema);
                Map<String, Object> jsonMap = groupToJsonMapByType(group, schema);
                sendMsg(jsonMap, kafkaMsgHandler);
            }
        }
        //最终做一次flush
        kafkaMsgHandler.flush();
        reader.close();
        // 确保消息全部发送
        logger.info("application will ending .... , msg flushing , wait 0.5s ");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 转换数据格式发送到kafak
     *
     * @param jsonMap
     * @param kafkaMsgHandler
     */
    private void sendMsg(Map<String, Object> jsonMap, KafkaMsgHandler kafkaMsgHandler) {
        try {

            String key = Objects.toString(jsonMap.get(KeyName));
            Map<String, String> idJson = Collections.singletonMap(KeyName, key); // single map more better
            kafkaMsgHandler.sendMsg(objectMapper.writeValueAsString(idJson)
                    , objectMapper.writeValueAsString(jsonMap));
            logger.debug(" send msg to kafka msgKey:{}", key);
        } catch (JsonProcessingException e) {
            logger.error("--JSON ERROR--- send msg to kafka error.cause:", e);
            //ignore paras error
        }
    }

    // 将 Parquet 的 Group 对象转换为 JSON 对象（Map）
    private static Map<String, Object> groupToJsonMapByType(Group group, MessageType schema) {
        Map<String, Object> jsonMap = new LinkedHashMap<>(100);

        for (int fieldIndex = 0; fieldIndex < schema.getFieldCount(); fieldIndex++) {
            int fieldRepetitionCount = group.getFieldRepetitionCount(fieldIndex);
            String fieldName = schema.getFieldName(fieldIndex);
            Type fieldType = schema.getType(fieldIndex); // 获取字段类型

            // 处理重复字段
            if (fieldRepetitionCount > 1) {
                List<Object> values = new ArrayList<>();
                for (int i = 0; i < fieldRepetitionCount; i++) {
                    values.add(getValueByType(group, fieldIndex, i, fieldType));
                }
                jsonMap.put(fieldName, values); // 保存为列表
            } else if (fieldRepetitionCount == 1) {
                jsonMap.put(fieldName, getValueByType(group, fieldIndex, 0, fieldType));
            }
        }
        return jsonMap;
    }


    private static Object getValueByType(Group group, int fieldIndex, int repetitionIndex, Type fieldType) {
        if (fieldType.isPrimitive()) {
            PrimitiveType primitiveType = fieldType.asPrimitiveType();

            switch (primitiveType.getPrimitiveTypeName()) {
                case INT32:
                    return group.getInteger(fieldIndex, repetitionIndex);
                case INT64:
                    return handleInt64(group, fieldType, fieldIndex, repetitionIndex); // timestamp_micros
                case FLOAT:
                    return group.getFloat(fieldIndex, repetitionIndex);
                case DOUBLE:
                    return group.getDouble(fieldIndex, repetitionIndex);
                case BOOLEAN:
                    return group.getBoolean(fieldIndex, repetitionIndex);
                case BINARY:

                    return group.getBinary(fieldIndex, repetitionIndex).toStringUsingUTF8(); // 转换为字符串
                case FIXED_LEN_BYTE_ARRAY:
                    return group.getBinary(fieldIndex, repetitionIndex).toStringUsingUTF8(); // 转换为字符串
                case INT96:
                    return handleInt96(group, fieldIndex, repetitionIndex);
                default:
                    throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType.getPrimitiveTypeName());
            }
        } else {
            throw new IllegalArgumentException("Complex types (like Group) are not supported in this example.");
        }
    }

    // RDS 默认的 datetime 转 timestamp 是到 microseconds
    private static Object handleInt64(Group group, Type fieldType, int fieldIndex, int repetitionIndex) {

        long value = 0L;
        if (!Objects.isNull(fieldType.getLogicalTypeAnnotation()) &&
                Objects.requireNonNull(fieldType.getLogicalTypeAnnotation().toOriginalType()) == OriginalType.TIMESTAMP_MICROS) {
            value = group.getLong(fieldIndex, repetitionIndex) / 1_000L;
        } else {
            value = group.getLong(fieldIndex, repetitionIndex);
        }
        return value;
    }


    private static Long handleInt96(Group group, int fieldIndex, int repetitionIndex) {
        // 获取 INT96 数据
        Binary int96 = group.getInt96(fieldIndex, repetitionIndex);

        // 将 12 字节的 INT96 数据解析为时间戳
        ByteBuffer buffer = ByteBuffer.wrap(int96.getBytes());

        //1703 2322 5000 0
        //1703 2322 5000 0000
        //1703 2322 6700 0 000
        //2409 0000 0025 3829 00

        //时间类型
        //9223 3720 3685 4775 807
        // 解析 INT96 的高位和低位

        long seconds = buffer.getLong(); // 前 8 字节表示秒
        //时间戳 进行转换其它值 不做处理
        long value = buffer.getLong();
        if (1603232267000_000L < value && value < 1803232267000_000L) {
            value = value / 1_000;
        }
        logger.debug("handle time ts:{} , convert_ts:{}", seconds, value);

        return value;
    }


    /**
     *  打印 Parquet 文件的每一行数据
      */
    private static void printGroup(Group group, MessageType schema) {
        for (int fieldIndex = 0; fieldIndex < schema.getFieldCount(); fieldIndex++) {
            int fieldRepetitionCount = group.getFieldRepetitionCount(fieldIndex);
            String fieldName = schema.getFieldName(fieldIndex);

            for (int i = 0; i < fieldRepetitionCount; i++) {
                String value = group.getValueToString(fieldIndex, i);
                System.out.println(fieldName + ": " + value);
            }
        }
    }
}
