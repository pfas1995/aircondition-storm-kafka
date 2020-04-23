package config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.logging.log4j.core.util.Loader.getClassLoader;

/**
 *  配置类，从 application.yml 中读取 kafka, 模型服务， 反向控制服务的配置信息
 */
public class AppConfig {


    /**
     * 全局配置类
     */
    public static class AppGlobalConfig {
        public static String env;
        public static String appName;

        public static void initConfig (Map<String, Object> map) {

            env = (String) map.getOrDefault("env", null);
            appName = (String) map.getOrDefault("app-name", UUID.randomUUID().toString());
        }
    }
    /**
     *  kafka 配置
     */
    public static class DefaultKafkaConfig {
        public static String host;
        public static int port;
        public static List<String> topic;
        public static String groupId;
        public static Boolean enableAutoCommit;
        public static String autoOffsetReset;



        public static void init(Map<String, Object> map) {
            host = (String) map.getOrDefault("host", null);
            port = (int) map.getOrDefault("port", null);
            topic = (List<String>) map.getOrDefault("topic", null);
            groupId = (String) map.getOrDefault("group-id", null);
            autoOffsetReset = (String) map.getOrDefault("auto-offset-reset", null);
            enableAutoCommit = (Boolean) map.getOrDefault("enable-auto-commit", null);
        }

        public static Properties kafkaProperties() {
            Properties properties = new Properties();
            if (groupId != null) {
                properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            }
            if (autoOffsetReset != null) {
                properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            }
            if (enableAutoCommit != null) {
                properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
            }
            return properties;
        }


    }

    /**
     * 模型服务配置
     */
    public static class ModelServerConfig {
        public static String host;
        public static int port;
        public static String modelUrl;
        public static String modelConfigUrl;

        public static void init(Map<String, Object> map) {
            host = (String) map.getOrDefault("host", null);
            port = (int) map.getOrDefault("port", null);
            modelUrl = (String) map.getOrDefault("model-url", null);
            modelConfigUrl = (String) map.getOrDefault("model-config-url", null);
        }
    }

    public static class BoltField {

    }

    /**
     * 反向控制服务配置
     */
    public static class ControlServerConfig {
        public static String host;
        public static int port;
        public static String controlUrl;

        public static void init(Map<String, Object> map) {
            host = (String) map.getOrDefault("host", null);
            port = (int) map.getOrDefault("port", null);
            controlUrl = (String) map.getOrDefault("control-url", null);
        }
    }

    /**
     * 配置入口，配置所有的配置类
     */
    public static void initConfig() {
        InputStream inputStream = getClassLoader().getResourceAsStream("application.yml");
        Yaml yaml = new Yaml();
        Map<String, Object> map = yaml.load(inputStream);

        Map appConfig = (Map) map.getOrDefault("app", null);
        if (appConfig != null) {
            AppGlobalConfig.initConfig(appConfig);
        }

        Map kafkaConfig = (Map) map.getOrDefault("kafka-spout", null);
        if (kafkaConfig != null) {
            DefaultKafkaConfig.init(kafkaConfig);
        }

        Map modelConfig = (Map) map.getOrDefault("model-server", null);
        if (modelConfig != null) {
            ModelServerConfig.init(modelConfig);
        }

        Map controlConfig = (Map)map.getOrDefault("control-server", null);
        if (controlConfig != null) {
            ControlServerConfig.init(controlConfig);
        }
    }
}
