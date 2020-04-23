import config.AppConfig;
import core.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Run {

    public static void main(String[] args) throws Exception {

        // 初始化配置
        AppConfig.initConfig();
        if ("test".equals(AppConfig.AppGlobalConfig.env)) {
//            Test test = new Test();
//            test.run();
        }
        else {
            final TopologyBuilder tp = new TopologyBuilder();


            KafkaSpoutConfig<String, String> kafkaSpoutConfigKT = KafkaSpoutConfig.builder(
                    String.format("%s:%s", AppConfig.DefaultKafkaConfig.host, AppConfig.DefaultKafkaConfig.port),
                    AppConfig.DefaultKafkaConfig.topic.get(0))
                    .setProp(AppConfig.DefaultKafkaConfig.kafkaProperties())
                    .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                    .build();

            KafkaSpoutConfig<String, String> kafkaSpoutConfigCYG = KafkaSpoutConfig.builder(
                    String.format("%s:%s", AppConfig.DefaultKafkaConfig.host, AppConfig.DefaultKafkaConfig.port),
                    AppConfig.DefaultKafkaConfig.topic.get(1))
                    .setProp(AppConfig.DefaultKafkaConfig.kafkaProperties())
                    .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                    .build();

            KafkaSpout<String, String> kafkaSpoutKT = new KafkaSpout<>(kafkaSpoutConfigKT);
            KafkaSpout<String, String> kafkaSpoutCYG = new KafkaSpout<>(kafkaSpoutConfigCYG);
            tp.setSpout("KT", kafkaSpoutKT,1);
            tp.setSpout("CYG", kafkaSpoutCYG, 1);
            tp.setBolt("Merge", new MergeBolt(), 1).shuffleGrouping("KT").shuffleGrouping("CYG");
            tp.setBolt("Feature", new FeatureBolt(), 1).fieldsGrouping("Merge", new Fields("merge"));
//            tp.setBolt("test", new TestBolt(), 1).shuffleGrouping("KT").shuffleGrouping("CYG");


//            tp.setBolt("kt-receiver", new KTBolt(), 1).shuffleGrouping("KT");
//            tp.setBolt("cyg-receiver", new CYGBolt(), 1).shuffleGrouping("CYG");
//            tp.setBolt("merge", new MergeBolt(), 1).shuffleGrouping("cyg-receiver").shuffleGrouping("kt-receiver");


            // 提交运行
            StormTopology sp = tp.createTopology();
            Config conf = new Config();
            if ("local".equals(AppConfig.AppGlobalConfig.env)) {
                LocalCluster cluster = new LocalCluster();

                cluster.submitTopology(AppConfig.AppGlobalConfig.appName, conf, sp);
            }
            else if("cluster".equals(AppConfig.AppGlobalConfig.env)){
                StormSubmitter.submitTopology(AppConfig.AppGlobalConfig.appName, conf, sp);
            }

        }
    }
}
