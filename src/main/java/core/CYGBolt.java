package core;

import com.google.gson.Gson;
import com.sun.javafx.scene.control.skin.VirtualFlow;
import config.AppConfig;
import msg.OriginalMsg;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CYGBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(CYGBolt.class);

    private Map<String, Object> topoConf;

    private TopologyContext context;

    private OutputCollector collector;

    private Gson gson;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.topoConf = topoConf;
        this.context = context;
        this.collector = collector;
        this.gson = new Gson();
    }


    /**
     * 解析 tuple 里面的参数信息
     * 得到 一个String
     * i = 4：根据Kafka的传输过来的数据来确定
     */
    private String parseTuple(Tuple tuple) {
        return tuple.getValue(4).toString();
    }

    public String timestamp2Str(long timestamp) {
        try {
            String format = "yyyy-MM-dd HH:mm:ss";
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return sdf.format(timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * parse the topic of msg from tuple
     * @param tuple
     * @return
     */
    public String parseTopic(Tuple tuple) {
        return tuple.getValue(0).toString();
    }

    /**
     * parse the msg from tuple
     * @param tuple
     * @return
     */
    public String parseMsg(Tuple tuple) {
        return tuple.getValue(4).toString();
    }

    @Override
    public void execute(Tuple tuple) {

        String topic = parseTopic(tuple);
        String str = parseTuple(tuple);
        OriginalMsg originalMsg = null;
        try {
            originalMsg = gson.fromJson(str, OriginalMsg.class);
            if (originalMsg == null) {
                logger.error("wrong msg: {}", tuple);
//                collector.ack(tuple);
                return;
            }
            int sensorCount = originalMsg.sensorCount();
            logger.error("topic: {}, time:  {}, sensor count: {}", topic, timestamp2Str(originalMsg.getTimestamp()), sensorCount);

            collector.emit(new Values(str));

        } catch (Exception e) {
            logger.error(e.getMessage() + " " + tuple.toString());
        }
        finally {

        }



//        collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("merge"));
    }
}
