package core;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.sun.org.apache.xpath.internal.operations.Or;
import com.sun.xml.internal.ws.api.message.ExceptionHasMessage;
import config.AppConfig;
import msg.OriginalMsg;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.*;


public class TestBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(TestBolt.class);

    private Map<String, Object> topoConf;

    private TopologyContext context;

    private OutputCollector collector;

    private Gson gson;

    private Queue<OriginalMsg> ktMsg;

    private Queue<OriginalMsg> cygMsg;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.topoConf = topoConf;
        this.context = context;
        this.collector = collector;
        this.gson = new Gson();
        this.ktMsg = new LinkedList<>();
        this.cygMsg = new LinkedList<>();
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
        if (topic.equals(AppConfig.DefaultKafkaConfig.topic.get(0))) {
            OriginalMsg originalMsg = null;
            try {
                OriginalMsg.SensorMsg[] sensorMsgs = gson.fromJson(str, OriginalMsg.SensorMsg[].class);
                if (sensorMsgs.length == 0) {
                    throw new Exception("msg parser false");
                }
                List<OriginalMsg.SensorMsg> values = Arrays.asList(sensorMsgs);
                originalMsg = new OriginalMsg(sensorMsgs[0].t, values);
            } catch (Exception e) {
                logger.error(e.getMessage() + " " + tuple.toString());
            }

            if (originalMsg == null) {
                logger.error("wrong msg: {}", tuple);
                return;
            }


            int sensorCount = originalMsg.sensorCount();
            logger.error("topic: {}, time:  {}, sensor count: {}", topic, timestamp2Str(originalMsg.getTimestamp()), sensorCount);
        }
        else if (topic.equals(AppConfig.DefaultKafkaConfig.topic.get(1))) {
            OriginalMsg originalMsg = null;
            try {
                originalMsg = gson.fromJson(str, OriginalMsg.class);
            } catch (Exception e) {
                logger.error(e.getMessage() + " " + tuple.toString());
            }

            if (originalMsg == null) {
                logger.error("wrong msg: {}", tuple);
//                collector.ack(tuple);
                return;
            }


            int sensorCount = originalMsg.sensorCount();
            logger.error("topic: {}, time:  {}, sensor count: {}", topic, timestamp2Str(originalMsg.getTimestamp()), sensorCount);
        }

//        while (!ktMsg.isEmpty() && !cygMsg.isEmpty()) {
//            OriginalMsg ktm = ktMsg.peek();
//            OriginalMsg cygm = cygMsg.peek();
//            if (Math.abs(ktm.getTimestamp() - cygm.getTimestamp()) < 1000) {
//                logger.error("time:  {}, sensor count: {}", timestamp2Str(ktm.getTimestamp()), ktm.sensorCount());
//                logger.error("time:  {}, sensor count: {}", timestamp2Str(cygm.getTimestamp()), cygm.sensorCount());
//            }
//            else if (ktm.getTimestamp() > cygm.getTimestamp()) {
//                cygMsg.poll();
//            }
//            else {
//                ktm.getTimestamp();
//            }
//        }
//
//        collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
