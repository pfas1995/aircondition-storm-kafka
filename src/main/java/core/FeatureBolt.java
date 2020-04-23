package core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import config.AppConfig;
import msg.OriginalMsg;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FileUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class FeatureBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(FeatureBolt.class);

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

    public void append2CSV(String json, String filename) throws IOException {

        String[] header = {"时间", "室外温度", "室外湿度", "加湿阀开度设定", "加湿阀开度反馈", "加热阀门开度设定", "加热阀门开度反馈",
                "蒸汽总管温度", "蒸汽总管压力", "表冷阀门开度设定", "表冷阀门开度反馈", "表冷器进水温度 ", "表冷器出水温度",
                "表冷器进水压力", "混风阀开度设定", "混风阀开度反馈", "新风温度", "新风湿度", "新风阀开度设定", "新风阀开度反馈",
                "送风温度", "送风湿度", "回风温度", "回风湿度", "初效过滤器堵塞", "送风机两侧压差", "室内平均湿度",
                "室内平均温度", "测点1温度", "测点1湿度", "测点2温度", "测点2湿度", "测点3温度", "测点3湿度", "测点4温度",
                "测点4湿度", "测点5温度", "测点5湿度", "消防连锁信号", "超声波水加湿远程/就地", "超声波水加湿系统开",
                "超声波水加湿系统关", "超声波水加湿远程开", "超声波水加湿远程关", "超声波水加湿1档运行反馈", "超声波水加湿2档运行反馈",
                "超声波水加湿3档运行反馈", "超声波水加湿4档运行反馈", "超声波水加湿5档运行反馈", "超声波水加湿6档运行反馈",
                "超声波水加湿1档运行设定", "超声波水加湿2档运行设定", "超声波水加湿3档运行设定", "超声波水加湿4档运行设定",
                "超声波水加湿5档运行设定", "超声波水加湿6档运行设定", "超声波加湿系统1档故障状态", "超声波加湿系统2档故障状态",
                "超声波加湿系统3档故障状态", "超声波加湿系统4档故障状态", "超声波加湿系统5档故障状态", "超声波加湿系统6档故障状态",
                "送风机远程/就地", "送风机运行状态", "送风机故障", "送风机启停", "送风机频率", "温度指标设置", "湿度指标设置",
                "送风机电流", "叶柜进柜总重量", "叶柜出料剩余总重量"};

        List<Object> values = new ArrayList<>();
        JSONObject j = JSON.parseObject(json);
        long timestamp = j.getLong("timestamp");
        JSONArray jsonArray = j.getJSONArray("values");
        values.add(timestamp2Str(timestamp));
        float weight = 0;
        float reWeight = 0;
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject v = jsonArray.getJSONObject(i);
            if (v.getString("id").contains("REWEIGHT")) {
                reWeight += v.getFloat("v");
            }
            else if (v.getString("id").contains("WEIGHT")) {
                weight += v.getFloat("v");
            }
            else if (v.getString("id").equals("DL_KZS5_CY_PdS7205-1-dup")) {
                continue;
            }
            else {
                try {
                    values.add(v.getFloat("v"));
                }
                catch (Exception e) {
                    values.add(v.getBoolean("v"));
                }
            }
        }
        values.add(weight);
        values.add(reWeight);

        boolean append = false;
        File file = new File(filename);
        if (file.exists()) {
            append = true;
        }
        Appendable fileWriter = new FileWriter(filename,true);
        CSVPrinter printer = append? CSVFormat.RFC4180.print(fileWriter):CSVFormat.RFC4180.withHeader(header).print(fileWriter);
        printer.printRecord(values.toArray());
        printer.close();



    }

    /**
     * parse the topic of msg from tuple
     * @param tuple
     * @return
     */
//    public String parseTopic(Tuple tuple) {
//        return tuple.getValue(0).toString();
//    }

    /**
     * parse the msg from tuple
     * @param tuple
     * @return
     */
    public String parseMsg(Tuple tuple) {
        return tuple.getValue(0).toString();
    }

    @Override
    public void execute(Tuple tuple) {
        String json = parseMsg(tuple);
        try {
//            FileUtil.append(json, "data.json");
            append2CSV(json, "data.csv");
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        logger.error(tuple.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
