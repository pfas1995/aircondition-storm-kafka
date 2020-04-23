package msg;

import com.google.gson.annotations.Expose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class OriginalMsg implements Serializable {

    public class SensorMsg {
        public Object id;
        public Object v;
        public long t;
    }

//    @Expose
    public long timestamp;

    @Expose
    public List<SensorMsg> values;


    public int sensorCount() {
        return values == null? 0:values.size();
    }


//    /**
//     * 记录所有PLC点位的对应关系
//     */
//    private static final Map<String, String> mapping = new HashMap<>();
//
//
//    /**
//     * 记录数值点位的数量 (5H.5H开头的)
//     */
//    private static int requiredCount;
//
//    private static final Logger logger = LoggerFactory.getLogger(OriginalMsg.class);
//
//    static {
//        mapping.put("batch", "6032.6032.LD5_YT603_2_YS2ROASTBATCHNO");
//        mapping.put("brand", "6032.6032.LD5_YT603_2_YS2ROASTBRAND");
//        mapping.put("deviceStatus1", "5H.5H.LD5_KL2226_PHASE1");
//        mapping.put("deviceStatus2", "5H.5H.LD5_KL2226_PHASE2");
//        mapping.put("siroxWorkStatus", "5H.5H.LD5_KL2226_SiroxWorkStatus");
//        mapping.put("flowAcc", "5H.5H.LD5_CK2222_TbcLeafFlowSH");
//        mapping.put("humidOut", "5H.5H.LD5_KL2226_ZF2LeafMois");
//        mapping.put("humidIn", "5H.5H.LD5_KL2226_InputMoisture");
//        mapping.put("humidSetting", "5H.5H.LD5_KL2226_TT1LastMoisSP");
//        mapping.put("windSpeed", "5H.5H.LD5_KL2226_ProcAirSpeedPV");
//        mapping.put("tempSetting1", "5H.5H.LD5_KL2226_BucketTemp1SP");
//        mapping.put("tempActual1", "5H.5H.LD5_KL2226_BucketTemp1PV");
//        mapping.put("tempSetting2", "5H.5H.LD5_KL2226_BucketTemp2SP");
//        mapping.put("tempActual2", "5H.5H.LD5_KL2226_BucketTemp2PV");
//        mapping.put("press", "5H.5H.LD5_KL2226_BacCoverPrePV");
//
//
//        mapping.values().forEach(new Consumer<String>() {
//            @Override
//            public void accept(String s) {
//                requiredCount = s.startsWith("5H.5H") ? requiredCount + 1 : requiredCount;
//            }
//        });
//
//    }

//    private static String getKey(String value) {
//        String key = "";
//        for (Map.Entry<String, String> entry : mapping.entrySet()) {
//            if (value.equals(entry.getValue())) {
//                key = entry.getKey();
//            }
//        }
//        return key;
//    }



    public OriginalMsg(long timestamp, List<SensorMsg> values) {
        this.timestamp = timestamp;
        this.values = values;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<SensorMsg> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "OriginalMsg{" +
                "timestamp=" + timestamp +
                ", values=" + values +
                '}';
    }

    //    /**
//     * 从点位中得到数据
//     * 1. 点位信息缺失 -> 返回空数组
//     * 2. 点位信息重复 -> 返回空数组
//     */
//    public Double[] generate() {
//        if (values.size() == 0) {
//            return new Double[]{};
//        }
//        Map<String, Double> result = new HashMap<>();
//        Double[] msg = new Double[]{};
//
//        int count = 0;
//        for (SensorMsg sensorMsg : values) {
//            String key = getKey((String.valueOf(sensorMsg.id)));
//            // 数据中可能存在2个相同的key (brand 和 batch)
//            if (sensorMsg.id instanceof String && mapping.containsValue(sensorMsg.id) && !result.containsKey(key)) {
//                if (((String.valueOf(sensorMsg.id)) ).startsWith("5H.5H")) {
//                    count++;
//                    result.put(key, (Double) sensorMsg.v);
//                }
//            }
//        }
//        if (count != requiredCount) {
//            logger.error("Time in " + getTimestamp() + " is missing or duplicate value: current=" + count + " while required=" + requiredCount);
//            return new Double[]{};
//        }
//        msg = new Double[]{
//                result.get("humidOut") - result.get("humidSetting"),
//                result.get("windSpeed"),
//                result.get("humidOut"),
//                result.get("humidIn"),
//                result.get("press"),
//                result.get("tempActual1"),
//                result.get("tempSetting1"),
//                result.get("tempActual2"),
//                result.get("tempSetting2"),
//        };
//
//        return msg;
//    }


}
