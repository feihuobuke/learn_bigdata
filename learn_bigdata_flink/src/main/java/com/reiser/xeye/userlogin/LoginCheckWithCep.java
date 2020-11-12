package com.reiser.xeye.userlogin;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * @author: reiserx
 * Date:2020/11/8
 * Des:5秒内连续3次登陆失败
 * <p>
 * CEP 就是 「做筛选」
 */
public class LoginCheckWithCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<LoginEvent> sourceStream = env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data4.csv").map(line -> {
            String[] fields = line.split(",");

            return new LoginEvent(Long.parseLong(fields[0].trim()), fields[1].trim(), fields[2].trim(), Long.parseLong(fields[3].trim()));
        }).assignTimestampsAndWatermarks(new LoginCheckEventTimeExtractor(10000));


        Pattern<LoginEvent, LoginEvent> loginFailTwiceInThreeSecondsEventPattern = Pattern.<LoginEvent>begin("begin").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).next("next").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).next("next2").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).within(Time.seconds(5));

        PatternStream<LoginEvent> patternStream = CEP.pattern(sourceStream, loginFailTwiceInThreeSecondsEventPattern);

        patternStream.select(new LoginFailMatch()).print();

        env.execute(LoginCheckWithCep.class.getSimpleName());


    }

    private static class LoginFailMatch implements PatternSelectFunction<LoginEvent, Warning> {
        @Override
        public Warning select(Map<String, List<LoginEvent>> map) throws Exception {
            // 从map中按照名称取出对应的事件
            LoginEvent firstFail = map.get("begin").iterator().next();
            LoginEvent lastFail = map.get("next").iterator().next();
            LoginEvent lastFail2 = map.get("next2").iterator().next();
            return new Warning(firstFail.getUserId(), firstFail.eventTime, lastFail2.getEventTime(), "login fail 3 times");
        }
    }


    private static class LoginCheckEventTimeExtractor implements AssignerWithPeriodicWatermarks<LoginEvent> {

        long maxEventTime = 0L;
        long maxOufOfOrderness;

        LoginCheckEventTimeExtractor(int maxOufOfOrderness) {
            this.maxOufOfOrderness = maxOufOfOrderness;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxEventTime - maxOufOfOrderness);
        }

        @Override
        public long extractTimestamp(LoginEvent element, long previousElementTimestamp) {
            long currentEventTime = element.getEventTime() * 1000;
            maxEventTime = Math.max(currentEventTime, maxEventTime);
            return currentEventTime;
        }
    }


    // 输入的登录事件样例类
    static class LoginEvent {
        private Long userId;
        private String ip;
        private String eventType;
        private Long eventTime;

        public LoginEvent(Long userId, String ip, String eventType, Long eventTime) {
            this.userId = userId;
            this.ip = ip;
            this.eventType = eventType;
            this.eventTime = eventTime;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public Long getEventTime() {
            return eventTime;
        }

        public void setEventTime(Long eventTime) {
            this.eventTime = eventTime;
        }
    }

    // 输出的异常报警信息样例类
    static class Warning {
        Long userId;
        Long firstFailTime;
        Long lastFailTime;
        String warningMsg;

        public Warning(Long userId, Long firstFailTime, Long lastFailTime, String warningMsg) {
            this.userId = userId;
            this.firstFailTime = firstFailTime;
            this.lastFailTime = lastFailTime;
            this.warningMsg = warningMsg;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getFirstFailTime() {
            return firstFailTime;
        }

        public void setFirstFailTime(Long firstFailTime) {
            this.firstFailTime = firstFailTime;
        }

        public Long getLastFailTime() {
            return lastFailTime;
        }

        public void setLastFailTime(Long lastFailTime) {
            this.lastFailTime = lastFailTime;
        }

        public String getWarningMsg() {
            return warningMsg;
        }

        public void setWarningMsg(String warningMsg) {
            this.warningMsg = warningMsg;
        }

        @Override
        public String toString() {
            return "Warning{" +
                    "userId=" + userId +
                    ", firstFailTime=" + firstFailTime +
                    ", lastFailTime=" + lastFailTime +
                    ", warningMsg='" + warningMsg + '\'' +
                    '}';
        }
    }


}
