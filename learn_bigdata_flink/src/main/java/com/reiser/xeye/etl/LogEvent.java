package com.reiser.xeye.etl;

import java.util.List;

/**
 * @author: reiserx
 * Date:2020/11/8
 * Des:
 */
public class LogEvent {

    /**
     * dt : 2019-11-19 20:33:39
     * countryCode : TW
     * data : [{"type":"s1","score":0.8,"level":"D"},{"type":"s2","score":0.1,"level":"B"}]
     */

    private String dt;
    private String countryCode;
    private List<DataBean> data;

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public List<DataBean> getData() {
        return data;
    }

    public void setData(List<DataBean> data) {
        this.data = data;
    }

    public static class DataBean {
        /**
         * type : s1
         * score : 0.8
         * level : D
         */

        private String type;
        private double score;
        private String level;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }
    }
}
