package com.reiser.xeye.etl;

/**
 * @author: reiserx
 * Date:2020/11/8
 * Des:
 */
public class RealTimeEvent {
    private String dt;
    private long eventTime;
    private String area;
    private String type;
    private double score;
    private String level;
    private String countryCode;

    public RealTimeEvent(String dt, long eventTime, String countryCode, String type, double score, String level) {
        this.dt = dt;
        this.eventTime = eventTime;
        this.countryCode = countryCode;
        this.type = type;
        this.score = score;
        this.level = level;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

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

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    @Override
    public String toString() {
        return "RealTimeEvent{" +
                "dt='" + dt + '\'' +
                ", area='" + area + '\'' +
                ", type='" + type + '\'' +
                ", score=" + score +
                ", level='" + level + '\'' +
                ", countryCode='" + countryCode + '\'' +
                '}';
    }
}
