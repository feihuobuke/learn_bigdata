package com.reiser.sparktop;

import java.io.Serializable;

/**
 * @author: reiserx
 * Date:2020/11/24
 * Des:
 */
public class Log implements Serializable {
    Long categoryId;
    Long clickCount;
    Long orderCount;
    Long payCount;

    public Log(Long categoryId, Long clickCount, Long orderCount, Long payCount) {
        this.categoryId = categoryId;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getPayCount() {
        return payCount;
    }

    public void setPayCount(Long payCount) {
        this.payCount = payCount;
    }

    @Override
    public String toString() {
        return "category_id=" + categoryId +
                "｜click_category_count=" + clickCount +
                "｜order_category_count=" + orderCount +
                "｜pay_category_count=" + payCount;
    }


}
