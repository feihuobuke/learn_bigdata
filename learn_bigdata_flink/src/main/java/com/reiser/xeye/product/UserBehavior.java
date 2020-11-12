package com.reiser.xeye.product;

/**
 * @author: reiserx
 * Date:2020/11/6
 * Des:用户行为类
 */
public class UserBehavior {
    private Long userId;
    private Long productId;
    private int categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior(Long userId, Long productId, int categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.productId = productId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
