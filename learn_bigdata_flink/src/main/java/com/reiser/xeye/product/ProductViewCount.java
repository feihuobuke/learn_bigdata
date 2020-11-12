package com.reiser.xeye.product;

/**
 * @author: reiserx
 * Date:2020/11/6
 * Des:产品pv
 */
public class ProductViewCount {
    private Long productId;
    private Long windowEnd;
    private Long count;

    public ProductViewCount(Long productId, Long windowEnd, Long count) {
        this.productId = productId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
