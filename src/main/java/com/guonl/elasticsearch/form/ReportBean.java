package com.guonl.elasticsearch.form;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Created by guonl
 * Date 2019/1/29 2:10 PM
 * Description:
 */
@Data
public class ReportBean {
    private String mallName;
    private String mallId;
    private String giftName;
    private Integer giftId;
    private String specification;
    private Integer convertType;
    private Integer costScore;
    private Date putInDate;
    private BigDecimal money;
    private Integer putInType;
    private Integer putInSum;
    private String putInUserName;
}
