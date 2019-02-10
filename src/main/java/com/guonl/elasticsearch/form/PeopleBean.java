package com.guonl.elasticsearch.form;

import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.util.Date;

/**
 * Created by guonl
 * Date 2019/2/1 1:59 PM
 * Description:
 */
@Data
public class PeopleBean {

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date date;

    private String country;

    private String name;

    private Integer age;

    private String id;

}
