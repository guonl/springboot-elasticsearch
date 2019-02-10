package com.guonl.elasticsearch.controller;

import com.guonl.elasticsearch.form.PeopleBean;
import com.guonl.elasticsearch.form.ReportBean;
import com.guonl.elasticsearch.util.ESUtil;
import com.guonl.elasticsearch.util.FrontResult;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.UUID;

/**
 * Created by guonl
 * Date 2019/1/29 2:06 PM
 * Description: 通过json的方式
 */
@Slf4j
@RestController
@RequestMapping("/es")
public class ESPeopleController {

    @Autowired
    private ESUtil esUtil;
    @Autowired
    private TransportClient client;

    @ResponseBody
    @RequestMapping(value = "/add/index",method = RequestMethod.GET)
    public FrontResult addIndex() throws Exception {

        ReportBean bean = new ReportBean();
        bean.setGiftId(1);
        bean.setGiftName("我的测试");
        bean.setCostScore(100);

        IndexResponse insert = esUtil.insert("guonl", "report", bean, ReportBean.class, "giftId");
        return FrontResult.success(insert);
    }

    /**
     * 新增数据
     * @return
     * @throws Exception
     */
    @ResponseBody
    @RequestMapping(value = "/add/people",method = RequestMethod.GET)
    public FrontResult addPeople() throws Exception {
        PeopleBean bean = new PeopleBean();
        bean.setName("晴岚");
        bean.setAge(20);
        bean.setCountry("中国");
        bean.setDate(new Date());
        bean.setId(UUID.randomUUID().toString());
        IndexResponse insert = esUtil.insert("people", "man", bean, PeopleBean.class, "id");
        return FrontResult.success(insert);
    }

    //查询接口
    @GetMapping("/get/man")
    public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {
        if (StringUtils.isEmpty(id)) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        GetResponse response = this.client.prepareGet("people", "man", id).get();
        if (!response.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity(response.getSource(), HttpStatus.OK);

    }





}
