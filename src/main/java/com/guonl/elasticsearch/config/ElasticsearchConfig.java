package com.guonl.elasticsearch.config;

import com.guonl.elasticsearch.util.ESUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by guonl
 * Date 2019/1/29 2:43 PM
 * Description:
 */
@Configuration
public class ElasticsearchConfig {

    @Value("${elasticsearch.address}")
    private String esAddress;

    @Value("${elasticsearch.cluster.name}")
    private String esName;

    @Bean
    public TransportClient esClient() throws UnknownHostException {

        Settings settings = Settings.builder()
                .put("cluster.name", this.esName)
                .put("client.transport.sniff", true)
//                .put("shield.user", "tribe_user:tribe_user")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);

        String[] addresses = esAddress.split(",");
        for (String address : addresses) {
            String[] split = address.split(":");
            InetSocketTransportAddress node = new InetSocketTransportAddress(
                    InetAddress.getByName(split[0]), Integer.parseInt(split[1])
            );
            client.addTransportAddress(node);
        }
        return client;
    }

    @Bean
    public ESUtil esUtil() throws UnknownHostException {
        ESUtil esUtil = new ESUtil();
        esUtil.setTransportClient(esClient());
        return esUtil;
    }


}
