package com.datamodel;

import com.datamodel.config.AppProperties;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
@MapperScan("com.datamodel.mapper")
public class DataModelApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataModelApplication.class, args);
    }
}
