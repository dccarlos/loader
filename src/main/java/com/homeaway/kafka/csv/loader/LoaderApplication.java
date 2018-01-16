package com.homeaway.kafka.csv.loader;

import java.io.FileInputStream;
import java.io.InputStreamReader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.context.annotation.Bean;

import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import au.com.bytecode.opencsv.CSVReader;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class LoaderApplication implements CommandLineRunner {

    @Value(value = "${csv.file}")
    public String file;

    @Value(value = "${csv.file.type}")
    public Integer fileType;

    @Value(value = "${topic}")
    public String topic;

    @Value(value = "${bootstrap.servers}")
    public String bootstrapAddress;
    
    @Value(value = "${schema.registry.url}")
    public String schemaReg;

    // mvn spring-boot:run -Drun.arguments="--bootstrap.servers=srv:9092,--schema.registry.url=schema.reg:8081,--csv.file=../file.csv,--csv.file.type=1,--topic=mytopic"
    // java -jar target/loader-0.0.1-SNAPSHOT.jar --bootstrap.servers=srv:9092 --schema.registry.url=schema.reg:8081 --csv.file=../file.csv --csv.file.type=1 --topic=mytopic
    public static void main(String[] args) {
        SpringApplication.run(LoaderApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(file), "UTF-8"), ',', '"', 1)) {
            String[] values = reader.readNext();

            while (values != null) {
                log.warn("Processing {}", Arrays.toString(values));

                switch (fileType) {
                    case 1 :
                        kafkaTemplate().send(topic, new Type1(values[0], values[1], values[2], values[3], values[4], values[5], values[6]));
                        break;
                    case 2 :
                        kafkaTemplate().send(topic, new Type2(values[0], values[1]));
                        break;
                    case 3 :
                        kafkaTemplate().send(topic,
                                new Type3(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8]));
                        break;
                    default :
                        throw new IllegalArgumentException("Invalid file type " + fileType);
                }

                values = reader.readNext();
            }
        }
    }

    @Bean
    public <T> ProducerFactory<String, T> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        configProps.put("schema.registry.url", schemaReg);

        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public <T> KafkaTemplate<String, T> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}