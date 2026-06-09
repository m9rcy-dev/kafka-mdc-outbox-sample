package com.example.kafkaoutbox.config;

import com.example.kafkaoutbox.common.BadInputException;
import com.example.kafkaoutbox.crypto.EncryptingByteArraySerializer;
import com.example.kafkaoutbox.crypto.PayloadCrypto;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
@EnableConfigurationProperties({AppKafkaProperties.class, AppOutboxProperties.class})
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    @Primary
    ProducerFactory<String, byte[]> rawProducerFactory(KafkaProperties kafkaProperties,
                                                       SslBundles sslBundles) {
        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(sslBundles),
                new StringSerializer(),
                new ByteArraySerializer()
        );
    }

    @Bean
    @Primary
    KafkaTemplate<String, byte[]> rawKafkaTemplate(
            @Qualifier("rawProducerFactory") ProducerFactory<String, byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    ProducerFactory<String, byte[]> processedProducerFactory(KafkaProperties kafkaProperties,
                                                            SslBundles sslBundles,
                                                            PayloadCrypto payloadCrypto) {
        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(sslBundles),
                new StringSerializer(),
                new EncryptingByteArraySerializer(payloadCrypto)
        );
    }

    @Bean
    KafkaTemplate<String, byte[]> processedKafkaTemplate(
            @Qualifier("processedProducerFactory") ProducerFactory<String, byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory,
            KafkaMdcRecordInterceptor interceptor,
            DefaultErrorHandler errorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRecordInterceptor(interceptor);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Keep concurrency at 1 for simplest local ordering demo. In production, ordering is per partition/key.
        factory.setConcurrency(1);
        return factory;
    }

    @Bean
    DefaultErrorHandler defaultErrorHandler(@Qualifier("rawKafkaTemplate") KafkaTemplate<String, byte[]> kafkaTemplate,
                                            AppKafkaProperties properties) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + properties.dlqSuffix(), record.partition())
        );

        var backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(10_000L);
        backOff.setMaxElapsedTime(30_000L);

        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(BadInputException.class, IllegalArgumentException.class);
        handler.setRetryListeners((record, ex, attempt) -> log.warn(
                "Retrying record topic={} partition={} offset={} attempt={}",
                record.topic(), record.partition(), record.offset(), attempt, ex));
        return handler;
    }
}
