package org.workspace7.demos;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.workspace7.demos.model.Status;
import org.workspace7.demos.stream.JsonChunkWriteStream;
import org.workspace7.model.OAuth1Param;
import org.workspace7.util.GeneralUtil;
import org.workspace7.util.TwitterOAuth1Util;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;

/**
 * 
 *
 */
public class App extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private KafkaProducer<String, String> kafkaProducer;
    private final Map<String, String> kafkaProducerConfig = new HashMap<>();

    public void start() {

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        // Default home
        router.get("/").handler(this::handleHome);

        // Stream relatime status from twitter
        router.post("/statuses").handler(this::handleStatuses);

        vertx.createHttpServer().requestHandler(router).listen(8080);

        kafkaProducerConfig.put("bootstrap.servers", config().getString("bootstrap.servers"));
        kafkaProducerConfig.put("acks", "1");
        kafkaProducerConfig.put("key.serializer", config().getString("key.serializer"));
        kafkaProducerConfig.put("value.serializer", config().getString("value.serializer"));
    }


    /**
     * 
     * @param routingContext
     */
    private void handleHome(RoutingContext routingContext) {
        routingContext.response().end("OK");
    }

    /**
     * 
     * @param routingContext
     */
    private void handleStatuses(RoutingContext routingContext) {
        kafkaProducer = KafkaProducer.create(vertx, kafkaProducerConfig);
        String consumerKey = config().getJsonObject("twitter").getString("consumerKey");
        String consumerSecret = config().getJsonObject("twitter").getString("consumerSecret");
        String token = config().getJsonObject("twitter").getString("token");
        String tokenSecret = config().getJsonObject("twitter").getString("tokenSecret");
        String streamUrl = config().getJsonObject("twitter").getString("streamURI",
                "https: //stream.twitter.com/1.1/statuses/filter.json");

        // Print the config
        Preconditions.checkNotNull(consumerKey,
                "Twitter API requires %s, visit https://developer.twitter.com", "consumerKey");
        Preconditions.checkNotNull(consumerSecret,
                "Twitter API requires %s, visit https://developer.twitter.com", "consumerSecret");
        Preconditions.checkNotNull(token,
                "Twitter API requires %s, visit https://developer.twitter.com", "token");
        Preconditions.checkNotNull(tokenSecret,
                "Twitter API requires %s, visit https://developer.twitter.com", "tokenSecret");

        Map<String, String> getParams = Collections.emptyMap();

        JsonObject requestBody = routingContext.getBodyAsJson();

        LOGGER.info("Request Body {}", requestBody);

        if (requestBody == null || requestBody.getMap().isEmpty()) {
            routingContext.response().end("Required valid request body");
        }

        Map<String, String> postParams = new HashMap<>();
        requestBody.getMap().forEach((k, v) -> postParams.put(k, String.valueOf(v)));

        if (!postParams.containsKey("language")) {
            postParams.put("language", "en");
        }

        LOGGER.info("Post Params {}", postParams);

        OAuth1Param oAuth1Param = new OAuth1Param(consumerKey, GeneralUtil.nonce(), "",
                GeneralUtil.timestampString(), token);

        try {
            URI requestURI = new URI(streamUrl);
            String strOAuth = TwitterOAuth1Util.oauth1HeaderString(requestURI, "post", getParams,
                    postParams, oAuth1Param, consumerSecret, tokenSecret);

            WebClient webClient = WebClient.create(vertx);

            JsonChunkWriteStream stream = new JsonChunkWriteStream();

            webClient.postAbs(streamUrl)
                    .as(new BodyCodec<>(io.vertx.ext.web.codec.BodyCodec.pipe(stream)))
                    .putHeader("Authorization", strOAuth)
                    .rxSendForm(MultiMap.caseInsensitiveMultiMap().addAll(postParams))
                    .doOnSuccess(x -> System.out.println(x)).subscribe();

            stream.toFlowable().map(jObj -> Json.mapper.convertValue(jObj, Status.class))
                    .subscribe(sObj -> {
                        String strChunk = Json.encode(sObj);
                        LOGGER.info("Got Status: {}", strChunk);
                        publishToKafka(postParams, strChunk);
                        routingContext.response().setChunked(true).write(strChunk, "UTF-8");
                    }, err -> {
                        LOGGER.error("Error processing status", err);
                        routingContext.response().setStatusCode(503)
                                .end("Error processing message " + err.getMessage());
                    }, () -> {
                        LOGGER.info("Status DONE");
                    });

        } catch (InvalidKeyException e) {
            LOGGER.error("Error processing status", e);
            routingContext.response().setStatusCode(503)
                    .end("Error processing message " + e.getMessage());
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Error processing status", e);
            routingContext.response().setStatusCode(503)
                    .end("Error processing message " + e.getMessage());
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Error processing status", e);
            routingContext.response().setStatusCode(503)
                    .end("Error processing message " + e.getMessage());
        } catch (URISyntaxException e) {
            LOGGER.error("Error processing status", e);
            routingContext.response().setStatusCode(503)
                    .end("Error processing message " + e.getMessage());
        }
    }

    /**
     * 
     * @param postParams
     * @param jObj
     */
    private void publishToKafka(Map<String, String> postParams, String value) {
        String key = postParams.get("track");
        KafkaProducerRecord<String, String> record =
                KafkaProducerRecord.create(config().getString("topic_name"), value);
        kafkaProducer.rxWrite(record).subscribe(metadata -> {
            LOGGER.info("Successfuly written record to Kafka topic {} Offset {} Partition @ {} ",
                    metadata.getTopic(), metadata.getOffset(), metadata.getPartition(),
                    new Date(metadata.getTimestamp()));
        }, err -> {
            LOGGER.error("Error writing record to Kafka", err);
        });
    }
}
