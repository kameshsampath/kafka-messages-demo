package org.workspace7.demos;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.workspace7.demos.model.Status;
import org.workspace7.model.OAuth1Param;
import org.workspace7.util.GeneralUtil;
import org.workspace7.util.TwitterOAuth1Util;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.reactivex.core.parsetools.JsonParser;

/**
 * 
 *
 */
public class App extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public void start() {

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        // Default home
        router.get("/").handler(this::handleHome);

        // Stream relatime status from twitter
        router.post("/statuses").handler(this::handleStatuses);

        vertx.createHttpServer().requestHandler(router).listen(8080);
    }


    private void handleHome(RoutingContext routingContext) {
        routingContext.response().end("OK");
    }

    private void handleStatuses(RoutingContext routingContext) {
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


        // TODO get from params
        String searchTerms = "knative,#knative,serverless,#serverless";
        Map<String, String> getParams = new HashMap<>();
        Map<String, String> postParams = new HashMap<>();
        postParams.put("track", searchTerms);
        // filterParams.put("filter_level","medium");
        postParams.put("language", "en");

        OAuth1Param oAuth1Param = new OAuth1Param(consumerKey, GeneralUtil.nonce(), "",
                GeneralUtil.timestampString(), token);

        try {
            URI requestURI = new URI(streamUrl);
            String strOAuth = TwitterOAuth1Util.oauth1HeaderString(requestURI, "post", getParams,
                    postParams, oAuth1Param, consumerSecret, tokenSecret);

            WebClient webClient = WebClient.create(vertx);

            JsonParser parser = JsonParser.newParser().objectValueMode();

            parser.toFlowable().map(j -> {
                Status s = Json.mapper.convertValue(j.objectValue().getMap(), Status.class);
                return s;
            }).subscribe(e -> {
                LOGGER.info("{}", Json.encodePrettily(e));
            }, Throwable::printStackTrace);

            webClient.postAbs(streamUrl).as(BodyCodec.pipe(new WriteStream<Buffer>() {

                @Override
                public boolean writeQueueFull() {
                    return false;
                }

                @Override
                public WriteStream<Buffer> write(Buffer data) {
                    LOGGER.trace("Buffer:{} length = {}", data, data.toString().trim().length());
                    if (data != null && data.toString().trim().length() > 0) {
                        parser.handle(
                                io.vertx.reactivex.core.buffer.Buffer.buffer(data.toString()));
                    }
                    return this;
                }

                @Override
                public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
                    return this;
                }

                @Override
                public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
                    return this;
                }

                @Override
                public void end() {
                    parser.end();
                }

                @Override
                public WriteStream<Buffer> drainHandler(@Nullable Handler<Void> handler) {
                    return this;
                }
            })).putHeader("Authorization", strOAuth)
                    .sendForm(MultiMap.caseInsensitiveMultiMap().addAll(postParams), ar -> {
                        if (ar.failed()) {
                            LOGGER.error("Error", ar.cause());
                        } else {
                            LOGGER.info("Getting messages");
                        }
                    });

        } catch (InvalidKeyException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
