package org.workspace7.demos.stream;

import io.reactivex.Flowable;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import io.vertx.reactivex.core.parsetools.JsonEvent;
import io.vertx.reactivex.core.parsetools.JsonParser;

/**
 * JsonChunkWriteStream
 */
public class JsonChunkWriteStream implements WriteStream<Buffer> {

    private JsonParser parser;

    public JsonChunkWriteStream() {
        parser = JsonParser.newParser().objectValueMode();
    }

    public Flowable<JsonObject> toFlowable() {
        return this.parser.toFlowable().map(JsonEvent::objectValue);
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        parser.exceptionHandler(handler);
        return this;
    }

    @Override
    public WriteStream<Buffer> write(Buffer data) {
        parser.write(io.vertx.reactivex.core.buffer.Buffer.newInstance(data));
        return this;
    }

    @Override
    public void end() {

    }

    @Override
    public void end(Buffer data) {
        parser.write(io.vertx.reactivex.core.buffer.Buffer.newInstance(data));
        parser.end();
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public WriteStream<Buffer> drainHandler(@Nullable Handler<Void> handler) {
        return this;
    }

}
