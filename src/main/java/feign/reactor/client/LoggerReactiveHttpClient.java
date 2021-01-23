/**
 * Copyright 2018 The Feign Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package feign.reactor.client;

import feign.Logger;
import feign.MethodMetadata;
import feign.Request;
import feign.Response;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static feign.reactor.utils.FeignUtils.methodTag;

/**
 * Wraps {@link ReactiveHttpClient} with log logic
 *
 * @author Sergii Karpenko
 */
public class LoggerReactiveHttpClient implements ReactiveHttpClient {

  private final Logger logger;
  private final Logger.Level loggerLevel;
  private final ReactiveHttpClient reactiveClient;
  private final String methodTag;

  public static ReactiveHttpClient log(ReactiveHttpClient reactiveClient, MethodMetadata methodMetadata, Logger logger, Logger.Level loggerLevel) {
    return new LoggerReactiveHttpClient(reactiveClient, methodMetadata, logger, loggerLevel);
  }

  private LoggerReactiveHttpClient(ReactiveHttpClient reactiveClient,
                                   MethodMetadata methodMetadata, Logger logger, Logger.Level loggerLevel) {
    this.reactiveClient = reactiveClient;
    this.methodTag = methodTag(methodMetadata);
    this.logger = logger;
    this.loggerLevel = loggerLevel;
  }

  @Override
  public Mono<Response> executeRequest(Mono<Request> requestMono) {

    AtomicLong start = new AtomicLong(-1);
    return requestMono.flatMap(request -> {
      start.set(System.currentTimeMillis());
      logger.logRequest(methodTag, loggerLevel, request);

      return reactiveClient.executeRequest(Mono.just(request))
          .doOnNext(response -> {
            try {
              logger.logAndRebufferResponse(methodTag, loggerLevel, response, System.currentTimeMillis() - start.get());
            } catch (IOException e) {
              throw new RuntimeException("get body error.");
            }
          })
          .doOnError(e -> {
            IOException exception = null;
            if(e instanceof IOException) {
              logger.logIOException(methodTag, loggerLevel, (IOException)e, System.currentTimeMillis() - start.get());
            }else if(e instanceof RuntimeException){
              throw (RuntimeException)e;
            }else{
              throw new RuntimeException("invoke error.",e);
            }
          });
    });
  }
}
