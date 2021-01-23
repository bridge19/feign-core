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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import static feign.reactor.utils.FeignUtils.methodTag;

/**
 * Wraps {@link ReactiveHttpClient} with retry logic provided by retryFunction
 *
 * @author Sergii Karpenko
 */
public class RetryReactiveHttpClient<T> implements ReactiveHttpClient {

  private final Logger logger;
  private final Logger.Level loggerLevel;
  private final String feignMethodTag;
  private final ReactiveHttpClient reactiveClient;
  private final Function<Flux<Throwable>, Flux<?>> retryFunction;

  public static <T> ReactiveHttpClient retry(
      ReactiveHttpClient reactiveClient,
      MethodMetadata methodMetadata,
      Function<Flux<Throwable>, Flux<Throwable>> retryFunction,Logger logger,Logger.Level loggerLevel) {
    return new RetryReactiveHttpClient<>(reactiveClient, methodMetadata, retryFunction,logger,loggerLevel);
  }

  private RetryReactiveHttpClient(ReactiveHttpClient reactiveClient,
                                  MethodMetadata methodMetadata,
                                  Function<Flux<Throwable>, Flux<Throwable>> retryFunction,
                                  Logger logger,
                                  Logger.Level loggerLevel) {
    this.reactiveClient = reactiveClient;
    this.feignMethodTag = methodTag(methodMetadata);
    this.retryFunction = wrapWithLog(retryFunction, feignMethodTag);
    this.logger = logger;
    this.loggerLevel = loggerLevel;
  }

  @Override
  public Mono<Response> executeRequest(Mono<Request> requestMono) {
    return reactiveClient.executeRequest(requestMono)
        .retryWhen(retryFunction)
        .onErrorMap(outOfRetries());
  }

  private Function<Throwable, Throwable> outOfRetries() {
    return throwable -> {
      logger.logRetry(feignMethodTag,loggerLevel);
      return new OutOfRetriesException(throwable, feignMethodTag);
    };
  }

  private Function<Flux<Throwable>, Flux<?>> wrapWithLog(
      Function<Flux<Throwable>, Flux<Throwable>> retryFunction,
      String feignMethodTag) {
    return throwableFlux -> retryFunction.apply(throwableFlux)
        .doOnNext(throwable -> {
            logger.logRetry(feignMethodTag,loggerLevel);
        });
  }

  public static class OutOfRetriesException extends Exception {
    OutOfRetriesException(Throwable cause, String feignMethodTag) {
      super("All retries used for: " + feignMethodTag, cause);
    }
  }
}
