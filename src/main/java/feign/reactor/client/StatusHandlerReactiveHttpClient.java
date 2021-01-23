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

import feign.MethodMetadata;
import feign.Request;
import feign.Response;
import feign.reactor.client.statushandler.ReactiveStatusHandler;
import reactor.core.publisher.Mono;

import static feign.reactor.utils.FeignUtils.methodTag;

/**
 * Uses statusHandlers to process status of http response
 *
 * @author Sergii Karpenko
 */

public class StatusHandlerReactiveHttpClient implements ReactiveHttpClient {

  private final ReactiveHttpClient reactiveClient;
  private final String methodTag;

  private final ReactiveStatusHandler statusHandler;

  public static ReactiveHttpClient handleStatus(
      ReactiveHttpClient reactiveClient,
      MethodMetadata methodMetadata,
      ReactiveStatusHandler statusHandler) {
    return new StatusHandlerReactiveHttpClient(reactiveClient, methodMetadata, statusHandler);
  }

  private StatusHandlerReactiveHttpClient(ReactiveHttpClient reactiveClient,
                                          MethodMetadata methodMetadata,
                                          ReactiveStatusHandler statusHandler) {
    this.reactiveClient = reactiveClient;
    this.methodTag = methodTag(methodMetadata);
    this.statusHandler = statusHandler;
  }

  @Override
  public Mono<Response> executeRequest(Mono<Request> request) {
    return reactiveClient.executeRequest(request).map(response -> {
      if (statusHandler.shouldHandle(response.status())) {
        throw statusHandler.decode(methodTag, response);
      } else {
        return response;
      }
    });
  }
}
