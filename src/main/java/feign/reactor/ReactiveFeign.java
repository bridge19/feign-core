/**
 * Copyright 2018 The Feign Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package feign.reactor;

import feign.*;
import feign.InvocationHandlerFactory.MethodHandler;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.codec.ErrorDecoder;
import feign.reactor.client.ReactiveClientFactory;
import feign.reactor.client.ReactiveHttpClient;
import feign.reactor.client.ReactiveHttpRequestInterceptor;
import feign.reactor.client.statushandler.ReactiveStatusHandler;
import feign.reactor.client.statushandler.ReactiveStatusHandlers;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static feign.Util.checkNotNull;
import static feign.Util.isDefault;
import static feign.reactor.client.InterceptorReactiveHttpClient.intercept;
import static feign.reactor.client.LoggerReactiveHttpClient.log;
import static feign.reactor.client.ResponseMappers.ignore404;
import static feign.reactor.client.ResponseMappers.mapResponse;
import static feign.reactor.client.RetryReactiveHttpClient.retry;
import static feign.reactor.client.StatusHandlerReactiveHttpClient.handleStatus;

/**
 * Allows Feign interfaces to accept {@link Publisher} as body and return reactive {@link Mono} or
 * {@link Flux}.
 *
 * @author Sergii Karpenko
 */
public class ReactiveFeign {

  private final ParseHandlersByName targetToHandlersByName;
  private final InvocationHandlerFactory factory;
  private final QueryMapEncoder queryMapEncoder;

  protected ReactiveFeign(
      final ParseHandlersByName targetToHandlersByName,
      final InvocationHandlerFactory factory,final QueryMapEncoder queryMapEncoder) {
    this.targetToHandlersByName = targetToHandlersByName;
    this.factory = factory;
    this.queryMapEncoder = queryMapEncoder;
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  @SuppressWarnings("unchecked")
  public <T> T newInstance(Target<T> target) {
    final Map<String, MethodHandler> nameToHandler = targetToHandlersByName.apply(target);
    final Map<Method, MethodHandler> methodToHandler = new LinkedHashMap<>();
    final List<DefaultMethodHandler> defaultMethodHandlers = new LinkedList<>();

    for (final Method method : target.type().getMethods()) {
      if (isDefault(method)) {
        final DefaultMethodHandler handler = new DefaultMethodHandler(method);
        defaultMethodHandlers.add(handler);
        methodToHandler.put(method, handler);
      } else {
        methodToHandler.put(method,
            nameToHandler.get(Feign.configKey(target.type(), method)));
      }
    }

    final InvocationHandler handler = factory.create(target, methodToHandler);
    T proxy = (T) Proxy.newProxyInstance(target.type().getClassLoader(),
        new Class<?>[] {target.type()}, handler);

    for (final DefaultMethodHandler defaultMethodHandler : defaultMethodHandlers) {
      defaultMethodHandler.bindTo(proxy);
    }

    return proxy;
  }

  /**
   * ReactiveFeign builder.
   */
  public static class Builder<T> {
    protected Contract contract = new Contract.Default();
    protected Function<MethodMetadata, ReactiveHttpClient> clientFactory;
    protected ReactiveHttpRequestInterceptor requestInterceptor;
    protected BiFunction<MethodMetadata, Response, Response> responseMapper;
    protected ReactiveStatusHandler statusHandler =
        ReactiveStatusHandlers.defaultFeign(new ErrorDecoder.Default());
    protected InvocationHandlerFactory invocationHandlerFactory =
        new ReactiveInvocationHandler.Factory();
    protected boolean decode404 = false;
    protected Target<T> target;
    private Encoder encoder = new Encoder.Default();
    protected Decoder decoder;
    private ErrorDecoder errorDecoder = new ErrorDecoder.Default();
    private Logger.Level logLevel = Logger.Level.FULL;
    private Logger logger = new Logger.JavaLogger();
    private QueryMapEncoder queryMapEncoder = new QueryMapEncoder.Default();
    private Request.Options options = new Request.Options();

    private Function<Flux<Throwable>, Flux<Throwable>> retryFunction;

    protected Builder(){
    }

    public Builder<T> clientFactory(Function<MethodMetadata, ReactiveHttpClient> clientFactory) {
      this.clientFactory = clientFactory;
      return this;
    }

    /**
     * @param contract contract.
     * @return this builder
     */
    public Builder<T> contract(final Contract contract) {
      this.contract = contract;
      return this;
    }

    public Builder<T> requestInterceptor(ReactiveHttpRequestInterceptor requestInterceptor) {
      this.requestInterceptor = requestInterceptor;
      return this;
    }

    /**
     * This flag indicates that the reactive feign client should process responses with 404 status,
     * specifically returning empty {@link Mono} or {@link Flux} instead of throwing
     * {@link FeignException}.
     * <p>
     * <p>
     * This flag only works with 404, as opposed to all or arbitrary status codes. This was an
     * explicit decision: 404 - empty is safe, common and doesn't complicate redirection, retry or
     * fallback policy.
     *
     * @return this builder
     */
    public Builder<T> decode404() {
      this.decode404 = true;
      return this;
    }

    public Builder<T> statusHandler(ReactiveStatusHandler statusHandler) {
      this.statusHandler = statusHandler;
      return this;
    }

    /**
     * The most common way to introduce custom logic on handling http response
     *
     * @param responseMapper
     * @return
     */
    public Builder<T> responseMapper(BiFunction<MethodMetadata, Response, Response> responseMapper) {
      this.responseMapper = responseMapper;
      return this;
    }

    public Builder<T> encoder(Encoder encoder) {
      this.encoder = encoder;
      return this;
    }

    public Builder<T> decoder(Decoder decoder) {
      this.decoder = decoder;
      return this;
    }

    public Builder<T> retryWhen(
                                Function<Flux<Throwable>, Flux<Throwable>> retryFunction) {
      this.retryFunction = retryFunction;
      return this;
    }

    public Builder<T> retryWhen(ReactiveRetryPolicy retryPolicy) {
      return retryWhen(retryPolicy.toRetryFunction());
    }

    /**
     * Defines target and builds client.
     *
     * @param apiType API interface
     * @param url base URL
     * @return built client
     */
    public T target(final Class<T> apiType, final String url) {
      return target(new Target.HardCodedTarget<>(apiType, url));
    }

    /**
     * Defines target and builds client.
     *
     * @param target target instance
     * @return built client
     */
    public T target(final Target<T> target) {
      this.target = target;
      return build().newInstance(target);
    }

    protected ReactiveFeign build() {
      final ParseHandlersByName handlersByName = new ParseHandlersByName(
              contract,options,encoder,decoder,queryMapEncoder,errorDecoder, buildReactiveMethodHandlerFactory());
      return new ReactiveFeign(handlersByName, invocationHandlerFactory,queryMapEncoder);
    }

    protected ReactiveMethodHandlerFactory buildReactiveMethodHandlerFactory() {
      return new ReactiveClientMethodHandler.Factory(buildReactiveClientFactory(),decoder);
    }

    protected ReactiveClientFactory buildReactiveClientFactory() {
      return methodMetadata -> {

        checkNotNull(clientFactory,
                "clientFactory wasn't provided in ReactiveFeign builder");

        ReactiveHttpClient reactiveClient = clientFactory.apply(methodMetadata);

        if (requestInterceptor != null) {
          reactiveClient = intercept(reactiveClient, requestInterceptor);
        }

        reactiveClient = log(reactiveClient, methodMetadata,logger,logLevel);

        if (responseMapper != null) {
          reactiveClient = mapResponse(reactiveClient, methodMetadata, responseMapper);
        }

        if (decode404) {
          reactiveClient = mapResponse(reactiveClient, methodMetadata, ignore404());
        }

        if (statusHandler != null) {
          reactiveClient = handleStatus(reactiveClient, methodMetadata, statusHandler);
        }

        if (retryFunction != null) {
          reactiveClient = retry(reactiveClient, methodMetadata, retryFunction,logger,logLevel);
        }

        return reactiveClient;
      };
    }
  }

  public static final class ParseHandlersByName {
    private final Contract contract;
    private final Request.Options options;
    private final Encoder encoder;
    private final Decoder decoder;
    private final ErrorDecoder errorDecoder;
    private final QueryMapEncoder queryMapEncoder;
    private final ReactiveMethodHandlerFactory factory;

    ParseHandlersByName(
        Contract contract,
        Request.Options options,
        Encoder encoder,
        Decoder decoder,
        QueryMapEncoder queryMapEncoder,
        ErrorDecoder errorDecoder,
        ReactiveMethodHandlerFactory factory) {
      this.contract = contract;
      this.options = options;
      this.factory = factory;
      this.errorDecoder = errorDecoder;
      this.queryMapEncoder = queryMapEncoder;
      this.encoder = checkNotNull(encoder, "encoder");
      this.decoder = checkNotNull(decoder, "decoder");
    }

    Map<String, MethodHandler> apply(final Target target) {
      List<MethodMetadata> metadata = contract.parseAndValidatateMetadata(target.type());
      Map<String, MethodHandler> result = new LinkedHashMap<String, MethodHandler>();
      for (MethodMetadata md : metadata) {
        ReflectiveFeign.BuildTemplateByResolvingArgs buildTemplate;
        if (!md.formParams().isEmpty() && md.template().bodyTemplate() == null) {
          buildTemplate = new ReflectiveFeign.BuildFormEncodedTemplateFromArgs(md, encoder, queryMapEncoder);
        } else if (md.bodyIndex() != null) {
          buildTemplate = new ReflectiveFeign.BuildEncodedTemplateFromArgs(md, encoder, queryMapEncoder);
        } else {
          buildTemplate = new ReflectiveFeign.BuildTemplateByResolvingArgs(md, queryMapEncoder);
        }
        result.put(md.configKey(),
            factory.create(target, md,buildTemplate));
      }
      return result;
    }
  }
}
