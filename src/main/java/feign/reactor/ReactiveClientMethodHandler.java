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
package feign.reactor;

import feign.*;
import feign.codec.Decoder;
import feign.reactor.client.ReactiveClientFactory;
import feign.reactor.client.ReactiveHttpClient;
import feign.reactor.utils.Pair;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static feign.Util.checkNotNull;
import static feign.reactor.utils.MultiValueMapUtils.*;
import static java.util.stream.Collectors.*;
import static reactor.core.publisher.Mono.just;

/**
 * Method handler for asynchronous HTTP requests via {@link ReactiveHttpClient}.
 *
 * @author Sergii Karpenko
 */
public class ReactiveClientMethodHandler implements InvocationHandlerFactory.MethodHandler {

  private final Target target;
  private final MethodMetadata methodMetadata;
  private final ReactiveHttpClient reactiveClient;
  private final Function<Map<String, ?>, String> pathExpander;
  private final Map<String, List<Function<Map<String, ?>, String>>> headerExpanders;
  private final Map<String, Collection<String>> queriesAll;
  private final Map<String, List<Function<Map<String, ?>, String>>> queryExpanders;
  private final Type returnType;
  private final Decoder decoder;
  private final RequestTemplate.Factory buildTemplateFromArgs;

  private ReactiveClientMethodHandler(Target target,
                                      MethodMetadata methodMetadata,
                                      ReactiveHttpClient reactiveClient,
                                      Decoder decoder,
                                      RequestTemplate.Factory buildTemplateFromArgs) {
    this.target = checkNotNull(target, "target must be not null");
    this.methodMetadata = checkNotNull(methodMetadata,
        "methodMetadata must be not null");
    this.reactiveClient = checkNotNull(reactiveClient, "client must be not null");
    this.pathExpander = buildExpandFunction(methodMetadata.template().url());
    this.headerExpanders = buildExpanders(methodMetadata.template().headers());
    this.queriesAll = new HashMap<>(methodMetadata.template().queries());
    if (methodMetadata.formParams() != null) {
      methodMetadata.formParams()
          .forEach(param -> add(queriesAll, param, "{" + param + "}"));
    }
    this.queryExpanders = buildExpanders(queriesAll);
    this.returnType = methodMetadata.returnType();
    this.decoder = decoder;
    this.buildTemplateFromArgs = buildTemplateFromArgs;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object invoke(final Object[] argv) {
    RequestTemplate requestTemplate = buildTemplateFromArgs.create(argv);
    return executeRequest(requestTemplate);
  }

  private Object executeRequest(RequestTemplate requestTemplate) {
    Mono<Request> requestMono = targetRequest(requestTemplate);
    Type actualReturnType = ofActualReturnType(returnType);
    Type publisherType = ofPublisherType(returnType);
    Mono<Response> response = reactiveClient.executeRequest(requestMono);
    if (publisherType == Flux.class) {
      return  response.map(responseBody -> {
        List decodeResult = (List) decode(responseBody, actualReturnType);
        return decodeResult==null?Collections.EMPTY_LIST:decodeResult;
      }).flatMapMany(Flux::fromIterable).defaultIfEmpty(Collections.EMPTY_LIST);
    } else {
      Mono<Object> result = (response).flatMap(responseBody -> {
        Object decodeResult = decode(responseBody, actualReturnType);
        return decodeResult==null?Mono.empty():just(decodeResult);
      });
     return publisherType == null?result.block():result;
    }
  }

  private Object decode(Response response, Type returnType){
    try {
      return decoder.decode(response, returnType);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  Mono<Request> targetRequest(RequestTemplate template) {
    return Mono.just(target.apply(new RequestTemplate(template)));
  }

  private String queryLine(Map<String, Collection<String>> queries) {
    if (queries.isEmpty()) {
      return "";
    }

    StringBuilder queryBuilder = new StringBuilder();
    for (Map.Entry<String, Collection<String>> query : queries.entrySet()) {
      String field = query.getKey();
      for (String value : query.getValue()) {
        queryBuilder.append('&');
        queryBuilder.append(field);
        if (value != null) {
          queryBuilder.append('=');
          if (!value.isEmpty()) {
            queryBuilder.append(value);
          }
        }
      }
    }
    queryBuilder.deleteCharAt(0);
    return queryBuilder.insert(0, '?').toString();
  }

  protected Map<String, Collection<String>> queries(Object[] argv,
                                                    Map<String, ?> substitutionsMap) {
    Map<String, Collection<String>> queries = new LinkedHashMap<>();

    // queries from template
    queriesAll.keySet()
        .forEach(queryName -> addAll(queries, queryName,
            queryExpanders.get(queryName).stream()
                .map(expander -> expander.apply(substitutionsMap))
                .collect(toList())));

    // queries from args
    if (methodMetadata.queryMapIndex() != null) {
      ((Map<String, ?>) argv[methodMetadata.queryMapIndex()])
          .forEach((key, value) -> {
            if (value instanceof Iterable) {
              ((Iterable<?>) value).forEach(element -> add(queries, key, element.toString()));
            } else {
              add(queries, key, value.toString());
            }
          });
    }

    return queries;
  }

  protected Map<String, List<String>> headers(Object[] argv, Map<String, ?> substitutionsMap) {

    Map<String, List<String>> headers = new LinkedHashMap<>();

    // headers from template
    methodMetadata.template().headers().keySet()
        .forEach(headerName -> addAllOrdered(headers, headerName,
            headerExpanders.get(headerName).stream()
                .map(expander -> expander.apply(substitutionsMap))
                .collect(toList())));

    // headers from args
    if (methodMetadata.headerMapIndex() != null) {
      ((Map<String, ?>) argv[methodMetadata.headerMapIndex()])
          .forEach((key, value) -> {
            if (value instanceof Iterable) {
              ((Iterable<?>) value)
                  .forEach(element -> addOrdered(headers, key, element.toString()));
            } else {
              addOrdered(headers, key, value.toString());
            }
          });
    }

    return headers;
  }

  protected Publisher<Object> body(Object[] argv) {
    if (methodMetadata.bodyIndex() != null) {
      Object body = argv[methodMetadata.bodyIndex()];
      if (body instanceof Publisher) {
        return (Publisher<Object>) body;
      } else {
        return just(body);
      }
    } else {
      return Mono.empty();
    }
  }

  private static Map<String, List<Function<Map<String, ?>, String>>> buildExpanders(
      Map<String, Collection<String>> templates) {
    Stream<Pair<String, String>> headersFlattened = templates.entrySet().stream()
        .flatMap(e -> e.getValue().stream()
            .map(v -> new Pair<>(e.getKey(), v)));
    return headersFlattened.collect(groupingBy(
        entry -> entry.left,
        mapping(entry -> buildExpandFunction(entry.right), toList())));
  }

  /**
   * @param template
   * @return function that able to map substitutions map to actual value for specified template
   */
  private static final Pattern PATTERN = Pattern.compile("\\{([^}]+)\\}");

  private static Function<Map<String, ?>, String> buildExpandFunction(String template) {
    List<Function<Map<String, ?>, String>> chunks = new ArrayList<>();
    Matcher matcher = PATTERN.matcher(template);
    int previousMatchEnd = 0;
    while (matcher.find()) {
      String textChunk = template.substring(previousMatchEnd, matcher.start());
      if (textChunk.length() > 0) {
        chunks.add(data -> textChunk);
      }

      String substitute = matcher.group(1);
      chunks.add(data -> {
        Object substitution = data.get(substitute);
        if (substitution != null) {
          return substitution.toString();
        } else {
          return substitute;
        }
      });
      previousMatchEnd = matcher.end();
    }

    String textChunk = template.substring(previousMatchEnd, template.length());
    if (textChunk.length() > 0) {
      chunks.add(data -> textChunk);
    }

    return traceData -> chunks.stream().map(chunk -> chunk.apply(traceData))
        .collect(Collectors.joining());
  }

  public static class Factory implements ReactiveMethodHandlerFactory {
    private final ReactiveClientFactory reactiveClientFactory;
    private final Decoder decoder;

    public Factory(final ReactiveClientFactory reactiveClientFactory, Decoder decoder) {
      this.reactiveClientFactory = checkNotNull(reactiveClientFactory, "client must not be null");
      this.decoder = decoder;
    }

    @Override
    public ReactiveClientMethodHandler create(Target target,
                                              final MethodMetadata metadata,RequestTemplate.Factory buildTemplateFromArgs) {

      return new ReactiveClientMethodHandler(target, metadata,
          reactiveClientFactory.apply(metadata), decoder,buildTemplateFromArgs);
    }
  }

  private static Type ofActualReturnType(Type returnType) {
    Type publisherType = ofPublisherType(returnType);
    if (publisherType == Mono.class || publisherType == Flux.class) {
      Type actualType = ((ParameterizedType) returnType).getActualTypeArguments()[0];
      if (publisherType == Mono.class) {
        return actualType;
      } else {
        return new ParameterizedType() {
          @Override
          public Type[] getActualTypeArguments() {
            return new Type[]{actualType};
          }

          @Override
          public Type getRawType() {
            return List.class;
          }

          @Override
          public Type getOwnerType() {
            return null;
          }
        };
      }
    } else {
      return returnType;
    }
  }

  private static Type ofPublisherType(Type returnType) {
    if (returnType instanceof ParameterizedType) {
      Type rawType = ((ParameterizedType) returnType).getRawType();
      if (rawType == Mono.class || rawType == Flux.class) {
        return rawType;
      }
    }
    return null;
  }
}
