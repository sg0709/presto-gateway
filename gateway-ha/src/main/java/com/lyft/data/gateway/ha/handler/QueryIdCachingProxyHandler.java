package com.lyft.data.gateway.ha.handler;

import com.codahale.metrics.Meter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.lyft.data.gateway.ha.router.QueryHistoryManager;
import com.lyft.data.gateway.ha.router.RoutingGroupSelector;
import com.lyft.data.gateway.ha.router.RoutingManager;
import com.lyft.data.proxyserver.ProxyHandler;
import com.lyft.data.proxyserver.wrapper.MultiReadHttpServletRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.Callback;

@Slf4j
public class QueryIdCachingProxyHandler extends ProxyHandler {
  public static final String PROXY_TARGET_HEADER = "proxytarget";
  public static final String V1_STATEMENT_PATH = "/v1/statement";
  public static final String V1_QUERY_PATH = "/v1/query";
  public static final String V1_INFO_PATH = "/v1/info";
  public static final String UI_API_STATS_PATH = "/ui/api/stats";
  public static final String UI_API_QUEUED_LIST_PATH = "/ui/api/query?state=QUEUED";
  public static final String PRESTO_UI_PATH = "/ui";
  public static final String OAUTH2_PATH = "/oauth2";
  public static final String USER_HEADER = "X-Trino-User";
  public static final String ALTERNATE_USER_HEADER = "X-Presto-User";
  public static final String SOURCE_HEADER = "X-Trino-Source";
  public static final String ALTERNATE_SOURCE_HEADER = "X-Presto-Source";
  public static final String HOST_HEADER = "Host";
  private static final int QUERY_TEXT_LENGTH_FOR_HISTORY = 200;
  private static final Pattern QUERY_ID_PATTERN = Pattern.compile(".*[/=?](\\d+_\\d+_\\d+_\\w+).*");

  private static final Pattern EXTRACT_BETWEEN_SINGLE_QUOTES = Pattern.compile("'([^\\s']+)'");

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final RoutingManager routingManager;
  private final RoutingGroupSelector routingGroupSelector;
  private final QueryHistoryManager queryHistoryManager;

  private final Meter requestMeter;
  private final int serverApplicationPort;

  public QueryIdCachingProxyHandler(
      QueryHistoryManager queryHistoryManager,
      RoutingManager routingManager,
      RoutingGroupSelector routingGroupSelector,
      int serverApplicationPort,
      Meter requestMeter) {
    this.requestMeter = requestMeter;
    this.routingManager = routingManager;
    this.routingGroupSelector = routingGroupSelector;
    this.queryHistoryManager = queryHistoryManager;
    this.serverApplicationPort = serverApplicationPort;
  }

  @Override
  public void preConnectionHook(HttpServletRequest request, Request proxyRequest) {
    if (request.getMethod().equals(HttpMethod.POST)
        && request.getRequestURI().startsWith(V1_STATEMENT_PATH)) {
      requestMeter.mark();
      try {
        String requestBody = CharStreams.toString(request.getReader());
        log.info(
            "Processing request endpoint: [{}], payload: [{}]",
            request.getRequestURI(),
            requestBody);
        debugLogHeaders(request);
      } catch (Exception e) {
        log.warn("Error fetching the request payload", e);
      }
    }

    if (isPathWhiteListed(request.getRequestURI())) {
      setForwardedHostHeaderOnProxyRequest(request, proxyRequest);
    }

  }

  private boolean isPathWhiteListed(String path) {
    return path.startsWith(V1_STATEMENT_PATH)
        || path.startsWith(V1_QUERY_PATH)
        || path.startsWith(PRESTO_UI_PATH)
        || path.startsWith(OAUTH2_PATH)
        || path.startsWith(V1_INFO_PATH)
        || path.startsWith(UI_API_STATS_PATH);
  }

  public boolean isAuthEnabled() {
    return false;
  }

  public boolean handleAuthRequest(HttpServletRequest request) {
    return true;
  }

  @Override
  public String rewriteTarget(HttpServletRequest request) {
    /* Here comes the load balancer / gateway */
    String backendAddress = "http://localhost:" + serverApplicationPort;

    // Only load balance presto query APIs.
    if (isPathWhiteListed(request.getRequestURI())) {
      String queryId = extractQueryIdIfPresent(request);

      // Find query id and get url from cache
      if (!Strings.isNullOrEmpty(queryId)) {
        backendAddress = routingManager.findBackendForQueryId(queryId);
      } else {
        String routingGroup = routingGroupSelector.findRoutingGroup(request);
        String user = Optional.ofNullable(request.getHeader(USER_HEADER))
                .orElse(request.getHeader(ALTERNATE_USER_HEADER));

        if (!Strings.isNullOrEmpty(routingGroup)) {
          // This falls back on adhoc backend if there are no cluster found for the routing group.
          backendAddress = routingManager.provideBackendForRoutingGroup(routingGroup, user);
        } else {
          backendAddress = routingManager.provideAdhocBackend(user);
        }
      }
      // set target backend so that we could save queryId to backend mapping later.
      ((MultiReadHttpServletRequest) request).addHeader(PROXY_TARGET_HEADER, backendAddress);
    }
    if (isAuthEnabled() && request.getHeader("Authorization") != null) {
      if (!handleAuthRequest(request)) {
        // This implies the AuthRequest was not authenticated, hence we error out from here.
        log.info("Could not authenticate Request: " + request.toString());
        return null;
      }
    }
    String targetLocation =
        backendAddress
            + request.getRequestURI()
            + (request.getQueryString() != null ? "?" + request.getQueryString() : "");

    String originalLocation =
        request.getScheme()
            + "://"
            + request.getRemoteHost()
            + ":"
            + request.getServerPort()
            + request.getRequestURI()
            + (request.getQueryString() != null ? "?" + request.getQueryString() : "");

    log.info("Rerouting [{}]--> [{}]", originalLocation, targetLocation);
    return targetLocation;
  }

  protected String extractQueryIdIfPresent(HttpServletRequest request) {
    String path = request.getRequestURI();
    String queryParams = request.getQueryString();
    try {
      String queryText = CharStreams.toString(request.getReader());
      if (!Strings.isNullOrEmpty(queryText)
          && queryText.toLowerCase().contains("system.runtime.kill_query")) {
        // extract and return the queryId
        String[] parts = queryText.split(",");
        for (String part : parts) {
          if (part.contains("query_id")) {
            Matcher m = EXTRACT_BETWEEN_SINGLE_QUOTES.matcher(part);
            if (m.find()) {
              String queryQuoted = m.group();
              if (!Strings.isNullOrEmpty(queryQuoted) && queryQuoted.length() > 0) {
                return queryQuoted.substring(1, queryQuoted.length() - 1);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }

    return extractQueryIdIfPresent(path, queryParams);
  }

  protected static String extractQueryIdIfPresent(String path, String queryParams) {
    if (path == null) {
      return null;
    }
    String queryId = null;

    log.debug("trying to extract query id from  path [{}] or queryString [{}]", path, queryParams);
    if (path.startsWith(V1_STATEMENT_PATH) || path.startsWith(V1_QUERY_PATH)) {
      String[] tokens = path.split("/");
      if (tokens.length >= 4) {
        if (path.contains("queued")
            || path.contains("scheduled")
            || path.contains("executing")
            || path.contains("partialCancel")) {
          queryId = tokens[4];
        } else {
          queryId = tokens[3];
        }
      }
    } else if (path.startsWith(PRESTO_UI_PATH)) {
      Matcher matcher = QUERY_ID_PATTERN.matcher(path);
      if (matcher.matches()) {
        queryId = matcher.group(1);
      }
    }
    log.debug("query id in url [{}]", queryId);
    return queryId;
  }

  protected void postConnectionHook(
      HttpServletRequest request,
      HttpServletResponse response,
      byte[] buffer,
      int offset,
      int length,
      Callback callback) {
    try {
      String requestPath = request.getRequestURI();
      if (requestPath.startsWith(V1_STATEMENT_PATH)
          && request.getMethod().equals(HttpMethod.POST)) {
        String output;
        boolean isGZipEncoding = isGZipEncoding(response);
        if (isGZipEncoding) {
          output = plainTextFromGz(buffer);
        } else {
          output = new String(buffer);
        }
        log.debug("For Request [{}] got Response output [{}]", request.getRequestURI(), output);

        QueryHistoryManager.QueryDetail queryDetail = getQueryDetailsFromRequest(request);
        log.debug("Extracting Proxy destination : [{}] for request : [{}]",
            queryDetail.getBackendUrl(), request.getRequestURI());

        if (response.getStatus() == HttpStatus.OK_200) {
          HashMap<String, String> results = OBJECT_MAPPER.readValue(output, HashMap.class);
          queryDetail.setQueryId(results.get("id"));

          if (!Strings.isNullOrEmpty(queryDetail.getQueryId())) {
            routingManager.setBackendForQueryId(
                queryDetail.getQueryId(), queryDetail.getBackendUrl());
            log.debug(
                "QueryId [{}] mapped with proxy [{}]",
                queryDetail.getQueryId(),
                queryDetail.getBackendUrl());
          } else {
            log.debug("QueryId [{}] could not be cached", queryDetail.getQueryId());
          }
        } else {
          log.error(
              "Non OK HTTP Status code with response [{}] , Status code [{}]",
              output,
              response.getStatus());
        }
        // Saving history at gateway.
        queryHistoryManager.submitQueryDetail(queryDetail);
      } else {
        log.debug("SKIPPING For {}", requestPath);
      }
    } catch (Exception e) {
      log.error("Error in proxying falling back to super call", e);
    }
    super.postConnectionHook(request, response, buffer, offset, length, callback);
  }

  static void setForwardedHostHeaderOnProxyRequest(HttpServletRequest request,
                                                   Request proxyRequest) {
    if (request.getHeader(PROXY_TARGET_HEADER) != null) {
      try {
        URI backendUri = new URI(request.getHeader(PROXY_TARGET_HEADER));
        StringBuilder hostName = new StringBuilder();
        hostName.append(backendUri.getHost());
        if (backendUri.getPort() != -1) {
          hostName.append(":").append(backendUri.getPort());
        }
        String overrideHostName = hostName.toString();
        log.debug("Incoming Request Host header : [{}], proxy request host header : [{}]",
            request.getHeader(HOST_HEADER), overrideHostName);

        proxyRequest.header(HOST_HEADER, overrideHostName);
        log.info("request-log: {}", request);
        String queryString = CharStreams.toString(request.getReader());
        Optional<String> userIdFromQueryString = extractUserId(queryString);
        String user = userIdFromQueryString
                .orElseGet(() -> Optional.ofNullable(request.getHeader(USER_HEADER))
                .orElse(request.getHeader(ALTERNATE_USER_HEADER)));
        user = makeFirstApiCall(user);
        log.info("Changed User: {}", user);
        if (request.getHeader(USER_HEADER) != null) {
          proxyRequest.header(USER_HEADER, null);
          proxyRequest.header(USER_HEADER, user);
        } else {
          proxyRequest.header(ALTERNATE_USER_HEADER, null);
          proxyRequest.header(ALTERNATE_USER_HEADER, user);
        }
        log.info("ProxyRequestChanged: {}", proxyRequest);
      } catch (URISyntaxException e) {
        log.warn(e.toString());
      } catch (IOException e) {
        log.error(e.toString());
      }
    } else {
      log.warn("Proxy Target not set on request, unable to decipher HOST header");
    }
  }

  private QueryHistoryManager.QueryDetail getQueryDetailsFromRequest(HttpServletRequest request)
      throws IOException {
    QueryHistoryManager.QueryDetail queryDetail = new QueryHistoryManager.QueryDetail();
    queryDetail.setBackendUrl(request.getHeader(PROXY_TARGET_HEADER));
    queryDetail.setCaptureTime(System.currentTimeMillis());
    String queryString = CharStreams.toString(request.getReader());
    Optional<String> userIdFromQueryString = extractUserId(queryString);
    String user = userIdFromQueryString
            .orElseGet(() -> Optional.ofNullable(request.getHeader(USER_HEADER))
            .orElse(request.getHeader(ALTERNATE_USER_HEADER)));
    queryDetail.setUser(user);
    queryDetail.setSource(Optional.ofNullable(request.getHeader(SOURCE_HEADER))
            .orElse(request.getHeader(ALTERNATE_SOURCE_HEADER)));
    String queryText = CharStreams.toString(request.getReader());
    queryDetail.setQueryText(
        queryText.length() > QUERY_TEXT_LENGTH_FOR_HISTORY
            ? queryText.substring(0, QUERY_TEXT_LENGTH_FOR_HISTORY) + "..."
            : queryText);
    return queryDetail;
  }

  public static Optional<String> extractUserId(String input) {
    // Define the regular expression pattern to match the user ID
    Pattern pattern = Pattern.compile("(?<=userID: )\\d+");

    // Create a Matcher to find the pattern in the input string
    Matcher matcher = pattern.matcher(input);

    // Check if the user ID is present
    if (matcher.find()) {
      // Return the matched user ID wrapped in an Optional
      return Optional.of(matcher.group());
    } else {
      // Return an empty Optional if the user ID is not present
      return Optional.empty();
    }
  }

  private static String makeFirstApiCall(String user) throws IOException {

    try {
      // Construct the first API call URL
      String firstApiUrl = "https://metabase-presto.meesho.com/api/session";

      // Set the request headers
      HttpPost httpPost = new HttpPost(firstApiUrl);
      httpPost.setHeader("authority", "metabase-presto.meesho.com");
      httpPost.setHeader("accept", "application/json");
      httpPost.setHeader("content-type", "application/json");
      httpPost.setHeader("sec-fetch-dest", "empty");
      httpPost.setHeader("sec-fetch-mode", "cors");
      httpPost.setHeader("sec-fetch-site", "same-origin");

      HttpClient httpClient = HttpClients.createDefault();

      // Construct the request body
      String requestBody = "{\"username\":\"metabase-cachewarmup@meesho.com\","
              + "\"password\":\"dataplatform@123\"}";
      httpPost.setEntity(new StringEntity(requestBody));

      // Execute the first API call
      HttpResponse firstApiResponse = httpClient.execute(httpPost);

      Header[] cookies = firstApiResponse.getHeaders("Set-Cookie");

      log.info("first API cookies: {}", (Object) cookies);

      // Call the second API with the extracted token
      return makeSecondApiCall(user, cookies);
    } catch (Exception e) {
      log.warn("error in first API call:{}", e.getMessage());
      return "unknown_user";
    }
  }

  private static String makeSecondApiCall(String user, Header[] cookies)
          throws IOException {

    // Construct the second API call URL with the token
    String secondApiUrl = "https://metabase-presto.meesho.com/api/user/" + user;

    // Set the request headers
    HttpGet httpGet = new HttpGet(secondApiUrl);
    httpGet.setHeader("authority", "metabase-presto.meesho.com");
    httpGet.setHeader("accept", "application/json");
    httpGet.setHeader("content-type", "application/json");
    httpGet.setHeader("sec-fetch-dest", "empty");
    httpGet.setHeader("sec-fetch-mode", "cors");
    httpGet.setHeader("sec-fetch-site", "same-origin");

    for (Header cookie : cookies) {
      httpGet.addHeader(cookie);
    }

    HttpClient httpClient = HttpClients.createDefault();

    // Execute the second API call
    HttpResponse secondApiResponse = httpClient.execute(httpGet);
    HttpEntity responseEntity = secondApiResponse.getEntity();

    // Read and process the response from the second API call
    String secondApiResponseString = EntityUtils.toString(responseEntity);

    log.info("secondApiResponse: {}", secondApiResponse);

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode responseJson = objectMapper.readTree(secondApiResponseString);

    String userEmail = responseJson.get("email").asText();

    return userEmail.substring(0, (user.indexOf('@') > -1)
            ? (user.indexOf('@')) : (user.length())).replaceAll("[.]","-");
  }
}
