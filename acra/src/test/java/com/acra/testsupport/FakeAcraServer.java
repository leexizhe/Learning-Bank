package com.acra.testsupport;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A real HTTP server on a real socket, standing in for ACRA. The bodies below were recorded from live sandbox calls
 * rather than invented - the entities envelope is the kind of detail a hand-written sample gets wrong.
 */
public final class FakeAcraServer implements AutoCloseable {

    public static final String SAMPLE_PROFILE = """
      {"entities":[{
        "uen":"16888888A",
        "entityName":"ABC ENTERPRISE",
        "registrationDate":"2016-08-18",
        "statusOfBusiness":"LIVE",
        "constitutionOfBusiness":"SOLE-PROPRIETOR",
        "principalPlaceOfBusiness":{"type":"Local","streetName":"ABC ROAD","floor":"01",
          "unit":"123","postalCode":"123456","houseNumber":"123","buildingName":"ABC BUILDING"},
        "primaryActivity":{"code":"47112",
          "description":"MINI-MARTS, CONVENIENCE STORES AND PROVISION SHOPS"},
        "partner":{"principalName":"LIM AH HUAT","id":"S8888888H",
          "nationalityPlaceOfOrigin":"SINGAPORE CITIZEN","position":"OWNER",
          "dateOfAppointment":"2016-08-08"}
      }]}
      """;

    /** What the sandbox returns for a UEN that does not exist: 200 OK with an empty array, not a 404. */
    public static final String NO_SUCH_ENTITY = "{\"entities\":[]}";

    private static final String TOKEN_PATH = "/authorizeServer/oauth/token";
    private static final String PROFILE_PATH = "/api/acra/entityQuery/businessProfile";

    private final HttpServer server;

    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final AtomicInteger profileRequests = new AtomicInteger();
    private final List<String> presentedTokens = new CopyOnWriteArrayList<>();

    public FakeAcraServer() {
        try {
            // Port 0: the OS picks a free one.
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException("could not start the fake ACRA server", e);
        }
        server.createContext(TOKEN_PATH, this::handleToken);
        server.createContext(PROFILE_PATH, this::handleProfile);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public void reset() {
        tokenRequests.set(0);
        profileRequests.set(0);
        presentedTokens.clear();
    }

    public int tokenRequests() {
        return tokenRequests.get();
    }

    public int profileRequests() {
        return profileRequests.get();
    }

    /** The {@code token} header of each profile call, in order. */
    public List<String> presentedTokens() {
        return List.copyOf(presentedTokens);
    }

    private void handleToken(HttpExchange exchange) throws IOException {
        tokenRequests.incrementAndGet();

        if (!"client_credentials".equals(query(exchange).get("grant_type"))) {
            respond(exchange, 400, "{\"error\":\"unsupported_grant_type\"}");
            return;
        }

        // A distinct token per call, so a test can tell a reused token from a fresh one.
        String token = "test-token-" + tokenRequests.get();
        respond(exchange, 200, """
        {"access_token": "%s", "token_type": "Bearer", "expires_in": 1799, "scope": "read"}
        """.formatted(token));
    }

    private void handleProfile(HttpExchange exchange) throws IOException {
        profileRequests.incrementAndGet();
        presentedTokens.add(String.valueOf(exchange.getRequestHeaders().getFirst("token")));

        String uen = query(exchange).get("uen");
        respond(exchange, 200, "16888888A".equals(uen) ? SAMPLE_PROFILE : NO_SUCH_ENTITY);
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private Map<String, String> query(HttpExchange exchange) {
        String raw = exchange.getRequestURI().getRawQuery();
        Map<String, String> values = new HashMap<>();
        if (raw == null || raw.isBlank()) {
            return values;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                values.put(pair.substring(0, eq), pair.substring(eq + 1));
            }
        }
        return values;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
