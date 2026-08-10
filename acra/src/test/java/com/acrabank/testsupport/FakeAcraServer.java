package com.acrabank.testsupport;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ACRA, standing still.
 *
 * <p>This repository has a no-mocks rule, and the reason is that a mock lets you assert "was this method called"
 * instead of "did the right thing happen". A third-party government API is the one collaborator that genuinely cannot
 * be run locally the way Postgres and Kafka can, so the substitute is the next most honest thing: a real HTTP server on
 * a real socket, speaking real HTTP to an unmodified {@code RestClient}, serving bodies recorded from the sandbox.
 *
 * <p>What makes it useful rather than merely convenient is that it counts. Every assertion the token tests make is on
 * traffic this server actually received - how many times the token endpoint was hit across ten profile lookups, whether
 * the retry after a 401 carried a *different* token than the one that was rejected. Those are statements about the
 * mechanism, and no amount of stubbing a Java interface could make them.
 *
 * <p>Not covered here, deliberately: whether ACRA's real responses look like these. Only {@code SandboxSmokeIT} can
 * answer that, and it only runs when credentials are present.
 */
public final class FakeAcraServer implements AutoCloseable {

    public static final String TOKEN_PATH = "/authorizeServer/oauth/token";
    public static final String PROFILE_PATH = "/api/acra/entityQuery/businessProfile";

    private static final String APPLICATION_JSON = "application/json";

    /**
     * The genuine sandbox response for UEN 16888888A, copied verbatim from a live call rather than invented. That
     * matters: a hand-written sample tests the shape you imagined, which is exactly the shape the code was written
     * against, so it can only ever agree with itself. This one caught a real bug - the entities envelope, which no
     * guess had produced.
     */
    public static final String SAMPLE_PROFILE = """
      {"entities":[{
        "uen":"16888888A",
        "entityName":"ABC ENTERPRISE",
        "registrationDate":"2016-08-18",
        "commencementDate":"2016-08-18",
        "statusOfBusiness":"LIVE",
        "statusDate":"2016-08-08",
        "expiryDate":"2017-08-08",
        "constitutionOfBusiness":"SOLE-PROPRIETOR",
        "principalPlaceOfBusiness":{"type":"Local","streetName":"ABC ROAD","floor":"01",
          "unit":"123","postalCode":"123456","houseNumber":"123","buildingName":"ABC BUILDING"},
        "primaryActivity":{"code":"47112",
          "description":"MINI-MARTS, CONVENIENCE STORES AND PROVISION SHOPS"},
        "secondaryActivity":{"code":"46900",
          "description":"WHOLESALE TRADE OF A VARIETY OF GOODS WITHOUT A DOMINANT PRODUCT"},
        "authorisedRepresentative":{"principalName":"NG AH MEI","id":"S7788778H",
          "nationalityCitizenship":"SINGAPORE CITIZEN","dateOfAppointment":"2016-08-08",
          "address":{"type":"LOCAL","streetName":"ABC ROAD","floor":"01","unit":"02",
            "postalCode":"123456","houseNumber":"123","buildingName":"INTERNATIONAL PLAZA"}},
        "partner":{"principalName":"LIM AH HUAT","id":"S8888888H",
          "nationalityPlaceOfOrigin":"SINGAPORE CITIZEN","position":"OWNER",
          "dateOfAppointment":"2016-08-08",
          "address":{"type":"LOCAL","streetName":"ABC AVENUE","floor":"01","unit":"02",
            "postalCode":"123456","houseNumber":"123","buildingName":"DEF BUILDING"}}
      }]}
      """;

    /**
     * What the sandbox actually returns for a UEN that does not exist: {@code 200 OK} with an empty array, not a 404.
     * Recorded from a live call against UEN 99999999X.
     */
    public static final String NO_SUCH_ENTITY = "{\"entities\":[]}";

    private final HttpServer server;

    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final AtomicInteger profileRequests = new AtomicInteger();
    private final AtomicInteger tokensIssued = new AtomicInteger();

    private final List<String> tokenAuthHeaders = new CopyOnWriteArrayList<>();
    private final List<String> presentedTokens = new CopyOnWriteArrayList<>();
    private final List<String> requestedUens = new CopyOnWriteArrayList<>();

    private final Deque<Canned> profileScript = new ArrayDeque<>();

    private volatile Long expiresIn = 1799L;
    private volatile Canned defaultProfileResponse = new Canned(200, SAMPLE_PROFILE);

    public record Canned(int status, String body) {}

    public FakeAcraServer() {
        try {
            // Port 0: the OS picks a free one, so parallel modules never collide.
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException("could not start the fake ACRA server", e);
        }
        server.createContext(TOKEN_PATH, this::handleToken);
        server.createContext(PROFILE_PATH, this::handleProfile);
        // A real thread pool, not a single thread: TokenStampedeIT needs concurrent requests to actually be concurrent,
        // otherwise the server itself would be serialising them and the test would pass for the wrong reason.
        server.setExecutor(Executors.newFixedThreadPool(16));
        server.start();
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    // --- what the test tells the server to do -------------------------------------

    /** Set the {@code expires_in} the token endpoint advertises; {@code null} omits the field. */
    public void issueTokensValidFor(Long seconds) {
        this.expiresIn = seconds;
    }

    /** Queue one response for the profile endpoint, ahead of the default. */
    public void nextProfileResponse(int status, String body) {
        synchronized (profileScript) {
            profileScript.addLast(new Canned(status, body));
        }
    }

    public void defaultProfileResponse(int status, String body) {
        this.defaultProfileResponse = new Canned(status, body);
    }

    public void reset() {
        tokenRequests.set(0);
        profileRequests.set(0);
        tokensIssued.set(0);
        tokenAuthHeaders.clear();
        presentedTokens.clear();
        requestedUens.clear();
        synchronized (profileScript) {
            profileScript.clear();
        }
        expiresIn = 1799L;
        defaultProfileResponse = new Canned(200, SAMPLE_PROFILE);
    }

    // --- what the server observed -------------------------------------------------

    public int tokenRequests() {
        return tokenRequests.get();
    }

    public int profileRequests() {
        return profileRequests.get();
    }

    /** The {@code Authorization} header of each token call, so Basic auth can be verified. */
    public List<String> tokenAuthHeaders() {
        return Collections.unmodifiableList(tokenAuthHeaders);
    }

    /** The {@code token} header of each profile call, in order. */
    public List<String> presentedTokens() {
        return Collections.unmodifiableList(presentedTokens);
    }

    public List<String> requestedUens() {
        return Collections.unmodifiableList(requestedUens);
    }

    // --- handlers -----------------------------------------------------------------

    private void handleToken(HttpExchange exchange) throws IOException {
        tokenRequests.incrementAndGet();
        tokenAuthHeaders.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));

        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        if (!"client_credentials".equals(query.get("grant_type"))) {
            respond(exchange, 400, "{\"error\":\"unsupported_grant_type\"}");
            return;
        }

        // A distinct token per call, so a test can tell a reused token from a fresh one.
        String token = "test-token-" + tokensIssued.incrementAndGet();
        String expiry = expiresIn == null ? "" : "\"expires_in\": " + expiresIn + ",";
        respond(exchange, 200, """
        {"access_token": "%s", "token_type": "Bearer", %s "scope": "read"}
        """.formatted(token, expiry));
    }

    private void handleProfile(HttpExchange exchange) throws IOException {
        profileRequests.incrementAndGet();
        presentedTokens.add(String.valueOf(exchange.getRequestHeaders().getFirst("token")));
        requestedUens.add(String.valueOf(
                parseQuery(exchange.getRequestURI().getRawQuery()).get("uen")));

        Canned canned;
        synchronized (profileScript) {
            canned = profileScript.isEmpty() ? defaultProfileResponse : profileScript.removeFirst();
        }
        respond(exchange, canned.status(), canned.body());
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", APPLICATION_JSON);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private Map<String, String> parseQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return Map.of();
        }
        Map<String, String> values = new java.util.HashMap<>();
        for (String pair : rawQuery.split("&")) {
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
