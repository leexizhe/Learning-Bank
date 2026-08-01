package com.postgresbank.testsupport;

import com.postgresbank.common.Account;
import com.postgresbank.common.AccountRepository;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * Shared plumbing for the IT suite - a fresh account, or a raw read of one of Postgres's own stat
 * counters.
 */
public final class TestSupport {

  private TestSupport() {}

  /**
   * A fresh account per call, so containers reused across test classes never share starting state.
   */
  public static Account openAccount(AccountRepository accounts) {
    return accounts.save(new Account("owner-" + UUID.randomUUID()));
  }

  /**
   * Reads a single column off {@code pg_stat_user_tables} for one relation - e.g. {@code
   * n_dead_tup}, {@code n_tup_hot_upd}. Takes an existing {@link Connection} rather than opening a
   * fresh one: {@code n_dead_tup} is a live gauge that opportunistic HOT-page pruning can reduce
   * between statements, so the extra latency of acquiring a new pooled connection on every poll is
   * enough to race against it and read a stale (lower) count. {@code n_tup_hot_upd} has no such
   * race - it's a monotonic lifetime counter - but reusing the caller's connection costs nothing
   * either way, so one method covers both.
   */
  public static long readStatCounter(Connection c, String relname, String column) throws Exception {
    try (PreparedStatement ps =
        c.prepareStatement("select " + column + " from pg_stat_user_tables where relname = ?")) {
      ps.setString(1, relname);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  public static long readStatCounter(DataSource dataSource, String relname, String column)
      throws Exception {
    try (Connection c = dataSource.getConnection()) {
      return readStatCounter(c, relname, column);
    }
  }

  public static void updateAccountOwner(Connection c, long accountId, String newOwner)
      throws Exception {
    try (PreparedStatement ps = c.prepareStatement("update accounts set owner = ? where id = ?")) {
      ps.setString(1, newOwner);
      ps.setLong(2, accountId);
      ps.executeUpdate();
    }
  }
}
