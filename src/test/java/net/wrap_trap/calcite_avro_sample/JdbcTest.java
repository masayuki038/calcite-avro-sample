package net.wrap_trap.calcite_avro_sample;

import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class JdbcTest {

  private final static String CONNECTION_URL = "jdbc:calcite:model=target/test-classes/model.json";

  @BeforeClass
  public static void setUpOnce() throws ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
  }

  @Test
  public void filterByEmpId() throws Exception {
    try (Connection conn = DriverManager.getConnection(CONNECTION_URL)) {
       try (PreparedStatement pstmt = conn.prepareStatement("select emp_id, name from test where emp_id=?")) {
         pstmt.setLong(1, 1L);
        try (ResultSet rs = pstmt.executeQuery()) {
          while (rs.next()) {
            assertThat(rs.getLong("emp_id"), is(1L));
            assertThat(rs.getString("name"), is("test1"));
          }
        }
      }
    }
  }

  @Test
  public void filterByName() throws Exception {
    try (Connection conn = DriverManager.getConnection(CONNECTION_URL)) {
      try (PreparedStatement pstmt = conn.prepareStatement("select emp_id, name from test where name=?")) {
        pstmt.setString(1, "test2");
        try (ResultSet rs = pstmt.executeQuery()) {
          while (rs.next()) {
            assertThat(rs.getLong("emp_id"), is(2L));
            assertThat(rs.getString("name"), is("test2"));
          }
        }
      }
    }
  }
}
