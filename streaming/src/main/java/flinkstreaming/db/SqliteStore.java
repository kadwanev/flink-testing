package flinkstreaming.db;

import flinkstreaming.Config;
import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;

import java.sql.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class SqliteStore {

    private static SqliteStore INSTANCE = null;

    public static SqliteStore getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SqliteStore();
        }
        return INSTANCE;
    }

    private Connection conn = null;

    private SqliteStore() {
        String dbUrl = "jdbc:sqlite:" + Config.DB_LOCATION + "/store.sqlite";
        try {
            DriverManager.registerDriver(new org.sqlite.JDBC());
            this.conn = DriverManager.getConnection(dbUrl);
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("Sqlite database has been connected.");
                createCustomerTable();
                createAccountTable();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error initializing sqlite", e);
        }
    }

    private static final String CUSTOMER_CREATE_STMT = String.format(
            "CREATE TABLE IF NOT EXISTS customers ( " +
                    "  customerId integer PRIMARY KEY, " +
                    "  email text NOT NULL, " +
                    "  notifyPreference integer NOT NULL, " +
                    "  state text NOT NULL, " +
                    "  lastUpdated text NOT NULL " +
                    "); "
    );
    private void createCustomerTable() throws SQLException {
        Statement stmt = this.conn.createStatement();
        stmt.execute(CUSTOMER_CREATE_STMT);
        stmt.close();
    }

    private static final String CUSTOMER_INSERT_STMT = String.format(
            "INSERT INTO customers (customerId,email,notifyPreference,state,lastUpdated) VALUES (?,?,?,?,?) " +
                    "  ON CONFLICT(customerId) DO UPDATE SET " +
                    "    email=excluded.email," +
                    "    notifyPreference=excluded.notifyPreference," +
                    "    state=excluded.state," +
                    "    lastUpdated=excluded.lastUpdated; "
    );
    public void insertCustomer(CustomerMessage am) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(CUSTOMER_INSERT_STMT);
        stmt.setInt(1, am.customerId);
        stmt.setString(2, am.email);
        stmt.setInt(3, am.notifyPreference == true ? 1 : 0);
        stmt.setString(4, am.state);
        stmt.setString(5, am.eventTime.format(DateTimeFormatter.ISO_INSTANT));
        stmt.executeUpdate();
        stmt.close();
    }

    private static final String CUSTOMER_QUERY_STMT = String.format(
            "SELECT customerId,email,notifyPreference,state,lastUpdated FROM customers WHERE customerId=? "
    );
    public Optional<CustomerMessage> getCustomer(int customerId) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(CUSTOMER_QUERY_STMT);
        stmt.setInt(1, customerId);
        try {
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                CustomerMessage cm = new CustomerMessage();
                cm.customerId = rs.getInt(1);
                cm.email = rs.getString(2);
                cm.notifyPreference = rs.getInt(3) == 1 ? true : false;
                cm.state = rs.getString(4);
                cm.eventTime = ZonedDateTime.parse(rs.getString(5));
                return Optional.of(cm);
            }
            return Optional.empty();
        }
        finally {
            stmt.close();
        }
    }

    private static final String ACCOUNT_CREATE_STMT = String.format(
            "CREATE TABLE IF NOT EXISTS accounts ( " +
                    "  accountId integer PRIMARY KEY, " +
                    "  customerId integer PRIMARY KEY, " +
                    "  message text NOT NULL, " +
                    "  lastUpdated text NOT NULL " +
                    "); "
    );
    private void createAccountTable() throws SQLException {
        Statement stmt = this.conn.createStatement();
        stmt.execute(ACCOUNT_CREATE_STMT);
        stmt.close();
    }

    private static final String ACCOUNT_INSERT_STMT = String.format(
            "INSERT INTO accounts (accountId,customerId,message,lastUpdated) VALUES (?,?,?,?) " +
            "  ON CONFLICT(accountId) DO UPDATE SET " +
            "    customerId=excluded.customerId," +
            "    message=excluded.message," +
            "    lastUpdated=excluded.lastUpdated; "
    );
    public void insertAccount(AccountMessage am) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(ACCOUNT_INSERT_STMT);
        stmt.setInt(1, am.accountId);
        stmt.setInt(2, am.customerId);
        stmt.setString(3, am.message);
        stmt.setString(4, am.eventTime.format(DateTimeFormatter.ISO_INSTANT));
        stmt.executeUpdate();
        stmt.close();
    }

    private static final String ACCOUNT_QUERY_STMT = String.format(
            "SELECT accountId,message,lastUpdated FROM accounts WHERE accountId=? "
    );
    public Optional<AccountMessage> getAccount(int accountId) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(ACCOUNT_QUERY_STMT);
        stmt.setInt(1, accountId);
        try {
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                AccountMessage am = new AccountMessage();
                am.accountId = rs.getInt(1);
                am.message = rs.getString(2);
                am.eventTime = ZonedDateTime.parse(rs.getString(3));
                return Optional.of(am);
            }
            return Optional.empty();
        }
        finally {
            stmt.close();
        }
    }

}
