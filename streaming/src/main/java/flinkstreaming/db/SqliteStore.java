package flinkstreaming.db;

import flinkstreaming.Config;
import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;
import flinkstreaming.model.TransactionMessage;

import java.sql.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
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
                createTransactionTable();
                createCustomerValueQueryTable();
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
                return Optional.of(loadCustomer(rs));
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
                    "  customerId integer NOT NULL, " +
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
            "SELECT accountId,customerId,message,lastUpdated FROM accounts WHERE accountId=? "
    );
    public Optional<AccountMessage> getAccount(int accountId) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(ACCOUNT_QUERY_STMT);
        stmt.setInt(1, accountId);
        try {
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return Optional.of(loadAccount(rs));
            }
            return Optional.empty();
        }
        finally {
            stmt.close();
        }
    }

    private static final String TRANSACTION_CREATE_STMT = String.format(
            "CREATE TABLE IF NOT EXISTS transactions ( " +
                    "  accountId integer NOT NULL, " +
                    "  amount integer NOT NULL, " +
                    "  lastUpdated text NOT NULL " +
                    "); "
    );
    private void createTransactionTable() throws SQLException {
        Statement stmt = this.conn.createStatement();
        stmt.execute(TRANSACTION_CREATE_STMT);
        stmt.close();
    }

    private static final String TRANSACTION_INSERT_STMT = String.format(
            "INSERT INTO transactions (accountId,amount,lastUpdated) VALUES (?,?,?);"
    );
    public void insertTransaction(TransactionMessage tm) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(TRANSACTION_INSERT_STMT);
        stmt.setInt(1, tm.accountId);
        stmt.setInt(2, tm.amount);
        stmt.setString(3, tm.eventTime.format(DateTimeFormatter.ISO_INSTANT));
        stmt.executeUpdate();
        stmt.close();
    }

    private static final String ACCOUNT_TRANSACTION_QUERY_STMT = String.format(
            "SELECT accountId,amount,lastUpdated FROM transactions WHERE accountId=? ORDER BY strftime('%%s', lastUpdated) desc "
    );
    public List<TransactionMessage> getAccountTransactions(int accountId) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(ACCOUNT_TRANSACTION_QUERY_STMT);
        stmt.setInt(1, accountId);
        try {
            ResultSet rs = stmt.executeQuery();
            ArrayList<TransactionMessage> list = new ArrayList<>();
            while (rs.next()) {
                list.add(loadTransaction(rs));
            }
            return list;
        }
        finally {
            stmt.close();
        }
    }

    private static final String CUSTOMER_ACCOUNT_QUERY_STMT = String.format(
            "SELECT accountId,customerId,message,lastUpdated FROM accounts WHERE customerId=? ORDER BY accountId "
    );
    public List<AccountMessage> getCustomerAccounts(int customerId) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(CUSTOMER_ACCOUNT_QUERY_STMT);
        stmt.setInt(1, customerId);
        try {
            ResultSet rs = stmt.executeQuery();
            ArrayList<AccountMessage> list = new ArrayList<>();
            while (rs.next()) {
                list.add(loadAccount(rs));
            }
            return list;
        }
        finally {
            stmt.close();
        }
    }

    // Load Model Helpers
    private CustomerMessage loadCustomer(ResultSet rs) throws SQLException {
        CustomerMessage cm = new CustomerMessage();
        cm.customerId = rs.getInt(1);
        cm.email = rs.getString(2);
        cm.notifyPreference = rs.getInt(3) == 1 ? true : false;
        cm.state = rs.getString(4);
        cm.eventTime = ZonedDateTime.parse(rs.getString(5));
        return cm;
    }
    private AccountMessage loadAccount(ResultSet rs) throws SQLException {
        AccountMessage am = new AccountMessage();
        am.accountId = rs.getInt(1);
        am.customerId = rs.getInt(2);
        am.message = rs.getString(3);
        am.eventTime = ZonedDateTime.parse(rs.getString(4));
        return am;
    }
    private TransactionMessage loadTransaction(ResultSet rs) throws SQLException {
        TransactionMessage tm = new TransactionMessage();
        tm.accountId = rs.getInt(1);
        tm.amount = rs.getInt(2);
        tm.eventTime = ZonedDateTime.parse(rs.getString(3));
        return tm;
    }

    private static final String CUSTOMER_VALUE_QUERY_CREATE_STMT = String.format(
            "CREATE TABLE IF NOT EXISTS customerValueQuery ( " +
                    "  customerId integer PRIMARY KEY, " +
                    "  value text NOT NULL, " +
                    "  updateSource text NOT NULL, " +
                    "  lastUpdated text NOT NULL " +
                    "); "
    );
    private void createCustomerValueQueryTable() throws SQLException {
        Statement stmt = this.conn.createStatement();
        stmt.execute(CUSTOMER_VALUE_QUERY_CREATE_STMT);
        stmt.close();
    }

    private static final String CUSTOMER_VALUE_QUERY_INSERT_STMT = String.format(
            "INSERT INTO customerValueQuery (customerId,value,updateSource,lastUpdated) VALUES (?,?,?,?) " +
                    "  ON CONFLICT(customerId) DO UPDATE SET " +
                    "    value=excluded.value," +
                    "    updateSource=excluded.updateSource," +
                    "    lastUpdated=excluded.lastUpdated; "
    );
    public void insertCustomerValueQueryTable(int customerId, String value, String updateSource, ZonedDateTime lastUpdated) throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement(CUSTOMER_VALUE_QUERY_INSERT_STMT);
        stmt.setInt(1, customerId);
        stmt.setString(2, value);
        stmt.setString(3, updateSource);
        stmt.setString(4, lastUpdated.format(DateTimeFormatter.ISO_INSTANT));
        stmt.executeUpdate();
        stmt.close();
    }


}
