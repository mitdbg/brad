import java.sql.*;

// This example is used to test JDBC connections to BRAD VDBEs (using the Arrow
// Flight SQL JDBC driver).
public class JdbcConnectionExample {
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"); // Example driver class
        } catch (ClassNotFoundException e) {
            System.err.println("Driver not found: " + e.getMessage());
            return; // Exit if driver not found
        }

        String url = "jdbc:arrow-flight-sql://<host>:<port>/?useEncryption=false"; // Example URL
        String user = "your_username";
        String password = "your_password";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to the database!");

            // Regular statement.
            try (Statement statement = connection.createStatement()) {
                String sql = "SELECT 1, 2, 3 FROM homes LIMIT 5";

                // 5. Execute the query
                try (ResultSet resultSet = statement.executeQuery(sql)) {

                    // 6. Process the results
                    while (resultSet.next()) {
                        // Access columns by name or index
                        int val1 = resultSet.getInt(1);
                        int val2 = resultSet.getInt(2);
                        int val3 = resultSet.getInt(3);
                        System.out.println("val1: " + val1 + ", val2: " + val2 + ", val3: " + val3);
                    }
                }
            }

            // Prepared statement.
            String sql = "SELECT id, name FROM your_table WHERE status = ?"; // Example query
            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                // 4. Set parameters for the PreparedStatement
                String status = "active"; // Example parameter value
                statement.setString(1, status); // Set the first parameter (index 1)

                // 5. Execute the query
                try (ResultSet resultSet = statement.executeQuery()) {

                    // 6. Process the results (if any)
                    while (resultSet.next()) {
                        int id = resultSet.getInt("id");
                        String name = resultSet.getString("name");
                        System.out.println("ID: " + id + ", Name: " + name);
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }

    }
}
