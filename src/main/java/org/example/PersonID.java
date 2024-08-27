package org.example;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class PersonID {

    private static Properties properties = new Properties();
    private static String jdbcUrl2;
    private static String sqlQuery;
    private static final String OUTPUT_FILE = "C:/Users/AjayNelavetla/Desktop/PersonID/query_results.csv";

    static {
        try (InputStream input = new FileInputStream("src/main/java/org/example/application.properties")) {
            properties.load(input);
            jdbcUrl2 = properties.getProperty("jdbc.url2");
            sqlQuery = properties.getProperty("personId.sql_query");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting SQL query process...");
        String queryResults = executeSQLQuery();
        writeResultsToFile(queryResults);
    }

    private static String executeSQLQuery() {
        StringBuilder results = new StringBuilder();
        int recordCount = 0;

        try (Connection connection = DriverManager.getConnection(jdbcUrl2);
             Statement statement = connection.createStatement()) {
            System.out.println("Executing SQL query...");
            boolean hasResults = statement.execute(sqlQuery);

            while (hasResults || statement.getUpdateCount() != -1) {
                if (hasResults) {
                    try (ResultSet resultSet = statement.getResultSet()) {
                        while (resultSet.next()) {
                            String firstName = resultSet.getString("Firstname");
                            String lastName = resultSet.getString("Lastname");
                            String registeredDate = resultSet.getString("RegisteredDate");
                            results.append(String.format("Record %d:%n", recordCount + 1));
                            results.append(String.format("**FirstName**: %s%n", firstName));
                            results.append(String.format("**LastName**: %s%n", lastName));
                            results.append(String.format("**RegisteredDate**: %s%n%n", registeredDate));
                            recordCount++;
                        }
                    }
                }
                hasResults = statement.getMoreResults();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String queryResults = results.toString();
        String recordCountMessage = "Number of records: " + recordCount;

        return queryResults + "\n" + recordCountMessage;
    }

    private static void writeResultsToFile(String results) {
        try (FileWriter writer = new FileWriter(OUTPUT_FILE)) {
            writer.write(results);
            System.out.println("Query results have been written to " + OUTPUT_FILE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
