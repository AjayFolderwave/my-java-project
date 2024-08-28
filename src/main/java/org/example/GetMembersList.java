package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class GetMembersList {

//    This is for Testing - SQL-04
//    private static final String API_URL = "https://apipartner24.uat.commonapp.net/datacatalog/colleges";
//    private static final String API_KEY = "tKpciEuomX31SRdaZN0xg73rV17VgfZDkvw9wT70";

    private static final String API_URL = "https://apipartner24.commonapp.org/datacatalog/colleges";
    private static final String API_KEY = "KmzYqqm17d73pdemUGeSi2w0go34IqWb9RHfgEo0";
    private static final String JDBC_URL = "jdbc:sqlserver://10.0.4.7:1433;databaseName=MEFA_THRDPTY;integratedSecurity=true;encrypt=true;trustServerCertificate=true";
  //  private static final String JDBC_URL = "jdbc:sqlserver://localhost:1433;databaseName=MEFA_THRDPTY_TEST;integratedSecurity=true;encrypt=true;trustServerCertificate=true";

    private static final int PROGRAM_ID = 7; // Common App Program ID
    private static final int CONTRACT_YEAR = 2025; // Current contract year


    public static void main(String[] args) {
        String accessToken = GetAccessToken.getAccessToken();

        if (accessToken != null && !accessToken.isEmpty()) {
            System.out.println("Access Token: " + accessToken);
            fetchDataAndInsertIntoDB(accessToken);
        } else {
            System.out.println("Failed to get access token");
        }
    }

    private static void fetchDataAndInsertIntoDB(String accessToken) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(API_URL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("x-api-key", API_KEY.trim());
            connection.setRequestProperty("Authorization", accessToken.trim());

            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (InputStream is = connection.getInputStream();
                     BufferedReader br = new BufferedReader(new InputStreamReader(is, "utf-8"))) {

                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }

                    // Process the response
                    processMembersList(response.toString());
                }
            } else {
                try (InputStream errorStream = connection.getErrorStream();
                     BufferedReader br = new BufferedReader(new InputStreamReader(errorStream, "utf-8"))) {

                    StringBuilder errorResponse = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        errorResponse.append(responseLine.trim());
                    }
                    System.out.println("Error Response: " + errorResponse.toString());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static void processMembersList(String jsonResponse) {
        Set<Integer> activeInstitutions = new HashSet<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ArrayNode membersArray = (ArrayNode) objectMapper.readTree(jsonResponse);

            try (Connection connection = DriverManager.getConnection(JDBC_URL)) {
                String insertCommonAppMemberSQL = "INSERT INTO [MEFA_THRDPTY].[dbo].[CommonAppMembers] (MemberID, Name, Address1, Address2, City, State, Zip, Country, Phone, ContactEmail, Website, AcceptsTransfer, CAEssayReqdFY, CAEssayReqdTR, CommonAppOnly, CounselorEvalReqd, MaxTeacherEvalNum, TeacherEvalNum, InfoLink) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                String selectInstitutionSQL = "SELECT INSTITUTIONID FROM [MEFA_THRDPTY].[dbo].[dimInstitution] WHERE CAO_NK = ?";
                String insertMissingRecordSQL = "INSERT INTO [MEFA_THRDPTY].[dbo].[MissingRecords] (MemberID, Name, Address1, Address2, City, State, Zip, Country, Website) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                String selectContractSQL = "SELECT 1 FROM [MEFA_THRDPTY].[dbo].[CONTRACT] WHERE PROGRAMID = ? AND INSTITUTIONID = ? AND CONTRACTYEAR = ?";
                String insertContractSQL = "INSERT INTO [MEFA_THRDPTY].[dbo].[CONTRACT] (PROGRAMID, INSTITUTIONID, CONTRACTYEAR, STATUS, EFF_ST_DT, EFF_END_DT, CONTRACT_ST_DT, CONTRACT_END_DT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                String updateContractSQL = "UPDATE [MEFA_THRDPTY].[dbo].[CONTRACT] SET STATUS = ?, EFF_ST_DT = ?, EFF_END_DT = ?, CONTRACT_ST_DT = ?, CONTRACT_END_DT = ? WHERE PROGRAMID = ? AND INSTITUTIONID = ? AND CONTRACTYEAR = ?";

                PreparedStatement insertCommonAppMemberStatement = connection.prepareStatement(insertCommonAppMemberSQL);
                PreparedStatement selectInstitutionStatement = connection.prepareStatement(selectInstitutionSQL);
                PreparedStatement insertMissingRecordStatement = connection.prepareStatement(insertMissingRecordSQL);
                PreparedStatement selectContractStatement = connection.prepareStatement(selectContractSQL);
                PreparedStatement insertContractStatement = connection.prepareStatement(insertContractSQL);
                PreparedStatement updateContractStatement = connection.prepareStatement(updateContractSQL);

                java.sql.Date currentDate = new java.sql.Date(System.currentTimeMillis());
                java.sql.Date endDate = java.sql.Date.valueOf("2025-07-31");

                // Insert Common App members into CommonAppMembers table
                for (JsonNode memberNode : membersArray) {
                    try {
                        int memberId = getIntValue(memberNode, "memberId");
                        insertCommonAppMemberStatement.setInt(1, memberId);
                        insertCommonAppMemberStatement.setString(2, getValue(memberNode, "collegeName"));
                        insertCommonAppMemberStatement.setString(3, getValue(memberNode, "address1"));
                        insertCommonAppMemberStatement.setString(4, getValue(memberNode, "address2"));
                        insertCommonAppMemberStatement.setString(5, getValue(memberNode, "city"));
                        insertCommonAppMemberStatement.setString(6, getValue(memberNode, "state"));
                        insertCommonAppMemberStatement.setString(7, getValue(memberNode, "zip"));
                        insertCommonAppMemberStatement.setString(8, getValue(memberNode, "country"));
                        insertCommonAppMemberStatement.setString(9, getValue(memberNode, "phone"));
                        insertCommonAppMemberStatement.setString(10, getValue(memberNode, "contactEmail"));
                        insertCommonAppMemberStatement.setString(11, getValue(memberNode, "website"));
                        insertCommonAppMemberStatement.setBoolean(12, getBooleanValue(memberNode, "acceptsTransfer"));
                        insertCommonAppMemberStatement.setBoolean(13, getBooleanValue(memberNode, "caEssayReqdFY"));
                        insertCommonAppMemberStatement.setBoolean(14, getBooleanValue(memberNode, "caEssayReqdTR"));
                        insertCommonAppMemberStatement.setBoolean(15, getBooleanValue(memberNode, "commonAppOnly"));
                        insertCommonAppMemberStatement.setBoolean(16, getBooleanValue(memberNode, "counselorEvalReqd"));
                        insertCommonAppMemberStatement.setInt(17, getIntValue(memberNode, "maxTeacherEvalNum"));
                        insertCommonAppMemberStatement.setInt(18, getIntValue(memberNode, "teacherEvalNum"));
                        insertCommonAppMemberStatement.setString(19, getValue(memberNode, "infoLink"));

                        insertCommonAppMemberStatement.addBatch();
                    } catch (SQLException e) {
                        System.err.println("Error inserting member: " + e.getMessage());
                    }
                }
                insertCommonAppMemberStatement.executeBatch();

                // Process each member from the API
                for (JsonNode memberNode : membersArray) {
                    try {
                        int memberId = getIntValue(memberNode, "memberId");
                        String collegeName = getValue(memberNode, "collegeName");
                        activeInstitutions.add(memberId);

                        selectInstitutionStatement.setString(1, String.valueOf(memberId));
                        ResultSet institutionRS = selectInstitutionStatement.executeQuery();

                        if (institutionRS.next()) {
                            int institutionId = institutionRS.getInt("INSTITUTIONID");

                            selectContractStatement.setInt(1, PROGRAM_ID);
                            selectContractStatement.setInt(2, institutionId);
                            selectContractStatement.setInt(3, CONTRACT_YEAR);
                            ResultSet contractRS = selectContractStatement.executeQuery();

                            if (contractRS.next()) {
                                updateContractStatement.setString(1, "Active");
                                updateContractStatement.setDate(2, currentDate);
                                updateContractStatement.setDate(3, endDate);
                                updateContractStatement.setDate(4, currentDate);
                                updateContractStatement.setDate(5, endDate);
                                updateContractStatement.setInt(6, PROGRAM_ID);
                                updateContractStatement.setInt(7, institutionId);
                                updateContractStatement.setInt(8, CONTRACT_YEAR);
                                updateContractStatement.executeUpdate();
                            } else {
                                insertContractStatement.setInt(1, PROGRAM_ID);
                                insertContractStatement.setInt(2, institutionId);
                                insertContractStatement.setInt(3, CONTRACT_YEAR);
                                insertContractStatement.setString(4, "Active");
                                insertContractStatement.setDate(5, currentDate);
                                insertContractStatement.setDate(6, endDate);
                                insertContractStatement.setDate(7, currentDate);
                                insertContractStatement.setDate(8, endDate);
                                insertContractStatement.addBatch();
                            }
                        } else {
                            insertMissingRecordStatement.setInt(1, memberId);
                            insertMissingRecordStatement.setString(2, collegeName);
                            insertMissingRecordStatement.setString(3, getValue(memberNode, "address1"));
                            insertMissingRecordStatement.setString(4, getValue(memberNode, "address2"));
                            insertMissingRecordStatement.setString(5, getValue(memberNode, "city"));
                            insertMissingRecordStatement.setString(6, getValue(memberNode, "state"));
                            insertMissingRecordStatement.setString(7, getValue(memberNode, "zip"));
                            insertMissingRecordStatement.setString(8, getValue(memberNode, "country"));
                            insertMissingRecordStatement.setString(9, getValue(memberNode, "website"));
                            insertMissingRecordStatement.addBatch();
                        }
                    } catch (SQLException e) {
                        System.err.println("Error processing member: " + e.getMessage());
                    }
                }

                insertMissingRecordStatement.executeBatch();
                insertContractStatement.executeBatch();
                System.out.println("Contracts processed successfully.");
                System.out.println("Mismatched records processed successfully.");

                insertCommonAppMemberStatement.close();
                selectInstitutionStatement.close();
                insertMissingRecordStatement.close();
                selectContractStatement.close();
                insertContractStatement.close();
                updateContractStatement.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getValue(JsonNode node, String fieldName) {
        return node.has(fieldName) && !node.get(fieldName).isNull() ? node.get(fieldName).asText() : null;
    }

    private static boolean getBooleanValue(JsonNode node, String fieldName) {
        return node.has(fieldName) && !node.get(fieldName).isNull() && node.get(fieldName).asBoolean();
    }

    private static int getIntValue(JsonNode node, String fieldName) {
        return node.has(fieldName) && !node.get(fieldName).isNull() ? node.get(fieldName).asInt() : 0;
    }
}

