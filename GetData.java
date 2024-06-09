import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();
        
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            // Your implementation goes here....
            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW BidirectionalFriends AS " +
                "SELECT USER1_ID AS USER_ID1, USER2_ID AS USER_ID2 FROM " + friendsTableName + " " +
                "UNION " +
                "SELECT USER2_ID AS USER_ID1, USER1_ID AS USER_ID2 FROM " + friendsTableName
            );


            ResultSet rst = stmt.executeQuery(
                "SELECT U.MONTH_OF_BIRTH, C1.COUNTRY_NAME, C1.CITY_NAME, C1.STATE_NAME, C2.COUNTRY_NAME, " + 
                "C2.CITY_NAME, C2.STATE_NAME, U.GENDER, U.USER_ID, U.DAY_OF_BIRTH, U.LAST_NAME, U.FIRST_NAME, U.YEAR_OF_BIRTH" +
                "FROM " + userTableName + " U " +
                "JOIN " + currentCityTableName + " curC ON U.USER_ID = curC.USER_ID " +
                "JOIN " + hometownCityTableName + " homeC ON U.USER_ID = homeC.USER_ID " +
                "JOIN " + cityTableName + " C1 ON curC.CITY_ID = C1.CITY_ID " +
                "JOIN " + cityTableName + " C2 ON homeC.CITY_ID = C2.CITY_ID"
            );

            JSONArray userArray = new JSONArray();
            while (rst.next()) {
                JSONObject user = new JSONObject();

                user.put("MOB", rst.getString(1));

                user.put("hometown", new JSONObject());
                user.getJSONObject("hometown").put("country", rst.getString(2));
                user.getJSONObject("hometown").put("city", rst.getString(3));
                user.getJSONObject("hometown").put("state", rst.getString(4));

                user.put("current", new JSONObject());
                user.getJSONObject("current").put("country", rst.getString(5));
                user.getJSONObject("current").put("city", rst.getString(6));
                user.getJSONObject("current").put("state", rst.getString(7));

                user.put("gender", rst.getString(8));
                user.put("user_id", rst.getString(9));
                user.put("DOB", rst.getString(10));
                user.put("last_name", rst.getString(11));
                user.put("first_name", rst.getString(12));
                user.put("YOB", rst.getString(13));

                userArray.put(user);
            }

            ResultSet rstHomeNull = stmt.executeQuery(
                "SELECT U.MONTH_OF_BIRTH, C1.COUNTRY_NAME, C1.CITY_NAME, C1.STATE_NAME, " + 
                "U.GENDER, U.USER_ID, U.DAY_OF_BIRTH, U.LAST_NAME, U.FIRST_NAME, U.YEAR_OF_BIRTH " +
                "FROM " + userTableName + " U " +
                "JOIN " + currentCityTableName + " curC ON U.USER_ID = curC.USER_ID " +
                "JOIN " + hometownCityTableName + " homeC ON homeC.USER_ID IS NULL " +
                "JOIN " + cityTableName + " C1 ON curC.CITY_ID = C1.CITY_ID " 
            );

            while (rstHomeNull.next()) {
                JSONObject user = new JSONObject();

                user.put("MOB", rstHomeNull.getString(1));

                user.put("hometown", new JSONObject());

                user.put("current", new JSONObject());
                user.getJSONObject("current").put("country", rstHomeNull.getString(2));
                user.getJSONObject("current").put("city", rstHomeNull.getString(3));
                user.getJSONObject("current").put("state", rstHomeNull.getString(4));

                user.put("gender", rst.getString(5));
                user.put("user_id", rst.getString(6));
                user.put("DOB", rst.getString(7));
                user.put("last_name", rst.getString(8));
                user.put("first_name", rst.getString(9));
                user.put("YOB", rst.getString(10));

                userArray.put(user);
            }

            ResultSet rstCurNull = stmt.executeQuery(
                "SELECT U.MONTH_OF_BIRTH, C1.COUNTRY_NAME, C1.CITY_NAME, C1.STATE_NAME, " + 
                "U.GENDER, U.USER_ID, U.DAY_OF_BIRTH, U.LAST_NAME, U.FIRST_NAME, U.YEAR_OF_BIRTH " +
                "FROM " + userTableName + " U " +
                "JOIN " + currentCityTableName + " curC ON curC.USER_ID IS NULL " +
                "JOIN " + hometownCityTableName + " homeC ON U.USER_ID = homeC.USER_ID " +
                "JOIN " + cityTableName + " C1 ON curC.CITY_ID = C1.CITY_ID " 
            );

            while (rstCurNull.next()) {
                JSONObject user = new JSONObject();

                user.put("MOB", rstHomeNull.getString(1));

                user.put("hometown", new JSONObject());
                user.getJSONObject("hometown").put("country", rstHomeNull.getString(2));
                user.getJSONObject("hometown").put("city", rstHomeNull.getString(3));
                user.getJSONObject("hometown").put("state", rstHomeNull.getString(4));

                user.put("current", new JSONObject());

                user.put("gender", rst.getString(5));
                user.put("user_id", rst.getString(6));
                user.put("DOB", rst.getString(7));
                user.put("last_name", rst.getString(8));
                user.put("first_name", rst.getString(9));
                user.put("YOB", rst.getString(10));

                userArray.put(user);
            }

            ResultSet rstBothNull = stmt.executeQuery(
                "SELECT U.MONTH_OF_BIRTH, " + 
                "U.GENDER, U.USER_ID, U.DAY_OF_BIRTH, U.LAST_NAME, U.FIRST_NAME, U.YEAR_OF_BIRTH " +
                "FROM " + userTableName + " U " +
                "JOIN " + currentCityTableName + " curC ON curC.USER_ID IS NULL " +
                "JOIN " + hometownCityTableName + " homeC ON homeC.USER_ID IS NULL "
            );

            while (rstBothNull.next()) {
                JSONObject user = new JSONObject();

                user.put("MOB", rstHomeNull.getString(1));

                user.put("hometown", new JSONObject());

                user.put("current", new JSONObject());

                user.put("gender", rst.getString(2));
                user.put("user_id", rst.getString(3));
                user.put("DOB", rst.getString(4));
                user.put("last_name", rst.getString(5));
                user.put("first_name", rst.getString(6));
                user.put("YOB", rst.getString(7));

                userArray.put(user);
            }

            for (int i = 0; i < userArray.length(); i++) {
                JSONObject user = userArray.getJSONObject(i);
                
                JSONArray friends = new JSONArray();

                ResultSet rstFriends = stmt.executeQuery(
                    "FROM BidirectionalFriends " +
                    "SELECT USER_ID2 WHERE USER_ID1 = " + user.getString("user_id")
                );
                while (rstFriends.next()) {
                    friends.put(rstFriends.getInt(1));
                }
                user.put("friends", friends);
            }




            

            
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
