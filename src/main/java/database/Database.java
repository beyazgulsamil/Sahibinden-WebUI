package database;

import utils.SQLUtilities;
import utils.WebUtilities;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static utils.StringUtilities.Color.BLUE;

public class Database extends WebUtilities {
    SQLUtilities sql = new SQLUtilities();

    @SuppressWarnings("unused")
    public enum RecordAttribute {
        EMAIL("EmailAddress"),
        PHONE_NUMBER("PrimaryPhoneNumber");

        private final String key;

        RecordAttribute(String key){this.key = key;}

        public String getKey() {return key;}
    }

    public List<Map<String, Object>> composeQuery(RecordAttribute attribute, String value, String collectionName){
        String username     = properties.getProperty("sql-database-user");
        String password     = properties.getProperty("sql-database-password");
        String catalog      = properties.getProperty("sql-database-catalog");
        String url          = "jdbc:sqlserver://" + properties.getProperty("sql-database-url");
        String baseQuery    = properties.getProperty("sql-query").replace("{collection}", collectionName);
        String sqlQuery     = baseQuery + " " + attribute.getKey() + " = '" + value + "'";

        log.new Info("Query: " + sqlQuery);

        Connection connection = sql.getConnection(username, password, url, catalog);
        List<Map<String, Object>> results;
        long recordTimeout = Long.parseLong(properties.getProperty("record-timeout"));
        int checkInterval = (int) (recordTimeout/8000);
        long initialTime = System.currentTimeMillis();
        boolean resultsAcquired;
        boolean timeout = false;
        int duration = 0;
        int counter = 0;

        do {
            counter++;
            results = sql.getResults(connection, sqlQuery, false);
            resultsAcquired = results.size() > 0;

            if (resultsAcquired) break;
            try {
                TimeUnit.SECONDS.sleep(checkInterval);}
            catch (InterruptedException e) {throw new RuntimeException(e);}
            if (System.currentTimeMillis() - initialTime > recordTimeout) timeout = true;
            duration = (int) ((System.currentTimeMillis() - initialTime)/1000);
        }
        while (!timeout);

        if (counter > 1) log.new Info("Had to check " + strUtils.highlight(BLUE, counter) + " times.");

        if (resultsAcquired) log.new Info(
                "It took about " + strUtils.highlight(BLUE, duration) +
                        " seconds for the entry to be found in database " +
                        strUtils.highlight(BLUE, collectionName)
        );
        return results;
    }
}
