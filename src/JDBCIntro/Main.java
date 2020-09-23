package JDBCIntro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {
    private static Connection connection;
    private static String query;
    private static final String link = "jdbc:mysql://localhost:3306/";
    private static final String database = "minions_db";
    private static PreparedStatement statement;
    private static BufferedReader reader;

    public static void main(String[] args) throws SQLException, IOException {

        reader = new BufferedReader(new InputStreamReader(System.in));

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "Rosi.5200");

        connection = DriverManager.getConnection(link + database, properties);

        // 2.	Get Villains’ Names
        // getVillainsNamesAndCountMinionsEx();

        // 3.	Get Minion Names
        // getMinionsNamesAndAgeEx();

        // 4.	Add Minion
        // addMinionEx();

        //5.	Change Town Names Casing
        // changeTownNamesToUppercase();

        //6.	*Remove Villain
        // removeVillainEx();

        //7.	Print All Minion Names
        // printAllMinionNamesEx();

        //8.	Increase Minions Age
        // increaseMinionsAgeEx();

        //9.	Increase Age Stored Procedure
        // increaseAgeStoredProcedureEx();


    }

    private static void removeVillainEx() throws IOException, SQLException {
        int villainId = Integer.parseInt(reader.readLine());
        if (!checkIfEntityExistsById(villainId, "villains")) {
            System.out.println("No such villain was found");
        } else {
            String villainName = getEntityNameById(villainId, "villains");
            int countOfMinions = countOfMinionsByVillain(villainId);
            deleteRowsByVillainId(villainId, "minions_villains", "villain_id");
            deleteRowsByVillainId(villainId, "villains", "id");
            System.out.printf("%s was deleted%n", villainName);
            System.out.printf("%d minions released", countOfMinions);
        }
    }

    private static void deleteRowsByVillainId(int villainId, String table, String columnName) throws SQLException {
        query = "DELETE FROM " + table + " WHERE " + columnName + " = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1, villainId);
        statement.executeUpdate();
    }

    private static int countOfMinionsByVillain(int villainId) throws SQLException {
        query = "select count(minion_id) as count\n" +
                "from minions_villains\n" +
                "where  villain_id = ?";

        statement = connection.prepareStatement(query);
        statement.setInt(1, villainId);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        return resultSet.getInt("count");
    }

    private static void increaseAgeStoredProcedureEx() throws IOException, SQLException {
        int minionId = Integer.parseInt(reader.readLine());

        query = "CALL usp_get_older(?)";

        CallableStatement callableStatement = connection.prepareCall(query);
        callableStatement.setInt(1, minionId);
        callableStatement.execute();
    }

    private static void increaseMinionsAgeEx() throws IOException, SQLException {
        int[] minionIds = Arrays.stream(reader.readLine().split("\\s+")).mapToInt(Integer::parseInt).toArray();
        for (int minionId : minionIds) {
            query = "UPDATE minions\n" +
                    "SET age = age+1,name = lower(name)\n" +
                    "WHERE id = ?";
            statement = connection.prepareStatement(query);
            statement.setInt(1, minionId);
            statement.executeUpdate();
        }
        printMinionsNamesAndAge();
    }

    private static void printMinionsNamesAndAge() throws SQLException {
        query = "select name, age from minions";
        statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            System.out.printf("%s %d%n", resultSet.getString("name"),
                    resultSet.getInt("age"));
        }
    }

    private static void printAllMinionNamesEx() throws SQLException {
        query = "select name from minions;";
        statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();
        List<String> minions = new ArrayList<>();
        while (resultSet.next()) {
            minions.add(resultSet.getString("name"));
        }
        int count = minions.size();
        for (int i = 0; i < count / 2; i++) {
            System.out.println(minions.get(i));
            System.out.println(minions.get(count - i - 1));
        }
        if (count % 2 == 1) {
            System.out.println(minions.get(count / 2));
        }
    }

    private static void changeTownNamesToUppercase() throws IOException, SQLException {
        String country = reader.readLine();
        query = "UPDATE towns\n" +
                "SET name = UPPER(name)\n" +
                "WHERE country =  ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, country);
        statement.executeUpdate();
        printTownNames(country);
    }

    private static void printTownNames(String country) throws SQLException {
        query = "select name from towns where country = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, country);
        ResultSet resultSet = statement.executeQuery();
        List<String> towns = new ArrayList<>();

        while (resultSet.next()) {
            towns.add(resultSet.getString("name"));
        }

        int countOfTownsChanged = towns.size();

        if (countOfTownsChanged == 0) {
            System.out.println("No town names were affected.");
        } else {
            System.out.printf("%d town names were affected.%n", countOfTownsChanged);
            System.out.println(String.join(", ", towns));
        }
    }

    private static void addMinionEx() throws IOException, SQLException {
        String[] minionParameters = reader.readLine().split("\\s+");
        String minionName = minionParameters[1];
        int minionAge = Integer.parseInt(minionParameters[2]);
        String minionTown = minionParameters[3];
        String villainName = reader.readLine().replace("Villain: ", "");

        if (!checkIfEntityExistsByName(minionTown, "towns")) {
            insertEntityInTown(minionTown);
        }
        if (!checkIfEntityExistsByName(villainName, "villains")) {
            insertEntityInVillains(villainName);
        }
        insertEntityInMinions(minionName, minionAge, minionTown, villainName);
    }

    private static void insertEntityInMinions(String minionName, int minionAge, String minionTown, String villainName) throws SQLException {
        query = "insert into minions (name, age, town_id)\n" +
                "values (?, ?, (select id from towns where towns.name = ?));";
        statement = connection.prepareStatement(query);
        statement.setString(1, minionName);
        statement.setInt(2, minionAge);
        statement.setString(3, minionTown);
        statement.executeUpdate();
        linkMinionWithVillain(minionName, villainName);
    }

    private static void linkMinionWithVillain(String minionName, String villainName) throws SQLException {
        query = "insert into minions_villains (minion_id, villain_id)\n" +
                "values ((select id from minions where name = ?),\n" +
                "        (select id from villains where name = ?));";

        statement = connection.prepareStatement(query);
        statement.setString(1, minionName);
        statement.setString(2, villainName);
        statement.executeUpdate();
        System.out.printf("Successfully added %s to be minion of %s.", minionName, villainName);
    }

    private static void insertEntityInVillains(String name) throws SQLException {
        query = "insert into villains (name, evilness_factor) values (?, 'evil');";
        statement = connection.prepareStatement(query);
        statement.setString(1, name);
        statement.executeUpdate();
        System.out.printf("Villain %s was added to the database.%n", name);
    }

    private static void insertEntityInTown(String name) throws SQLException {
        query = "insert into towns (name) values(?)";
        statement = connection.prepareStatement(query);
        statement.setString(1, name);
        statement.executeUpdate();
        System.out.printf("Town %s was added to the database.%n", name);
    }

    private static boolean checkIfEntityExistsByName(String name, String table) throws SQLException {
        query = "select * from " + table + " where name = ?;";
        statement = connection.prepareStatement(query);
        statement.setString(1, name);
        ResultSet resultSet = statement.executeQuery();
        return resultSet.next();
    }

    private static void getMinionsNamesAndAgeEx() throws IOException, SQLException {
        System.out.printf("Enter Villain ID:%n");

        int villain_id = Integer.parseInt(reader.readLine());

        if (!checkIfEntityExistsById(villain_id, "villains")) {
            System.out.printf("No villain with ID %d exists in the database.", villain_id);
        } else {
            System.out.printf("Villain: %s%n",
                    getEntityNameById(villain_id, "villains"));
            getMinionsNamesAndAgeByVillainId(villain_id);

        }
    }

    private static void getMinionsNamesAndAgeByVillainId(int villain_id) throws SQLException {
        query = "select m.name as minion_name,m.age as minion_age from minions as m\n" +
                "join minions_villains mv on m.id = mv.minion_id\n" +
                "where villain_id = ?;";

        statement = connection.prepareStatement(query);
        statement.setInt(1, villain_id);

        ResultSet resultSet = statement.executeQuery();

        int count = 1;
        while (resultSet.next()) {
            System.out.printf("%d. %s %d%n",
                    count,
                    resultSet.getString("minion_name"),
                    resultSet.getInt("minion_age"));
        }
    }

    private static String getEntityNameById(int entityId, String tableName) throws SQLException {
        query = "select name from " + tableName + " WHERE id = ?";

        statement = connection.prepareStatement(query);
        statement.setInt(1, entityId);

        ResultSet resultSet = statement.executeQuery();

        return resultSet.next() ? resultSet.getString("name") : null;
    }

    private static boolean checkIfEntityExistsById(int entityId, String tableName) throws SQLException {
        query = "select * from " + tableName + " where id = ?";

        statement = connection.prepareStatement(query);
        statement.setInt(1, entityId);

        ResultSet resultSet = statement.executeQuery();

        return resultSet.next();
    }

    private static void getVillainsNamesAndCountMinionsEx() throws SQLException {
        query = "select v.name as villian_name, count(mv.minion_id) as count\n" +
                "from villains as v join minions_villains as mv on v.id = mv.villain_id\n" +
                "group by v.name\n" +
                "having count > 15\n" +
                "order by count desc";

        statement = connection.prepareStatement(query);

        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            System.out.printf("%s %n", resultSet.getString(1));
        }
    }
}
