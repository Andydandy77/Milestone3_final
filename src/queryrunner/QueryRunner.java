/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queryrunner;

import java.io.Console;
import java.util.ArrayList;
import java.util.Scanner;
import queryrunner.CommandLineTable;

/**
 *
 * QueryRunner takes a list of Queries that are initialized in it's constructor
 * and provides functions that will call the various functions in the QueryJDBC class 
 * which will enable MYSQL queries to be executed. It also has functions to provide the
 * returned data from the Queries. Currently the eventHandlers in QueryFrame call these
 * functions in order to run the Queries.
 */
public class QueryRunner {


    public QueryRunner()
    {
        this.m_jdbcData = new QueryJDBC();
        m_updateAmount = 0;
        m_queryArray = new ArrayList<>();
        m_error="";


        // TODO - You will need to change the queries below to match your queries.

        // You will need to put your Project Application in the below variable

        this.m_projectTeamApplication="COVIDTRACKING";    // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        // Each row that is added to m_queryArray is a separate query. It does not work on Stored procedure calls.
        // The 'new' Java keyword is a way of initializing the data that will be added to QueryArray. Please do not change
        // Format for each row of m_queryArray is: (QueryText, ParamaterLabelArray[], LikeParameterArray[], IsItActionQuery, IsItParameterQuery)

        //    QueryText is a String that represents your query. It can be anything but Stored Procedure
        //    Parameter Label Array  (e.g. Put in null if there is no Parameters in your query, otherwise put in the Parameter Names)
        //    LikeParameter Array  is an array I regret having to add, but it is necessary to tell QueryRunner which parameter has a LIKE Clause. If you have no parameters, put in null. Otherwise put in false for parameters that don't use 'like' and true for ones that do.
        //    IsItActionQuery (e.g. Mark it true if it is, otherwise false)
        //    IsItParameterQuery (e.g.Mark it true if it is, otherwise false)

        m_queryArray.add(new QueryData("SELECT pi.lname, pi.fname, c.date, b.business_name, s.state_name FROM State_Dep_Health s JOIN Businesses b USING (state_dep_health_state_id) JOIN Checkins c USING (business_id)" +
                "JOIN People p USING (people_id) JOIN Personal_Information pi USING (people_id) WHERE p.result = 'positive' ORDER BY s.state_name", null, null, false, false));        // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        m_queryArray.add(new QueryData("SELECT COUNT(*) AS positive_visits, b.business_name, s.state_name FROM State_Dep_Health s JOIN Businesses b USING (state_dep_health_state_id) JOIN Checkins c USING" +
                " (business_id) JOIN People p USING (people_id) WHERE p.result = 'positive' GROUP BY b.business_id ORDER BY positive_visits DESC; ", null, null, false, false));

        m_queryArray.add(new QueryData("SELECT Supply.hospital_id AS Hospital_ID, hospital_name AS Hospital_Name, inventory AS Inventory, Item_Description.item_name AS Item_Name FROM Item_Description JOIN" +
                " Supply ON Item_Description.item_id = Supply.item_id JOIN Hospitals ON Hospitals.hospital_id = Supply.hospital_id; ", null, null, false, false));
        m_queryArray.add(new QueryData("SELECT COUNT(*) AS Total_Tests, s.state_name FROM Tests JOIN State_Dep_Health s USING (state_dep_health_state_id) GROUP BY state_dep_health_state_id;", null, null, false, false));

        m_queryArray.add(new QueryData("SELECT status, count(people_id) as number FROM Cases group by status; ", null, null, false, false));

        m_queryArray.add(new QueryData("SELECT result, count(people_id) as number FROM People group by result;  ", null, null, false, false));

        m_queryArray.add(new QueryData("SELECT Count_Suc, Count_All, Count_Suc / Count_All AS Suc_rate, " +
                "Treatments.treatment_method_id, treatment_name FROM (SELECT COUNT(treatment_method_id) AS Count_Suc, " +
                "treatment_method_id FROM Cases WHERE status = 'recovered' GROUP BY treatment_method_id) AS x JOIN " +
                "(SELECT COUNT(treatment_method_id) AS Count_All, treatment_method_id FROM Cases GROUP BY treatment_method_id) " +
                "AS y ON x.treatment_method_id = y.treatment_method_id JOIN Treatments ON y.treatment_method_id = " +
                "Treatments.treatment_method_id;\n", new String [] {}, new boolean [] {false}, false, true));

        m_queryArray.add(new QueryData("SELECT AVG(2020 - YEAR(dob)) as average_deceased_age FROM Personal_Information p JOIN Cases c USING (people_id) WHERE status = 'deceased';", null, null, false, false));

        m_queryArray.add(new QueryData("SELECT s.state_name, COUNT(*) as recovered FROM Cases c JOIN State_Dep_Health s USING (state_dep_health_state_id) WHERE c.status = 'recovered' GROUP BY state_dep_health_state_id" +
                " order by recovered DESC limit 1; ", null, null, false, false));

        m_queryArray.add(new QueryData("SELECT h.hospital_name, s.item_id, s.inventory, i.item_name, i.description FROM Supply s INNER JOIN Hospitals h ON h.hospital_id = s.hospital_id " +
                "INNER JOIN Item_Description i ON i.item_id = s.item_id WHERE s.inventory <= ? ", new String [] {"LOW INVENTORY"}, new boolean [] {false}, false, true));

        m_queryArray.add(new QueryData("INSERT INTO `Checkins` (`people_id`, `business_id`, `date`, `checkins_id`) values(?,?,?,?)",new String [] {"people_id", "business_id", "date", "checkins_id"}, new boolean [] {false, false, false}, true, true));

        m_queryArray.add(new QueryData("UPDATE `Cases` SET `status` = ? WHERE (`case_id` = ?)",new String [] {"status", "case_id"}, new boolean [] {false, false, false}, true, true));

        m_queryArray.add(new QueryData("SELECT status, lname, fname, people_id FROM Cases join Personal_Information using(people_id) where (people_id = ?)",new String [] {"people_id"}, new boolean [] {false}, true, true));


        // THIS NEEDS TO CHANGE FOR YOUR APPLICATION
//        m_queryArray.add(new QueryData("insert into contact (contact_id, contact_name, contact_salary) values (?,?,?)",new String [] {"CONTACT_ID", "CONTACT_NAME", "CONTACT_SALARY"}, new boolean [] {false, false, false}, true, true));// THIS NEEDS TO CHANGE FOR YOUR APPLICATION
//        m_queryArray.add(new QueryData("insert into contact (contact_id, contact_name, contact_salary) values (?,?,?)",new String [] {"CONTACT_ID", "CONTACT_NAME", "CONTACT_SALARY"}, new boolean [] {false, false, false}, true, true));// THIS NEEDS TO CHANGE FOR YOUR APPLICATION

    }


    public int GetTotalQueries()
    {
        return m_queryArray.size();
    }

    public int GetParameterAmtForQuery(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetParmAmount();
    }

    public String  GetParamText(int queryChoice, int parmnum )
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetParamText(parmnum);
    }

    public String GetQueryText(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetQueryString();
    }

    /**
     * Function will return how many rows were updated as a result
     * of the update query
     * @return Returns how many rows were updated
     */

    public int GetUpdateAmount()
    {
        return m_updateAmount;
    }

    /**
     * Function will return ALL of the Column Headers from the query
     * @return Returns array of column headers
     */
    public String [] GetQueryHeaders()
    {
        return m_jdbcData.GetHeaders();
    }

    /**
     * After the query has been run, all of the data has been captured into
     * a multi-dimensional string array which contains all the row's. For each
     * row it also has all the column data. It is in string format
     * @return multi-dimensional array of String data based on the resultset
     * from the query
     */
    public String[][] GetQueryData()
    {
        return m_jdbcData.GetData();
    }

    public String GetProjectTeamApplication()
    {
        return m_projectTeamApplication;
    }
    public boolean  isActionQuery (int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.IsQueryAction();
    }

    public boolean isParameterQuery(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.IsQueryParm();
    }


    public boolean ExecuteQuery(int queryChoice, String [] parms)
    {
        boolean bOK = true;
        QueryData e=m_queryArray.get(queryChoice);
        bOK = m_jdbcData.ExecuteQuery(e.GetQueryString(), parms, e.GetAllLikeParams());
        return bOK;
    }

    public boolean ExecuteUpdate(int queryChoice, String [] parms)
    {
        boolean bOK = true;
        QueryData e=m_queryArray.get(queryChoice);
        bOK = m_jdbcData.ExecuteUpdate(e.GetQueryString(), parms);
        m_updateAmount = m_jdbcData.GetUpdateCount();
        return bOK;
    }


    public boolean Connect(String szHost, String szUser, String szPass, String szDatabase)
    {

        boolean bConnect = m_jdbcData.ConnectToDatabase("cs100", "mm_cpsc502102team04", "mm_cpsc502102team04Pass-", "mm_cpsc502102team04");
        if (bConnect == false)
            m_error = m_jdbcData.GetError();
        return bConnect;
    }

    public boolean Disconnect()
    {
        // Disconnect the JDBCData Object
        boolean bConnect = m_jdbcData.CloseDatabase();
        if (bConnect == false)
            m_error = m_jdbcData.GetError();
        return true;
    }

    public String GetError()
    {
        return m_error;
    }

    public void ShowQueries() {
        System.out.println("Connected to Database!");
        System.out.println("Queries displayed below: \n");


        for (int i = 0; i < this.GetTotalQueries(); i++) {
            System.out.println(i + 1 + ". " + this.GetQueryText(i));
        }
    }

    public void RunUserInputQuery() {
        String [] parmstring={};
        String [] headers;
        String [][] allData;
        boolean Ok=true;

        Scanner userIn = new Scanner(System.in);
        System.out.print("Query to run: ");
        int queryNum = Integer.parseInt(userIn.nextLine());

        if (this.isParameterQuery(queryNum)) {


        } else if (this.isActionQuery(queryNum)) {


        } else {
            Ok = this.ExecuteQuery(queryNum, parmstring);
        }
        if (Ok) {
            headers = this.GetQueryHeaders();
            allData = this.GetQueryData();

            CommandLineTable results = new CommandLineTable();
            results.setShowVerticalLines(true);
            results.setHeaders(headers);
            int numResults = allData.length;
            for (int j = 0; j < numResults; j++) {
                results.addRow(allData[j]);
            }
            results.print();
        }
    }



    private QueryJDBC m_jdbcData;
    private String m_error;
    private String m_projectTeamApplication;
    private ArrayList<QueryData> m_queryArray;
    private int m_updateAmount;

    /**
     * @param args the command line arguments
     */



    public static void main(String[] args) {
        // TODO code application logic here
        Console con = System.console();
        String hostname;
        final QueryRunner queryrunner = new QueryRunner();
        boolean console = false;
        if (args.length == 0) {
            java.awt.EventQueue.invokeLater(new Runnable() {
                public void run() {

                    new QueryFrame(queryrunner).setVisible(true);
                }
            });
        } else {
            if (args[0].equals("-console")) {

                //System.out.println("Nothing has been implemented yet. Please implement the necessary code");
                System.out.println("Connect to database using console");
                boolean connect = queryrunner.Connect("", "", "", "");
                if (!connect) {
                    System.out.println("Connection failed!");

                } else {

                    queryrunner.ShowQueries();
                    String[] parmstring = {};
                    String[] headers;
                    String[][] allData;
                    boolean Ok = true;

                    queryrunner.RunUserInputQuery();

                    Scanner userIn = new Scanner(System.in);

                    System.out.print("Type 'exit' to quit, anything else to continue: ");
                    while (!userIn.nextLine().equals("exit")){
                        queryrunner.RunUserInputQuery();
                        System.out.print("Type 'exit' to quit, anything else to continue: ");

                    }
                    System.out.println("Thanks for using the COVID Tracking system!");


                        // TODO
                        // You should code the following functionality:

                        //    You need to determine if it is a parameter query. If it is, then
                        //    you will need to ask the user to put in the values for the Parameters in your query
                        //    you will then call ExecuteQuery or ExecuteUpdate (depending on whether it is an action query or regular query)
                        //    if it is a regular query, you should then get the data by calling GetQueryData. You should then display this
                        //    output.
                        //    If it is an action query, you will tell how many row's were affected by it.
                        //
                        //    This is Psuedo Code for the task:
                        //    Connect()
                        //    n = GetTotalQueries()
                        //    for (i=0;i < n; i++)
                        //    {
                        //       Is it a query that Has Parameters
                        //       Then
                        //           amt = find out how many parameters it has
                        //           Create a paramter array of strings for that amount
                        //           for (j=0; j< amt; j++)
                        //              Get The Paramater Label for Query and print it to console. Ask the user to enter a value
                        //              Take the value you got and put it into your parameter array
                        //           If it is an Action Query then
                        //              call ExecuteUpdate to run the Query
                        //              call GetUpdateAmount to find out how many rows were affected, and print that value
                        //           else
                        //               call ExecuteQuery
                        //               call GetQueryData to get the results back
                        //               print out all the results
                        //           end if
                        //      }
                        //    Disconnect()


                        // NOTE - IF THERE ARE ANY ERRORS, please print the Error output
                        // NOTE - The QueryRunner functions call the various JDBC Functions that are in QueryJDBC. If you would rather code JDBC
                        // functions directly, you can choose to do that. It will be harder, but that is your option.
                        // NOTE - You can look at the QueryRunner API calls that are in QueryFrame.java for assistance. You should not have to
                        //    alter any code in QueryJDBC, QueryData, or QueryFrame to make this work.
//                System.out.println("Please write the non-gui functionality");

                    }
                }
            }
        }
    }
