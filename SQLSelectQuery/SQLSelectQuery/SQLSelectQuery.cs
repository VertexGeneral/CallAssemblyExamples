﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Sql;
using System.Data.SqlClient;

namespace SQLSelectQuery
{
    public class SQLSelectQuery
    {
        //The Assembly that is referenced from the "Call Assembly" Workflow Node must have a method called "RunCallAssembly."
        //The method must accept and return a Dictionary<string, string> Object.
        //The Dictionary Key is the Workflow Property ID and the Value is the Workflow Property Value.
        public Dictionary<string, string> RunCallAssembly(Dictionary<string, string> Input)
        {
            //Now, for each Workflow Property Value that we have in our Input, perform some sort of processing with that data.
            //In this example, we are taking a SQL Connection string and a SQL query, then running the SQL query to get a value.
            SqlConnection connection = new SqlConnection();
            SqlCommand command = new SqlCommand();

            //Finds the Property ID for the Connection String, SQL Statement, and Return Value properties in GlobalAction
            string connKey = Input.FirstOrDefault(x => x.Value == "Connection String").Key;
            string sqlKey = Input.FirstOrDefault(x => x.Value == "SQL Statement").Key;
            string returnKey = Input.FirstOrDefault(x => x.Value == "Return Value").Key;

            //connKey is used as our SQL Connection String
            connection.ConnectionString = @Input[connKey];  //conKey represents the ID of the property that contains the connection string.

            //sqlKey is used as our SQL Statement.
            String sqlQuery = Input[sqlKey]; //sqlKey represents the ID of the property that contains the SQL Query.

            sqlQuery = sqlQuery.Replace("#ARCHIVEID#", Input["ARCHIVEID"]);
            sqlQuery = sqlQuery.Replace("#DOCID#", Input["DOCUMENTID"]);
            sqlQuery = sqlQuery.Replace("#DATABASEID#", Input["DATABASEID"]);

            command.CommandText = sqlQuery;

            //Declare the Output variable that will be used to collect/return our processed data.
            Dictionary<string, string> Output = new Dictionary<string, string>();

            if (connection.ConnectionString != String.Empty && command.CommandText != String.Empty)
            {
                connection.Open();
                command.Connection = connection;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        if (reader[0] != null)
                        {
                            //The result that is returned from the SQL query is added to our return Dictionary object as our Return Value.
                            Output.Add(returnKey, reader[0].ToString()); //returnKey represents the ID of the property that contains the return value.
                        }
                        else
                        {
                            //Return "No data" if the return is empty. This allows EXEC statements to be passed through the assembly to execute stored procedures.
                            Output.Add(returnKey, "No data");
                        }
                    }
                }
                connection.Close();
            }

            //Finally, return our Output Dictionary Object that will be used set the new Values of each Workflow Property.
            //It is only necessary to return the Property ID's and Values of the Properties that are updated.
            return Output;
        }
    }
}
