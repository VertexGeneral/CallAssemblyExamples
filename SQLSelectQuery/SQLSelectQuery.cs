using System;
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

            //Finds the Property ID for the Connection, SQL, and Return properties in GlobalAction
            string connID = Input.FirstOrDefault(x => x.Value == "Connection").Key;
            string sqlID = Input.FirstOrDefault(x => x.Value == "SQL").Key;
            string returnID = Input.FirstOrDefault(x => x.Value == "Return").Key;

            //connID is used as our SQL Connection String
            connection.ConnectionString = @Input[connID];  //conID represents the ID of the property that contains the connection string.

            //sqlID is used as our SQL Query.
            String sqlQuery = Input[sqlID]; //sqlID represents the ID of the property that contains the SQL Query.
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
                            //The result that is returned from the SQL query is added to our return Dictionary object as Return's value.
                            Output.Add(returnID, reader[0].ToString()); //returnID represents the ID of the property that contains the return value.
                        }
                        else
                            Output.Add(returnID, "No data");
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
