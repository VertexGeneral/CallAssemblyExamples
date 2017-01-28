using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Sql;
using System.Data.SqlClient;
using System.IO;
using System.Xml.Linq;

namespace SQLSelectQuery
{
    public class SQLSelectQuery
    {
        //The Assembly that is referenced from the "Call Assembly" Workflow Node must have a method called "RunCallAssembly."
        //The method must accept and return a Dictionary<string, string> Object.
        //The Dictionary Key is the Workflow Property ID and the Value is the Workflow Property Value.
        public Dictionary<string, string> RunCallAssembly(Dictionary<string, string> Input)
        {
            //Declare the Output variable that will be used to collect/return our processed data.
            Dictionary<string, string> Output = new Dictionary<string, string>();

            try
            {
                //Property names and their ids are stored in an xml file. the name is the element, the id has to match the dictionary key.
                String mappingfile = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetCallingAssembly().Location), "PropertyMapping.xml");

                if (File.Exists(mappingfile))
                {


                    //Now, for each Workflow Property Value that we have in our Input, perform some sort of processing with that data.
                    //In this example, we are taking a SQL Connection string and a SQL query, then running the SQL query to get a value.
                    SqlConnection connection = new SqlConnection();
                    SqlCommand command = new SqlCommand();

                    //Declare an IEnumerable object to hold the WorkflowID found in our Input dictionary (must be set in the workflow as a process field)
                    IEnumerable<String> WorkflowID;

                    /* Use Linq to find the value for WorkflowID from the Input Dictionary (note: the property in your workflow
                    must be set to "WorkflowID=<MyID>" without quotes) */

                    WorkflowID = (from iv in Input
                                 where iv.Value.Contains(@"WorkflowID=")
                                 select iv.Value.Replace("WorkflowID=","")).ToList();

                    
                    //Declare IEnumerables to hold the IDs for the Connection String, SQL Statement, and Return Value found in the PropertyMapping.xml file
                    IEnumerable<String> ConnectionID = Enumerable.Empty<string>();
                    IEnumerable<String> StatementID = Enumerable.Empty<string>();
                    IEnumerable<String> ReturnID = Enumerable.Empty<string>();
                    
                    //Query PropertyMapping.xml for the relevant Workflow Field IDs
                    if (WorkflowID != null)
                    {

                        ConnectionID = (from values in XDocument.Load(mappingfile)
                                                    .Descendants("PropertyMap")
                                        where values.Element("WorkflowID").Value.Equals(WorkflowID.FirstOrDefault())
                                        select (String)values.Element("ConnectionString")).ToList();
                        StatementID = (from values in XDocument.Load(mappingfile)
                                                    .Descendants("PropertyMap")
                                        where values.Element("WorkflowID").Value.Equals(WorkflowID.FirstOrDefault())
                                        select (String)values.Element("SqlStatement")).ToList();
                        ReturnID = (from values in XDocument.Load(mappingfile)
                                                    .Descendants("PropertyMap")
                                        where values.Element("WorkflowID").Value.Equals(WorkflowID.FirstOrDefault())
                                        select (String)values.Element("ReturnValue")).ToList();
                                                
                    }

                    //The dictionary's key value for the connectionstring
                    connection.ConnectionString = @Input[ConnectionID.FirstOrDefault()];

                    //The dictionary's key value for the SQL SELECT Query.
                    String sqlQuery = Input[StatementID.FirstOrDefault()];

                    //These values are populated by the Call Assembly workflow node, but only in GlobalAction
                    //If statements allow this call assembly to work with GlobalCapture in addition to GlobalAction (GC does not pass these keys in the Input dictionary)
                    if (Input.ContainsKey("ARCHIVEID"))
                    {
                        sqlQuery = sqlQuery.Replace("#ARCHIVEID#", Input["ARCHIVEID"]);
                    }
                    if (Input.ContainsKey("DOCUMENTID"))
                    {
                        sqlQuery = sqlQuery.Replace("#DOCID#", Input["DOCUMENTID"]);
                    }
                    if (Input.ContainsKey("DATABASEID"))
                    {
                        sqlQuery = sqlQuery.Replace("#DATABASEID#", Input["DATABASEID"]);
                    }

                    //Set the SQL command statement
                    command.CommandText = sqlQuery;

                    //Try to run the query on the given database connection
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
                                    //The result that is returned from the SQL query is added to our return Dictionary object as Property id and Value.
                                    Output.Add(ReturnID.FirstOrDefault(), reader[0].ToString());
                                }
                                else
                                {
                                    Output.Add(ReturnID.FirstOrDefault(), "No Data");
                                }
                            }
                        }
                        connection.Close();
                    }
                }
                else
                {
                    Output.Add("-1", "PropertyMapping.xml not found.");
                }

            }
            catch (Exception ex)
            {
                //Log some errors out
                String errorPath = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetCallingAssembly().Location), "SQLCallAssembly.log");

                //Log the input dictionary for troubleshooting
                File.AppendAllText(errorPath, DateTime.Now.ToString() + ": An error has occured, input data: ");
                foreach (var procField in Input)
                {
                    File.AppendAllText(errorPath, String.Format("\r\nName: {0}, Value: {1}", procField.Key, procField.Value));
                }

                File.AppendAllText(errorPath, "\r\n" + DateTime.Now.ToString() + ": Exeption text: " + ex.Message + "\r\nStack Trace: " + ex.StackTrace);
            }
            //Finally, return our Output Dictionary Object that will be used set the new Values of each Workflow Property.
            //It is only necessary to return the Property ID's and Values of the Properties that are updated.
            return Output;
        }

        private String giveMeSpace(String path)
        {
            return path.Replace("%20", " ");
        }
    }

}
