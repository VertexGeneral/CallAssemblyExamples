using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Sql;
using System.Data.SqlClient;
using System.IO;

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
                //property names and their ids are stored in an xml file. the name is the element, the id has to match the dictionary key.
                String mappingfile = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetCallingAssembly().Location), "PropertyMapping.xml");

                if (File.Exists(mappingfile))
                {
                    //we create an object to deserialize the xml to. 
                    PropertyMapping propertyMap = new PropertyMapping();
                    propertyMap = propertyMap.Deserialize(mappingfile);

                    //Now, for each Workflow Property Value that we have in our Input, perform some sort of processing with that data.
                    //In this example, we are taking a SQL Connection string and a SQL query, then running the SQL query to get a value.
                    SqlConnection connection = new SqlConnection();
                    SqlCommand command = new SqlCommand();

                    //Create a variable to hold our WorkflowID from the Input dictionary
                    String workflowID;
                    

                    /* Use Linq to find the value for WorkflowID from the Input Dictionary (note: the property in your workflow
                    must be set to "WorkflowID=MyID" without quotes). Here I skip over null values because empty workflow properties are passed as nulls and 
                    default the workflowID variable to an empty string in case no values are found in the dictionary. This avoids throwing a null reference 
                    exception in case someoene forgets to pass a workflowID in their workflow fields.*/
                    workflowID = Input.Values.Where(x => x != null && x.Contains("WorkflowID=")).DefaultIfEmpty("").FirstOrDefault().Replace("WorkflowID=","");
                    
                    //Declare variables to hold the IDs for the Connection String, SQL Statement, and Return Value found in the PropertyMapping.xml file
                    string connectionID = "";
                    string statementID = "";
                    string returnID = "";

                    //Query PropertyMapping.xml for the relevant Workflow's Field IDs
                    if (workflowID != "")
                    {
                        PropertyMap workflowProperties = propertyMap.Workflows.Where(x => x.WorkflowID.ToLower() == workflowID.ToLower()).FirstOrDefault();
                        
                        if (workflowProperties != null)
                        {
                            connectionID = workflowProperties.ConnectionString;
                            statementID = workflowProperties.SqlStatement;
                            returnID = workflowProperties.ReturnValue;
                        }
                        else
                        {
                            Output.Add("-1", "WorkflowID not found in PropertyMapping.XML. Verify that WorkflowID value in XML matches that which was entered into the workflow field (case sensitive).");
                            return Output;
                        }
                    }
                    else
                    {
                        Output.Add("-1", "WorkflowID not found in Workflow fields. Verify that you have a workflow field set to WorkflowID=MyID. Note that the value of the field is case sensitive and must match a WorkflowID in the PropertyMapping.XML file.");
                        return Output;
                    }

                    //the dictionary's key value for the connectionstring
                    connection.ConnectionString = @Input[connectionID];  //1 represents the ID of the property that contains the connection string.

                    //the dictionary's key value for the SQL SELECT Query.
                    String sqlQuery = Input[statementID];
                    //these values are populated by the Call Assembly workflow node

                    //Allows this call assembly to work with GlobalCapture in addition to GlobalAction (GC does not pass these keys in the dict object)
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

                    command.CommandText = sqlQuery;

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
                                    Output.Add(returnID, reader[0].ToString());
                                }
                                else
                                {
                                    Output.Add(returnID, "No Data");
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

    /// <summary>
    /// create an object that will match the xml file
    /// </summary>
        public class PropertyMapping
    {
        private PropertyMap[] propertyMapField;

        [System.Xml.Serialization.XmlElementAttribute("PropertyMap")]
        public PropertyMap[] Workflows
        {
            get
            {
                return this.propertyMapField;
            }
            set
            {
                this.propertyMapField = value;
            }
        }

        public void Serialize(String file, PropertyMapping propertyMap)
        {
            System.Xml.Serialization.XmlSerializer xs
               = new System.Xml.Serialization.XmlSerializer(propertyMap.GetType());
            StreamWriter writer = File.CreateText(file);
            xs.Serialize(writer, propertyMap);
            writer.Flush();
            writer.Close();
            writer.Dispose();
        }

        public PropertyMapping Deserialize(string file)
        {
            System.Xml.Serialization.XmlSerializer xs
               = new System.Xml.Serialization.XmlSerializer(
                  typeof(PropertyMapping));
            StreamReader reader = File.OpenText(file);
            PropertyMapping propertyMap = (PropertyMapping)xs.Deserialize(reader);
            reader.Close();
            reader.Dispose();
            return propertyMap;
        }
    }

    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType = true)]
    public class PropertyMap
    {

        private string workflowIDField;
        private string connectionStringField;
        private string sqlStatementField;
        private string returnValueField;

        public string WorkflowID
        {
            get
            {
                return this.workflowIDField;
            }
            set
            {
                this.workflowIDField = value;
            }
        }

        public string ConnectionString
        {
            get
            {
                return this.connectionStringField;
            }
            set
            {
                this.connectionStringField = value;
            }
        }

        public string SqlStatement
        {
            get
            {
                return this.sqlStatementField;
            }
            set
            {
                this.sqlStatementField = value;
            }
        }

        public string ReturnValue
        {
            get
            {
                return this.returnValueField;
            }
            set
            {
                this.returnValueField = value;
            }
        }
    }
}
