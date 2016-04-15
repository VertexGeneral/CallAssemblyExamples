SQL Select Query Demo
=================

The following is a sample application extension that can be used in a GlobalAction "Call Assembly" Workflow node.
This is a quick and useful way to pull data from a SQL database using a document's index data in a GlobalAction workflow.
Use this method if you want:
- the SQL query to be dynamically set within the GlobalAction Workflow
- the ability to return a single value

The assembly takes in 2 Workflow Property values, a SQL Connection String and a SQL SELECT query, and 
returns the SQL query's result as the third Property's value.

The assembly performs the following steps:
1) The Input Dictionary object is used to set Property 1 as the SQL Connection string; Property 2 is set as the SQL Query
2) The SQL query is executed.
3) If a result is returned from the query, it is added to the Output Dictionary object with ID 3 and returned.

Key Notes:
- The method is called "RunCallAssembly." Any assembly that is being referenced by the Call Assembly node must have a 
  method with this name. This is the method that will execute when the assembly is loaded.
- The method accepts and returns a Dictionary<string, string> object. The Input dictionary is the list of all 
  workflow Properties that have been passed to Call Assembly. The Output dictionary only needs to return the list 
  of properties that are being updated with their new values.