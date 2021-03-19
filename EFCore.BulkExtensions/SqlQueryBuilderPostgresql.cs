using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EFCore.BulkExtensions
{
    public class SqlQueryBuilderPostgresql
    {
        public static string GetCopyString(TableInfo tableInfo, string tableName = null)
        {
            tableName = tableName ?? tableInfo.TableName;
            List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList(); 
            var commaSeparatedColumns = GetCommaSeparatedColumns(columnsList);
            

            var q = $"COPY \"{tableName}\" " +
                    $"({commaSeparatedColumns}) " +
                    $"FROM STDIN (FORMAT BINARY)";

            return q;
        }
        public static string GetCommaSeparatedColumns(List<string> columnsNames, string prefixTable = null, string equalsTable = null)
        {

            prefixTable += (prefixTable != null && prefixTable != ":") ? "." : "";
            equalsTable += (equalsTable != null && equalsTable != ":") ? "." : "";
            string commaSeparatedColumns = "";
            foreach (var columnName in columnsNames)
            {
                commaSeparatedColumns += prefixTable != "" ? $"{prefixTable}\"{columnName}\"" : $"\"{columnName}\"";
                commaSeparatedColumns += equalsTable != "" ? $" = {equalsTable}{columnName}" : "";
                commaSeparatedColumns += ", ";
            }
            if (commaSeparatedColumns != "")
            {
                commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 2, 2); // removes last excess comma and space: ", "
            }
            return commaSeparatedColumns;
        }
    }
}
