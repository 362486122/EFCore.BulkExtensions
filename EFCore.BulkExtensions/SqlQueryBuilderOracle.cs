﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace EFCore.BulkExtensions
{
    public static class SqlQueryBuilderOracle
    {
        public static string InsertIntoTable(TableInfo tableInfo, string tableName = null)
        {
            tableName = tableName ?? tableInfo.TableName;
            List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity);
            if (!keepIdentity && tableInfo.HasIdentity)
            {
                var identityColumnName = tableInfo.PropertyColumnNamesDict[tableInfo.IdentityColumnName];
                columnsList = columnsList.Where(a => a != identityColumnName).ToList();
            }

            var commaSeparatedColumns =GetCommaSeparatedColumns(columnsList);
            var commaSeparatedColumnsParams = GetCommaSeparatedColumns(columnsList, ":").Replace("\"","");

            var q = $"INSERT INTO \"{tableName}\" " +
                    $"({commaSeparatedColumns}) " +
                    $"VALUES ({commaSeparatedColumnsParams})"; 
             
            return q;
        }

        public static string GetCommaSeparatedColumns(List<string> columnsNames, string prefixTable = null)
        {

            prefixTable += (prefixTable != null && prefixTable != ":") ? "." : ""; 
            string commaSeparatedColumns = "";
            foreach (var columnName in columnsNames)
            {
                commaSeparatedColumns += prefixTable != "" ? $"{prefixTable}\"{columnName}\"" : $"\"{columnName}\"";
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