using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions.SQLAdapters.MySql
{
   public  class MySqlDialect: IQueryBuilderSpecialization
    {
        private static readonly int SelectStatementLength = "SELECT".Length;

        public List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
        {
            var sqlParametersReloaded = new List<object>();
            var c = context.Database.GetDbConnection();
            foreach (var parameter in sqlParameters)
            {
                var sqlParameter = (IDbDataParameter)parameter;
                sqlParametersReloaded.Add(new MySqlParameter(sqlParameter.ParameterName, sqlParameter.Value));
            }
            return sqlParametersReloaded;
        }

        public string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression)
        {
            return "+";
        }

        public (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery)
        {
            return (string.Empty, string.Empty);
        }

        public ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias,
            string tableAliasSuffixAs)
        {
            var result = new ExtractedTableAlias();
            var match = Regex.Match(fullQuery, @"FROM ([^\s]+)( AS [^\s]+)");
            result.TableAlias = match.Groups[1].Value;
            result.TableAliasSuffixAs = match.Groups[2].Value;
            result.Sql = fullQuery.Substring(match.Index + match.Length);
            return result;
        }

        public string FormatDeleteSql(string fullQuery)
        {
            var match = Regex.Match(fullQuery, @"FROM ([^\s]+) AS ([^\s]+)");
            var TableAliasSuffixAs = match.Groups[2].Value;
            return fullQuery.Replace($"{TableAliasSuffixAs}.","").Replace($"AS {TableAliasSuffixAs}","");
             
        }
    }
}
