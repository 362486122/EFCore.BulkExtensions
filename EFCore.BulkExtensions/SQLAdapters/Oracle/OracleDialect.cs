using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions.SQLAdapters.Oracle
{
    class OracleDialect : IQueryBuilderSpecialization
    {
        private static readonly int SelectStatementLength = "SELECT".Length;

        public List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
        {
            // if SqlServer, might need to convert
            // Microsoft.Data.SqlClient to System.Data.SqlClient
            var sqlParametersReloaded = new List<object>();
            var c = context.Database.GetDbConnection();
            foreach (var parameter in sqlParameters)
            {
                var sqlParameter = (IDbDataParameter)parameter;
                //默认参数里面可能包含@
                sqlParametersReloaded.Add(new OracleParameter($":{sqlParameter.ParameterName.Replace("@","")}", sqlParameter.Value));
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
            var match = Regex.Match(fullQuery, @"FROM (""[^""]+"")( ""[^""]+"")");
            result.TableAlias = match.Groups[1].Value;
            result.TableAliasSuffixAs = match.Groups[2].Value;
            result.Sql = fullQuery.Substring(match.Index + match.Length);
            return result;
        }
    }
}
