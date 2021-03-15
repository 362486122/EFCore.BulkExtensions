using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Text;

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
                sqlParametersReloaded.Add(SqlClientHelper.CorrectParameterType(c, sqlParameter));
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
            return new ExtractedTableAlias
            {
                TableAlias = tableAlias,
                TableAliasSuffixAs = tableAliasSuffixAs,
                Sql = fullQuery
            };
        }
    }
}
