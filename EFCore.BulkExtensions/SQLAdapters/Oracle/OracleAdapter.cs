using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SQLAdapters.Oracle
{
    public class OracleOperationsAdapter : ISqlOperationsAdapter
    {

        public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var connection = OpenAndGetSqlConnection(context, tableInfo.BulkConfig);
            bool doExplicitCommit = false;
            bool doPreInsert = entities.Count()>tableInfo.BulkConfig.BatchSize;
            IList<T> preInsertEntities = null;
            if (doPreInsert)
            {
                preInsertEntities = entities.Take(tableInfo.BulkConfig.BatchSize).ToList();
                entities = entities.Skip(tableInfo.BulkConfig.BatchSize).ToList();
            }
            try
            { 
                if (context.Database.CurrentTransaction == null)
                {  
                    doExplicitCommit = true;
                }
                var transaction = context.Database.CurrentTransaction == null ?
                    connection.BeginTransaction() :
                    context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig);

                var command = GetOracleCommand(context, type, entities, tableInfo, (OracleConnection)connection, (OracleTransaction)transaction);
                if(doPreInsert)
                {
                    LoadOracleValues(tableInfo, preInsertEntities, command);
                    command.ExecuteNonQuery();
                }
                LoadOracleValues(tableInfo, entities, command);
                command.ExecuteNonQuery();
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                context.Database.CloseConnection();
            }
        }

        
        public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            var connection =await OpenAndGetSqlConnectionAsync(context, tableInfo.BulkConfig,cancellationToken);
            bool doExplicitCommit = false;
            bool doPreInsert = entities.Count() > tableInfo.BulkConfig.BatchSize;
            IList<T> preInsertEntities = null;
            if (doPreInsert)
            {
                preInsertEntities = entities.Take(tableInfo.BulkConfig.BatchSize).ToList();
                entities = entities.Skip(tableInfo.BulkConfig.BatchSize).ToList();
            }
            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    doExplicitCommit = true;
                }
                var transaction = context.Database.CurrentTransaction == null ?
                    connection.BeginTransaction() :
                    context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig);

                var command = GetOracleCommand(context, type, entities, tableInfo, (OracleConnection)connection, (OracleTransaction)transaction);
                if (doPreInsert)
                {
                    LoadOracleValues(tableInfo, preInsertEntities, command);
                    await command.ExecuteNonQueryAsync();
                }
                LoadOracleValues(tableInfo, entities, command);
                await command.ExecuteNonQueryAsync();
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
               await context.Database.CloseConnectionAsync();
            }
        }

        public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            throw new NotImplementedException();
        }

        public Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            throw new NotImplementedException();
        }

        public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            throw new NotImplementedException();
        }

        public Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            throw new NotImplementedException();
        }

        public void Truncate(DbContext context, TableInfo tableInfo)
        {
            context.Database.ExecuteSqlRaw(SqlQueryBuilder.TruncateTable(tableInfo.FullTableName));
        }

        public async Task TruncateAsync(DbContext context, TableInfo tableInfo)
        {
            await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.TruncateTable(tableInfo.FullTableName));
        }

        #region Connection
        internal static DbConnection OpenAndGetSqlConnection(DbContext context, BulkConfig config)
        {
            context.Database.OpenConnection();

            return context.GetUnderlyingConnection(config);
        }

        internal static async Task<DbConnection> OpenAndGetSqlConnectionAsync(DbContext context, BulkConfig config, CancellationToken cancellationToken)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            return context.GetUnderlyingConnection(config);
        }

        private static Microsoft.Data.SqlClient.SqlBulkCopy GetSqlBulkCopy(Microsoft.Data.SqlClient.SqlConnection sqlConnection, IDbContextTransaction transaction, BulkConfig config)
        {
            var sqlBulkCopyOptions = config.SqlBulkCopyOptions;
            if (transaction == null)
            {
                return new Microsoft.Data.SqlClient.SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, null);
            }
            else
            {
                var sqlTransaction = (Microsoft.Data.SqlClient.SqlTransaction)transaction.GetUnderlyingTransaction(config);
                return new Microsoft.Data.SqlClient.SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, sqlTransaction);
            }
        }

        private static System.Data.SqlClient.SqlBulkCopy GetSqlBulkCopy(System.Data.SqlClient.SqlConnection sqlConnection, IDbContextTransaction transaction, BulkConfig config)
        {
            var sqlBulkCopyOptions = (System.Data.SqlClient.SqlBulkCopyOptions)config.SqlBulkCopyOptions;
            if (transaction == null)
            {
                return new System.Data.SqlClient.SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, null);
            }
            var sqlTransaction = (System.Data.SqlClient.SqlTransaction)transaction.GetUnderlyingTransaction(config);
            return new System.Data.SqlClient.SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, sqlTransaction);
        }
        #endregion

        #region OracleData
        internal static OracleCommand GetOracleCommand<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OracleConnection connection, OracleTransaction transaction)
        {
            
            OracleCommand command = connection.CreateCommand();
            command.Transaction = transaction;

            var operationType = tableInfo.BulkConfig.OperationType;

            switch (operationType)
            {
                case OperationType.Insert:
                    command.CommandText = SqlQueryBuilderOracle.InsertIntoTable(tableInfo);
                    break;
                case OperationType.InsertOrUpdate:
                    throw new NotSupportedException("Oracle not support insert or update");
                case OperationType.InsertOrUpdateDelete:
                    throw new NotSupportedException("Oracle not support insert or update delete");
                case OperationType.Update:
                    command.CommandText = SqlQueryBuilderOracle.UpdateSetTable(tableInfo);
                    break;
                case OperationType.Delete:
                    command.CommandText = SqlQueryBuilderOracle.DeleteFromTable(tableInfo);
                    break;
            }
            type = tableInfo.HasAbstractList ? entities[0].GetType() : type; 
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
            var properties = type.GetProperties(); 
            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    var propertyEntityType = entityPropertiesDict[property.Name];
                    string columnName = propertyEntityType.GetColumnName();
                    bool isNullable = true;
                    var propertyType = Nullable.GetUnderlyingType(property.PropertyType); 
                    if(propertyType==null)
                    {
                        propertyType= property.PropertyType;
                        isNullable = false;
                    } 
                    //TODO 不是值类型是否只有string?
                    object val=propertyType.IsValueType? Activator.CreateInstance(propertyType): string.Empty; 
                    var parameter = new OracleParameter($":{columnName}", val);
                    parameter.IsNullable = isNullable;
                    command.Parameters.Add(parameter);
                }
            }
            return command;
        }

        internal static void LoadOracleValues<T>(TableInfo tableInfo, IList<T> entities, OracleCommand command)
        {
            var propertyColumnsDict = tableInfo.PropertyColumnNamesDict;

            foreach (var propertyColumn in propertyColumnsDict)
            {
                List<object> values = new List<object>();
                if (!tableInfo.ShadowProperties.Contains(propertyColumn.Key))
                {
                    foreach (var entity in entities)
                    {
                        values.Add(tableInfo.FastPropertyDict[propertyColumn.Key].Get(entity));
                    }
                }
                else // IsShadowProperty
                {
                    foreach (var entity in entities)
                    {
                        values.Add(entity.GetType().Name);
                    }
                }
                //command.Parameters.Add($":{propertyColumn.Value}",values.ToArray());
                command.Parameters[$":{propertyColumn.Value}"].Value = values.ToArray();
            }
            command.ArrayBindCount = entities.Count();
            command.BindByName = true;
            
            command.CommandTimeout = tableInfo.BulkConfig.BulkCopyTimeout??300;
        }
        #endregion
    }
}
