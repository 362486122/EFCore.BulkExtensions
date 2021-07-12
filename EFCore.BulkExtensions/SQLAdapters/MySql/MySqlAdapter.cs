using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using MySql.Data.MySqlClient;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SQLAdapters.MySql
{
    public class MySqlAdapter : ISqlOperationsAdapter
    {

        public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var connection = OpenAndGetMySqlConnection(context, tableInfo.BulkConfig);
            bool doExplicitCommit = false;
            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (MySqlTransaction)(context.Database.CurrentTransaction == null ?
                                                      connection.BeginTransaction() :
                                                      context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));


                using (var stream = GetCsvStream(context, type, entities, tableInfo))
                {
                    var loader = InitLoader(connection, tableInfo, stream);
                    loader.Load();
                }
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

        public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress,
            CancellationToken cancellationToken)
        {
            var connection = await OpenAndGetSqliteConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
            bool doExplicitCommit = false;
            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (MySqlTransaction)(context.Database.CurrentTransaction == null ?
                                                      connection.BeginTransaction() :
                                                      context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                using (var stream = GetCsvStream(context, type, entities, tableInfo))
                {
                    var loader = InitLoader(connection, tableInfo, stream);
                    await loader.LoadAsync();
                }
                if (doExplicitCommit)
                {
                    await transaction.CommitAsync();
                }
            }
            finally
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }

        public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType,
            Action<decimal> progress) where T : class
        {
            var connection = OpenAndGetMySqlConnection(context, tableInfo.BulkConfig);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (MySqlTransaction)(context.Database.CurrentTransaction == null ?
                                                      connection.BeginTransaction() :
                                                      context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                using (var command = GetMySqlCommand(context, type, entities, tableInfo, connection, transaction))
                {
                    type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                    int rowsCopied = 0;
                    foreach (var item in entities)
                    {
                        LoadSqliteValues(tableInfo, item, command);
                        command.ExecuteNonQuery();
                        ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                    }

                    if (operationType != OperationType.Delete && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
                    {
                        command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();
                        long lastRowIdScalar = (long)command.ExecuteScalar();
                        string identityPropertyName = tableInfo.IdentityColumnName;
                        var identityPropertyInteger = false;
                        var identityPropertyUnsigned = false;
                        var identityPropertyByte = false;
                        var identityPropertyShort = false;

                        if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ulong))
                        {
                            identityPropertyUnsigned = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(uint))
                        {
                            identityPropertyInteger = true;
                            identityPropertyUnsigned = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(int))
                        {
                            identityPropertyInteger = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ushort))
                        {
                            identityPropertyShort = true;
                            identityPropertyUnsigned = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(short))
                        {
                            identityPropertyShort = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(byte))
                        {
                            identityPropertyByte = true;
                            identityPropertyUnsigned = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(sbyte))
                        {
                            identityPropertyByte = true;
                        }

                        for (int i = entities.Count - 1; i >= 0; i--)
                        {
                            if (identityPropertyByte)
                            {
                                if (identityPropertyUnsigned)
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (byte)lastRowIdScalar);
                                else
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (sbyte)lastRowIdScalar);
                            }
                            else if (identityPropertyShort)
                            {
                                if (identityPropertyUnsigned)
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ushort)lastRowIdScalar);
                                else
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (short)lastRowIdScalar);
                            }
                            else if (identityPropertyInteger)
                            {
                                if (identityPropertyUnsigned)
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (uint)lastRowIdScalar);
                                else
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (int)lastRowIdScalar);
                            }
                            else
                            {
                                if (identityPropertyUnsigned)
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ulong)lastRowIdScalar);
                                else
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], lastRowIdScalar);
                            }

                            lastRowIdScalar--;
                        }
                    }
                    if (doExplicitCommit)
                    {
                        transaction.Commit();
                    }
                }
            }
            finally
            {
                context.Database.CloseConnection();
            }
        }

        public async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType,
            Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            var connection = await OpenAndGetSqliteConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (MySqlTransaction)(context.Database.CurrentTransaction == null ?
                                                      connection.BeginTransaction() :
                                                      context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                using (var command = GetMySqlCommand(context, type, entities, tableInfo, connection, transaction))
                {

                    type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                    int rowsCopied = 0;

                    foreach (var item in entities)
                    {
                        LoadSqliteValues(tableInfo, item, command);
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                    }

                    if (operationType != OperationType.Delete && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
                    {
                        command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();
                        long lastRowIdScalar = (long)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                        string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;

                        var identityPropertyInteger = false;
                        var identityPropertyUnsigned = false;
                        var identityPropertyByte = false;
                        var identityPropertyShort = false;

                        if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ulong))
                        {
                            identityPropertyUnsigned = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(uint))
                        {
                            identityPropertyInteger = true;
                            identityPropertyUnsigned = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(int))
                        {
                            identityPropertyInteger = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ushort))
                        {
                            identityPropertyShort = true;
                            identityPropertyUnsigned = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(short))
                        {
                            identityPropertyShort = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(byte))
                        {
                            identityPropertyByte = true;
                            identityPropertyUnsigned = true;
                        }
                        else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(sbyte))
                        {
                            identityPropertyByte = true;
                        }

                        for (int i = entities.Count - 1; i >= 0; i--)
                        {
                            if (identityPropertyByte)
                            {
                                if (identityPropertyUnsigned)
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (byte)lastRowIdScalar);
                                else
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (sbyte)lastRowIdScalar);
                            }
                            else if (identityPropertyShort)
                            {
                                if (identityPropertyUnsigned)
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ushort)lastRowIdScalar);
                                else
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (short)lastRowIdScalar);
                            }
                            else if (identityPropertyInteger)
                            {
                                if (identityPropertyUnsigned)
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (uint)lastRowIdScalar);
                                else
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (int)lastRowIdScalar);
                            }
                            else
                            {
                                if (identityPropertyUnsigned)
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ulong)lastRowIdScalar);
                                else
                                    tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], lastRowIdScalar);
                            }

                            lastRowIdScalar--;
                        }
                    }
                    if (doExplicitCommit)
                    {
                        transaction.Commit();
                    }
                }
            }
            finally
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }

        public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            throw new NotImplementedException();
        }

        public Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress,
            CancellationToken cancellationToken) where T : class
        {
            throw new NotImplementedException();
        }

        public void Truncate(DbContext context, TableInfo tableInfo)
        {
            context.Database.ExecuteSqlRaw(SqlQueryBuilder.DeleteTable(tableInfo.FullTableName));
        }

        public async Task TruncateAsync(DbContext context, TableInfo tableInfo)
        {
            await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DeleteTable(tableInfo.FullTableName));
        }



        #region MySqlData

        internal static DataTable GetDataTable<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo)
        {
            DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);
            return dataTable;
        }

        internal static Stream GetCsvStream<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo)
        {
            var dt = GetDataTable<T>(context, type, entities, tableInfo);

            var csv = DataTableToCsv(dt, tableInfo.PropertyColumnNamesDict.Values.ToList());
            byte[] array = Encoding.UTF8.GetBytes(csv);
            return new MemoryStream(array);
        }
        internal static MySqlBulkLoader InitLoader(MySqlConnection connection, TableInfo tableInfo, Stream stream)
        {
            MySqlBulkLoader loader = new MySqlBulkLoader(connection)
            {

                FieldTerminator = ",",
                FieldQuotationCharacter = '"',
                EscapeCharacter = '\\',
                LineTerminator = "\r\n",
                SourceStream = stream,
                Local = true,
                NumberOfLinesToSkip = 0,
                TableName = tableInfo.TableName,
                CharacterSet = "utf8"
            };

            List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
            loader.Columns.AddRange(columnsList);
            return loader;
        }
        /// <summary>
        /// Common logic for two versions of GetDataTable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="type"></param>
        /// <param name="entities"></param>
        /// <param name="tableInfo"></param>
        /// <returns></returns>
        private static DataTable InnerGetDataTable<T>(DbContext context, ref Type type, IList<T> entities, TableInfo tableInfo)
        {
            var dataTable = new DataTable();
            var columnsDict = new Dictionary<string, object>();
            var ownedEntitiesMappedProperties = new HashSet<string>();

            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
            var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.GetTargetType().IsOwned()).ToDictionary(a => a.Name, a => a);
            var entityShadowFkPropertiesDict = entityType.GetProperties().Where(a => a.IsShadowProperty() && a.IsForeignKey()).ToDictionary(x => x.GetContainingForeignKeys().First().DependentToPrincipal.Name, a => a);
            var properties = type.GetProperties();
            var discriminatorColumn = tableInfo.ShadowProperties.Count == 0 ? null : tableInfo.ShadowProperties.ElementAt(0);

            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    var propertyEntityType = entityPropertiesDict[property.Name];
                    string columnName = propertyEntityType.GetColumnName();

                    var isConvertible = tableInfo.ConvertibleProperties.ContainsKey(columnName);
                    var propertyType = isConvertible ? tableInfo.ConvertibleProperties[columnName].ProviderClrType : property.PropertyType;

                    var underlyingType = Nullable.GetUnderlyingType(propertyType);
                    if (underlyingType != null)
                    {
                        propertyType = underlyingType;
                    }

                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
                else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                {
                    var fk = entityShadowFkPropertiesDict[property.Name];
                    entityPropertiesDict.TryGetValue(fk.GetColumnName(), out var entityProperty);
                    if (entityProperty == null) // BulkRead
                        continue;

                    var columnName = entityProperty.GetColumnName();
                    var propertyType = entityProperty.ClrType;
                    var underlyingType = Nullable.GetUnderlyingType(propertyType);
                    if (underlyingType != null)
                    {
                        propertyType = underlyingType;
                    }

                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
                {
                    Type navOwnedType = type.Assembly.GetType(property.PropertyType.FullName);

                    var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                    if (ownedEntityType == null)
                    {
                        ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(a => a.DefiningNavigationName == property.Name && a.DefiningEntityType.Name == entityType.Name);
                    }
                    var ownedEntityProperties = ownedEntityType.GetProperties().ToList();
                    var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                    foreach (var ownedEntityProperty in ownedEntityProperties)
                    {
                        if (!ownedEntityProperty.IsPrimaryKey())
                        {
                            string columnName = ownedEntityProperty.GetColumnName();
                            if (tableInfo.PropertyColumnNamesDict.ContainsValue(columnName))
                            {
                                ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                                ownedEntitiesMappedProperties.Add(property.Name + "_" + ownedEntityProperty.Name);
                            }
                        }
                    }

                    var innerProperties = property.PropertyType.GetProperties();
                    if (!tableInfo.LoadOnlyPKColumn)
                    {
                        foreach (var innerProperty in innerProperties)
                        {
                            if (ownedEntityPropertyNameColumnNameDict.ContainsKey(innerProperty.Name))
                            {
                                var columnName = ownedEntityPropertyNameColumnNameDict[innerProperty.Name];
                                var propertyName = $"{property.Name}_{innerProperty.Name}";

                                if (tableInfo.ConvertibleProperties.ContainsKey(propertyName))
                                {
                                    var convertor = tableInfo.ConvertibleProperties[propertyName];
                                    var underlyingType = Nullable.GetUnderlyingType(convertor.ProviderClrType) ?? convertor.ProviderClrType;
                                    dataTable.Columns.Add(columnName, underlyingType);
                                }
                                else
                                {
                                    var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                    dataTable.Columns.Add(columnName, ownedPropertyType);
                                }

                                columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                            }
                        }
                    }
                }
            }
            if (discriminatorColumn != null)
            {
                dataTable.Columns.Add(discriminatorColumn, typeof(string));
                columnsDict.Add(discriminatorColumn, type.Name);
            }

            foreach (var entity in entities)
            {
                foreach (var property in properties)
                {
                    var propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name) ? tableInfo.FastPropertyDict[property.Name].Get(entity) : null;

                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        string columnName = entityPropertiesDict[property.Name].GetColumnName();
                        if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                        {
                            propertyValue = tableInfo.ConvertibleProperties[columnName].ConvertToProvider.Invoke(propertyValue);
                        }
                    }

                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        columnsDict[property.Name] = propertyValue;
                    }
                    else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                    {
                        var fk = entityShadowFkPropertiesDict[property.Name];
                        var columnName = fk.GetColumnName();
                        entityPropertiesDict.TryGetValue(fk.GetColumnName(), out var entityProperty);
                        if (entityProperty == null) // BulkRead
                            continue;

                        columnsDict[columnName] = propertyValue == null ? null : fk.FindFirstPrincipal().PropertyInfo.GetValue(propertyValue);
                    }
                    else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                    {
                        var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                        foreach (var ownedProperty in ownedProperties)
                        {
                            var columnName = $"{property.Name}_{ownedProperty.Name}";
                            var ownedPropertyValue = tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                            if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                            {
                                var converter = tableInfo.ConvertibleProperties[columnName];
                                columnsDict[columnName] = propertyValue == null ? null : converter.ConvertToProvider.Invoke(ownedPropertyValue);
                            }
                            else
                            {
                                columnsDict[columnName] = propertyValue == null ? null : ownedPropertyValue;
                            }
                        }
                    }
                }
                var record = columnsDict.Values.ToArray();
                dataTable.Rows.Add(record);
            }

            return dataTable;
        }

        ///将DataTable转换为标准的CSV  
        /// </summary>  
        /// <param name="table">数据表</param>  
        /// <returns>返回标准的CSV</returns>  
        private static string DataTableToCsv(DataTable table, List<string> Columns)
        {
            //以半角逗号（即,）作分隔符，列为空也要表达其存在。  
            //列内容如存在半角逗号（即,）则用半角引号（即""）将该字段值包含起来。  
            //列内容如存在半角引号（即"）则应替换成半角双引号（""）转义，并用半角引号（即""）将该字段值包含起来。  
            StringBuilder sb = new StringBuilder();
            foreach (DataRow row in table.Rows)
            {
                for (var i = 0; i < Columns.Count(); i++)
                {
                    var colum = table.Columns[Columns[i]];
                    if (i != 0) sb.Append(",");

                    if (colum.DataType == typeof(string) && row[colum] != DBNull.Value)
                    {
                        sb.Append("\"" + row[colum].ToString().Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"");
                    }
                    else if (row[colum] == DBNull.Value)
                    {
                        sb.Append("\\N");
                    }
                    else sb.Append(row[colum].ToString());
                }
                sb.AppendLine();

            }
            return sb.ToString();
        }
        internal static MySqlCommand GetMySqlCommand<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, MySqlConnection connection, MySqlTransaction transaction)
        {
            MySqlCommand command = connection.CreateCommand();
            command.Transaction = transaction;

            var operationType = tableInfo.BulkConfig.OperationType;

            switch (operationType)
            {
                case OperationType.Insert:
                    command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.Insert);
                    break;
                case OperationType.InsertOrUpdate:
                    command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);
                    break;
                case OperationType.InsertOrUpdateDelete:
                    throw new NotSupportedException("Sqlite supports only UPSERT(analog for MERGE WHEN MATCHED) but does not have functionality to do: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'" +
                                                    "What can be done is to read all Data, find rows that are not in input List, then with those do the BulkDelete.");
                case OperationType.Update:
                    command.CommandText = SqlQueryBuilderSqlite.UpdateSetTable(tableInfo);
                    break;
                case OperationType.Delete:
                    command.CommandText = SqlQueryBuilderSqlite.DeleteFromTable(tableInfo);
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
                    var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                    /*var sqliteType = SqliteType.Text; // "String" || "Decimal" || "DateTime"
                    if (propertyType.Name == "Int16" || propertyType.Name == "Int32" || propertyType.Name == "Int64")
                        sqliteType = SqliteType.Integer;
                    if (propertyType.Name == "Float" || propertyType.Name == "Double")
                        sqliteType = SqliteType.Real;
                    if (propertyType.Name == "Guid" )
                        sqliteType = SqliteType.Blob; */

                    var parameter = new MySqlParameter($"@{columnName}", propertyType); // ,sqliteType // ,null
                    command.Parameters.Add(parameter);
                }
            }

            var shadowProperties = tableInfo.ShadowProperties;
            foreach (var shadowProperty in shadowProperties)
            {
                var parameter = new MySqlParameter($"@{shadowProperty}", typeof(string));
                command.Parameters.Add(parameter);
            }

            command.Prepare(); // Not Required (check if same efficiency when removed)
            return command;
        }


        internal static void LoadSqliteValues<T>(TableInfo tableInfo, T entity, MySqlCommand command)
        {
            var propertyColumnsDict = tableInfo.PropertyColumnNamesDict;
            foreach (var propertyColumn in propertyColumnsDict)
            {
                object value;
                if (!tableInfo.ShadowProperties.Contains(propertyColumn.Key))
                {
                    if (propertyColumn.Key.Contains(".")) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
                    {
                        var ownedPropertyNameList = propertyColumn.Key.Split('.');
                        var ownedPropertyName = ownedPropertyNameList[0];
                        var subPropertyName = ownedPropertyNameList[1];
                        var ownedFastProperty = tableInfo.FastPropertyDict[ownedPropertyName];
                        var ownedProperty = ownedFastProperty.Property;

                        var propertyType = Nullable.GetUnderlyingType(ownedProperty.GetType()) ?? ownedProperty.GetType();
                        if (!command.Parameters.Contains("@" + propertyColumn.Value))
                        {
                            var parameter = new MySqlParameter($"@{propertyColumn.Value}", propertyType);
                            command.Parameters.Add(parameter);
                        }

                        if (ownedProperty == null)
                        {
                            value = null;
                        }
                        else
                        {
                            var ownedPropertyValue = tableInfo.FastPropertyDict[ownedPropertyName].Get(entity);
                            var subPropertyFullName = $"{ownedPropertyName}_{subPropertyName}";
                            value = tableInfo.FastPropertyDict[subPropertyFullName]?.Get(ownedPropertyValue);
                        }
                    }
                    else
                    {
                        value = tableInfo.FastPropertyDict[propertyColumn.Key].Get(entity);
                    }
                }
                else // IsShadowProperty
                {
                    value = entity.GetType().Name;
                }

                if (tableInfo.ConvertibleProperties.ContainsKey(propertyColumn.Key) && value != DBNull.Value)
                {
                    value = tableInfo.ConvertibleProperties[propertyColumn.Key].ConvertToProvider.Invoke(value);
                }

                command.Parameters[$"@{propertyColumn.Value}"].Value = value ?? DBNull.Value;
            }
        }

        internal static async Task<MySqlConnection> OpenAndGetSqliteConnectionAsync(DbContext context, BulkConfig bulkConfig, CancellationToken cancellationToken)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return (MySqlConnection)context.Database.GetDbConnection();
        }

        internal static MySqlConnection OpenAndGetMySqlConnection(DbContext context, BulkConfig bulkConfig)
        {
            context.Database.OpenConnection();

            return (MySqlConnection)context.Database.GetDbConnection();
        }
        #endregion
    }
}
