using System;
using System.Collections.Generic;
using System.Linq;
using EFCore.BulkExtensions.SQLAdapters.Oracle;
using EFCore.BulkExtensions.SQLAdapters.Postgresql;
using EFCore.BulkExtensions.SQLAdapters.SQLite;
using EFCore.BulkExtensions.SQLAdapters.SQLServer;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters
{
    public enum DbServer
    {
        SqlServer,
        Sqlite,
        //PostgreSql, // ProviderName can be added as  optional Attribute of Enum so it can be defined when not the same, like Npgsql for PostgreSql
        //MySql,
        Oracle,
        PostgreSQL
    }

    public static class SqlAdaptersMapping
    {
        public static readonly Dictionary<DbServer, ISqlOperationsAdapter> SqlOperationAdapterMapping =
            new Dictionary<DbServer, ISqlOperationsAdapter>
            {
                {DbServer.Sqlite, new SqLiteOperationsAdapter()},
                {DbServer.SqlServer, new SqlOperationsServerAdapter()},
                {DbServer.Oracle,new OracleOperationsAdapter()},
                {DbServer.PostgreSQL,new PostgresqlAdapter() }
            };

        public static readonly Dictionary<DbServer, IQueryBuilderSpecialization> SqlQueryBuilderSpecializationMapping =
            new Dictionary<DbServer, IQueryBuilderSpecialization>
            {
                {DbServer.Sqlite, new SqLiteDialect()},
                {DbServer.SqlServer, new SqlServerDialect()},
                {DbServer.Oracle,new OracleDialect() },
                {DbServer.PostgreSQL,new PostgresqlDialect() }
            };

        public static ISqlOperationsAdapter CreateBulkOperationsAdapter(DbContext context)
        {
            var providerType = GetDatabaseType(context);
            return SqlOperationAdapterMapping[providerType];
        }

        public static IQueryBuilderSpecialization GetAdapterDialect(DbContext context)
        {
            var providerType = GetDatabaseType(context);
            return GetAdapterDialect(providerType);
        }

        public static IQueryBuilderSpecialization GetAdapterDialect(DbServer providerType)
        {
            return SqlQueryBuilderSpecializationMapping[providerType];
        }

        public static DbServer GetDatabaseType(DbContext context)
        {
            if (context.Database.ProviderName.EndsWith(DbServer.Sqlite.ToString()))
            {
                return DbServer.Sqlite;
            }
            else if (context.Database.ProviderName.Contains(DbServer.Oracle.ToString()))
            {
                return DbServer.Oracle;
            }
            else if(context.Database.ProviderName.Contains(DbServer.PostgreSQL.ToString()))
            {
                return DbServer.PostgreSQL;
            }
            return DbServer.SqlServer;
        }
    }
}
