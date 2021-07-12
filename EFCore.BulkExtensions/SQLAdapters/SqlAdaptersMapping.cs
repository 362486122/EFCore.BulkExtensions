using System;
using System.Collections.Generic;
using System.Linq;
using EFCore.BulkExtensions.SQLAdapters;
using EFCore.BulkExtensions.SQLAdapters.MySql;
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
        Oracle,
        PostgreSQL,
        Kdbndp, //金仓
        MySql,
    }

    public static class SqlAdaptersMapping
    {
        public static readonly Dictionary<DbServer, ISqlOperationsAdapter> SqlOperationAdapterMapping =
            new Dictionary<DbServer, ISqlOperationsAdapter>
            {
                {DbServer.Sqlite, new SqLiteOperationsAdapter()},
                {DbServer.SqlServer, new SqlOperationsServerAdapter()},
                {DbServer.Oracle,new OracleOperationsAdapter()},
                {DbServer.PostgreSQL,new PostgresqlAdapter() },
                {DbServer.Kdbndp,new KdbndpAdapter() },
                {DbServer.MySql,new MySqlAdapter() }
            };

        public static readonly Dictionary<DbServer, IQueryBuilderSpecialization> SqlQueryBuilderSpecializationMapping =
            new Dictionary<DbServer, IQueryBuilderSpecialization>
            {
                {DbServer.Sqlite, new SqLiteDialect()},
                {DbServer.SqlServer, new SqlServerDialect()},
                {DbServer.Oracle,new OracleDialect() },
                {DbServer.PostgreSQL,new PostgresqlDialect() },
                {DbServer.Kdbndp,new KdbndpDialect() },
                {DbServer.MySql,new MySqlDialect() }
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
            else if (context.Database.ProviderName.Contains(DbServer.PostgreSQL.ToString()))
            {
                return DbServer.PostgreSQL;
            }
            else if (context.Database.ProviderName.Contains("Kingbase"))
            {
                return DbServer.Kdbndp;
            }
            else if (context.Database.ProviderName.Contains("SqlServer"))
            {
                return DbServer.SqlServer;
            }
            else if(context.Database.ProviderName.Contains("MySql"))
            {
                return DbServer.MySql;
            }
            throw new NotSupportedException($"Not Support DbContext Provider:{context.Database.ProviderName}");
        }
    }
}
