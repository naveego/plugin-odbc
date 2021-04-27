using System;
using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using PluginODBC.API.Factory;
using PluginODBC.DataContracts;

namespace PluginODBC.API.Replication
{
    public static partial class Replication
    {
        private static readonly string EnsureTableQuery = @"SELECT COUNT(*) as c
FROM SYSIBM.SYSTABLES T
WHERE T.CREATOR = '{0}'
AND T.NAME = '{1}'";
        
        // private static readonly string EnsureTableQuery = @"SELECT * FROM {0}.{1}";

        public static async Task EnsureTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();
            
            Logger.Info($"Creating Schema... {table.SchemaName}");
            var cmd = connFactory.GetCommand($"CREATE SCHEMA {table.SchemaName}", conn);

            try
            {
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
            }
            
            cmd = connFactory.GetCommand(string.Format(EnsureTableQuery, table.SchemaName, table.TableName), conn);
            
            Logger.Info($"Creating Table: {string.Format(EnsureTableQuery, table.SchemaName, table.TableName)}");

            // check if table exists
            var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            var count = (int)reader.GetValueById("c");
            await conn.CloseAsync();
            
            if (count == 0)
            {
                // create table
                var querySb = new StringBuilder($@"CREATE TABLE
{Utility.Utility.GetSafeName(table.SchemaName, '"')}.{Utility.Utility.GetSafeName(table.TableName, '"')}(");
                var primaryKeySb = new StringBuilder("PRIMARY KEY (");
                var hasPrimaryKey = false;
                foreach (var column in table.Columns)
                {
                    querySb.Append(
                        $"{Utility.Utility.GetSafeName(column.ColumnName)} {column.DataType}{(column.PrimaryKey ? " NOT NULL UNIQUE" : "")},");
                    if (column.PrimaryKey)
                    {
                        primaryKeySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)},");
                        hasPrimaryKey = true;
                    }
                }

                if (hasPrimaryKey)
                {
                    primaryKeySb.Length--;
                    primaryKeySb.Append(")");
                    querySb.Append($"{primaryKeySb});");
                }
                else
                {
                    querySb.Length--;
                    querySb.Append(");");
                }

                var query = querySb.ToString();
                Logger.Info($"Creating Table: {query}");
                
                await conn.OpenAsync();

                cmd = connFactory.GetCommand(query, conn);

                await cmd.ExecuteNonQueryAsync();
                await conn.CloseAsync();
            }
        }
    }
}