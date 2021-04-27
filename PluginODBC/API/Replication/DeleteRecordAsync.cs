using System.Threading.Tasks;
using PluginODBC.API.Factory;
using PluginODBC.DataContracts;

namespace PluginODBC.API.Replication
{
    public static partial class Replication
    {
        private static readonly string DeleteRecordQuery = @"DELETE FROM {0}.{1}
WHERE {2} = '{3}'";

        public static async Task DeleteRecordAsync(IConnectionFactory connFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(string.Format(DeleteRecordQuery,
                    Utility.Utility.GetSafeName(table.SchemaName, '"'),
                    Utility.Utility.GetSafeName(table.TableName, '"'),
                    Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '"'),
                    primaryKeyValue
                ),
                conn);

            // check if table exists
            await cmd.ExecuteNonQueryAsync();

            await conn.CloseAsync();
        }
    }
}