using System.Threading.Tasks;
using PluginODBC.API.Factory;
using PluginODBC.DataContracts;

namespace PluginODBC.API.Replication
{
    public static partial class Replication
    {
        private static readonly string RecordExistsQuery = @"SELECT COUNT(*) as c
FROM (
SELECT * FROM {0}.{1}
WHERE {2} = '{3}'    
) as q";

        public static async Task<bool> RecordExistsAsync(IConnectionFactory connFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();
            
            var cmd = connFactory.GetCommand(string.Format(RecordExistsQuery,
                    Utility.Utility.GetSafeName(table.SchemaName, '"'),
                    Utility.Utility.GetSafeName(table.TableName, '"'),
                    Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '"'),
                    primaryKeyValue
                ),
                conn);

            // check if record exists
            var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            var count = (int) reader.GetValueById("c");
            await conn.CloseAsync();

            return count != 0;
        }
    }
}