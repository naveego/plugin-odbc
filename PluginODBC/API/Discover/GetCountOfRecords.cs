using System;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginODBC.API.Factory;

namespace PluginODBC.API.Discover
{
    public static partial class Discover
    {
        public static async Task<Count> GetCountOfRecords(IConnectionFactory connFactory, Schema schema)
        {
            // var query = schema.Query;
            // if (string.IsNullOrWhiteSpace(query))
            // {
            //     query = $"SELECT * FROM {schema.Id}";
            // }
            //
            // var conn = connFactory.GetConnection();
            // await conn.OpenAsync();
            //
            // var cmd = connFactory.GetCommand($"SELECT COUNT(*) as count FROM ({query}) as q", conn);
            // var reader = await cmd.ExecuteReaderAsync();
            //
            // var count = -1;
            // while (await reader.ReadAsync())
            // {
            //     count = Convert.ToInt32(reader.GetValueById("count"));
            // }
            //
            // await conn.CloseAsync();

            return new Count
            {
                Kind = Count.Types.Kind.Unavailable,
            };
        }
    }
}