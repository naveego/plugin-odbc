using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginODBC.API.Factory;
using PluginODBC.DataContracts;
using Constants = PluginODBC.API.Utility.Constants;

namespace PluginODBC.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetMetaDataQuery = @"SELECT * FROM {0}.{1} WHERE {2} = '{3}'";

        public static async Task<ReplicationMetaData> GetPreviousReplicationMetaDataAsync(IConnectionFactory connFactory,
            string jobId,
            ReplicationTable table)
        {
            try
            {
                ReplicationMetaData replicationMetaData = null;

                // ensure replication metadata table
                await EnsureTableAsync(connFactory, table);

                // check if metadata exists
                var conn = connFactory.GetConnection();
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(
                    string.Format(GetMetaDataQuery, 
                        Utility.Utility.GetSafeName(table.SchemaName, '"'),
                        Utility.Utility.GetSafeName(table.TableName, '"'), 
                        Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId),
                        jobId),
                    conn);
                var reader = await cmd.ExecuteReaderAsync();

                if (reader.HasRows())
                {
                    // metadata exists
                    await reader.ReadAsync();
                    
                    var request = JsonConvert.DeserializeObject<PrepareWriteRequest>(
                        reader.GetValueById(Constants.ReplicationMetaDataRequest).ToString());
                    var shapeName = reader.GetValueById(Constants.ReplicationMetaDataReplicatedShapeName)
                        .ToString();
                    var shapeId = reader.GetValueById(Constants.ReplicationMetaDataReplicatedShapeId)
                        .ToString();
                    var timestamp = DateTime.Parse(reader.GetValueById(Constants.ReplicationMetaDataTimestamp)
                        .ToString());
                    
                    replicationMetaData = new ReplicationMetaData
                    {
                        Request = request,
                        ReplicatedShapeName = shapeName,
                        ReplicatedShapeId = shapeId,
                        Timestamp = timestamp
                    };
                }

                await conn.CloseAsync();

                return replicationMetaData;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                throw;
            }
        }
    }
}