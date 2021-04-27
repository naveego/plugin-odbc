using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginODBC.API.Factory;

namespace PluginODBC.API.Write
{
    public static partial class Write
    {
        private static readonly SemaphoreSlim WriteSemaphoreSlim = new SemaphoreSlim(1, 1);

        public static async Task<string> WriteRecordAsync(IConnectionFactory connFactory, Schema schema, Record record,
            IServerStreamWriter<RecordAck> responseStream)
        {
            // debug
            Logger.Debug($"Starting timer for {record.RecordId}");
            var timer = Stopwatch.StartNew();
            
            var conn = connFactory.GetConnection();

            try
            {
                // Check if query is empty
                if (string.IsNullOrWhiteSpace(schema.Query))
                {
                    return "Query not defined.";
                }

                var recObj = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);
                
                // open the connection
                await conn.OpenAsync();

                // create new db connection and command
                var command = connFactory.GetCommand(schema.Query, conn);

                // add parameters
                foreach (var property in schema.Properties)
                {
                    // set odbc type, name, and value to parameter
                    OdbcType type;
                    switch (property.Type)
                    {
                        case PropertyType.String:
                            type = OdbcType.VarChar;
                            break;
                        case PropertyType.Bool:
                            type = OdbcType.Bit;
                            break;
                        case PropertyType.Integer:
                            type = OdbcType.Int;
                            break;
                        case PropertyType.Float:
                            type = OdbcType.Double;
                            break;
                        case PropertyType.Decimal:
                            type = OdbcType.Decimal;
                            break;
                        default:
                            type = OdbcType.VarChar;
                            break;
                    }
                    
                    command.AddParameter(property.Id, recObj[property.Id], type);
                }

                // get a reader object for the query
                command.Prepare();
                var affected = await command.ExecuteNonQueryAsync();

                Logger.Info($"Modified {affected} record(s).");

                var ack = new RecordAck
                {
                    CorrelationId = record.CorrelationId,
                    Error = ""
                };
                await responseStream.WriteAsync(ack);

                timer.Stop();
                Logger.Debug($"Acknowledged Record {record.RecordId} time: {timer.ElapsedMilliseconds}");

                return "";
            }
            catch (Exception e)
            {
                await conn.CloseAsync();
                
                Logger.Error(e, $"Error writing record {e.Message}");
                // send ack
                var ack = new RecordAck
                {
                    CorrelationId = record.CorrelationId,
                    Error = e.Message
                };
                await responseStream.WriteAsync(ack);

                timer.Stop();
                Logger.Debug($"Failed Record {record.RecordId} time: {timer.ElapsedMilliseconds}");

                return e.Message;
            }
            finally
            {
                await conn.CloseAsync();
                WriteSemaphoreSlim.Release();
            }
        }
    }
}