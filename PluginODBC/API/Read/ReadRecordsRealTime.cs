using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Grpc.Core;
using LiteDB;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginODBC.API.Factory;

namespace PluginODBC.API.Read
{
    public static partial class Read
    {
        private static readonly string JournalQuery =
            @"SELECT JOCTRR, JOLIB, JOMBR, JOSEQN, JOENTT FROM {0}.{1} WHERE JOSEQN > {2} AND JOLIB = '{3}' AND JOMBR = '{4}' AND JOCODE = 'R'";

        private static readonly string MaxSeqQuery = @"select MAX(JOSEQN) as MAX_JOSEQN FROM {0}.{1}";

        private static readonly string RrnQuery = @"{0} {1} RRN({2}) = {3}";

        private const string CollectionName = "realtimerecord";

        // public static bool useTestQuery = false;
        // private static readonly string rrnTestQuery = @"{0} {1} {2}.{3}.RRN = {4}";

        public class RealTimeRecord
        {
            [BsonId] public string Id { get; set; }
            [BsonField] public Dictionary<string, object> Data { get; set; }
        }

        public static async Task<long> ReadRecordsRealTimeAsync(IConnectionFactory connFactory, ReadRequest request,
            IServerStreamWriter<Record> responseStream,
            ServerCallContext context, string permanentPath)
        {
            Logger.Info("Beginning to read records real time...");

            var schema = request.Schema;
            var jobVersion = request.DataVersions.JobDataVersion;
            var shapeVersion = request.DataVersions.ShapeDataVersion;
            var jobId = request.DataVersions.JobId;
            var recordsCount = 0;
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            try
            {
                // setup db directory
                var path = Path.Join(permanentPath, "realtime", jobId);
                Directory.CreateDirectory(path);

                using (var db = new LiteDatabase(Path.Join(path, "RealTimeReadRecords.db")))
                {
                    var realtimeRecordsCollection = db.GetCollection<RealTimeRecord>(CollectionName);
                    Logger.Info("Real time read initializing...");


                    var realTimeSettings =
                        JsonConvert.DeserializeObject<RealTimeSettings>(request.RealTimeSettingsJson);
                    var realTimeState = !string.IsNullOrWhiteSpace(request.RealTimeStateJson)
                        ? JsonConvert.DeserializeObject<RealTimeState>(request.RealTimeStateJson)
                        : new RealTimeState();

                    // check to see if we need to load all the data
                    if (jobVersion > realTimeState.JobVersion || shapeVersion > realTimeState.ShapeVersion)
                    {
                        var rrnKeys = new List<string>();
                        var rrnSelect = new StringBuilder();
                        foreach (var table in realTimeSettings.TableInformation)
                        {
                            rrnKeys.Add(table.GetTargetTableName());
                            rrnSelect.Append($",RRN({table.GetTargetTableAlias()}) as {table.GetTargetTableName()}");
                        }

                        // check for UNIONS
                        var unionPattern = @"[Uu][Nn][Ii][Oo][Nn]";
                        var unionResult = Regex.Split(request.Schema.Query, unionPattern);
                        var loadQuery = new StringBuilder();
                        if (unionResult.Length == 0)
                        {
                            var fromPattern = @"[Ff][Rr][Oo][Mm]";
                            var fromResult = Regex.Split(request.Schema.Query, fromPattern);
                            loadQuery.Append($"{fromResult[0]}{rrnSelect}\nFROM {fromResult[1]}");
                        }
                        else
                        {
                            var index = 0;
                            foreach (var union in unionResult)
                            {
                                var fromPattern = @"[Ff][Rr][Oo][Mm]";
                                var fromResult = Regex.Split(union, fromPattern);
                                loadQuery.Append($"{fromResult[0]}{rrnSelect}\nFROM {fromResult[1]}");
                                index++;
                                if (index != unionResult.Length)
                                {
                                    loadQuery.Append(" UNION ");
                                }
                            }
                        }

                        // delete existing collection
                        realtimeRecordsCollection.DeleteAll();

                        var cmd = connFactory.GetCommand(loadQuery.ToString(), conn);

                        var readerRealTime = await cmd.ExecuteReaderAsync();

                        // check for changes to process
                        if (readerRealTime.HasRows())
                        {
                            while (await readerRealTime.ReadAsync())
                            {
                                // record map to send to response stream
                                var recordMap = new Dictionary<string, object>();
                                var recordKeysMap = new Dictionary<string, object>();
                                foreach (var property in schema.Properties)
                                {
                                    try
                                    {
                                        switch (property.Type)
                                        {
                                            case PropertyType.String:
                                            case PropertyType.Text:
                                            case PropertyType.Decimal:
                                                recordMap[property.Id] =
                                                    readerRealTime.GetValueById(property.Id, '"').ToString();
                                                if (property.IsKey)
                                                {
                                                    recordKeysMap[property.Id] =
                                                        readerRealTime.GetValueById(property.Id, '"').ToString();
                                                }

                                                break;
                                            default:
                                                recordMap[property.Id] =
                                                    readerRealTime.GetValueById(property.Id, '"');
                                                if (property.IsKey)
                                                {
                                                    recordKeysMap[property.Id] =
                                                        readerRealTime.GetValueById(property.Id, '"');
                                                }

                                                break;
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        Logger.Error(e, $"No column with property Id: {property.Id}");
                                        Logger.Error(e, e.Message);
                                        recordMap[property.Id] = null;
                                    }
                                }

                                // build local db entry
                                foreach (var rrnKey in rrnKeys)
                                {
                                    try
                                    {
                                        var rrn = readerRealTime.GetValueById(rrnKey, '"');

                                        // Create new real time record
                                        var realTimeRecord = new RealTimeRecord
                                        {
                                            Id = $"{rrnKey}_{rrn}",
                                            Data = recordKeysMap
                                        };

                                        // Insert new record into db
                                        realtimeRecordsCollection.Upsert(realTimeRecord);
                                    }
                                    catch (Exception e)
                                    {
                                        Logger.Error(e, $"No column with property Id: {rrnKey}");
                                        Logger.Error(e, e.Message);
                                    }
                                }

                                // Publish record
                                var record = new Record
                                {
                                    Action = Record.Types.Action.Upsert,
                                    DataJson = JsonConvert.SerializeObject(recordMap)
                                };

                                await responseStream.WriteAsync(record);
                                recordsCount++;
                            }
                        }

                        // get current max sequence numbers
                        var maxSeqMap = realTimeSettings.TableInformation
                            .GroupBy(t => t.GetTargetJournalAlias())
                            .ToDictionary(t => t.Key, x => (long) 0);

                        foreach (var seqItem in maxSeqMap)
                        {
                            var idSplit = seqItem.Key.Split("_");
                            var seqCmd = connFactory.GetCommand(string.Format(MaxSeqQuery, idSplit[0], idSplit[1]),
                                conn);

                            var seqReader = await seqCmd.ExecuteReaderAsync();

                            if (seqReader.HasRows())
                            {
                                await seqReader.ReadAsync();
                                realTimeState.LastJournalEntryIdMap[seqItem.Key] =
                                    Convert.ToInt64(seqReader.GetValueById("MAX_JOSEQN"));
                            }
                        }

                        // commit base real time state
                        realTimeState.JobVersion = jobVersion;
                        realTimeState.ShapeVersion = shapeVersion;

                        var realTimeStateCommit = new Record
                        {
                            Action = Record.Types.Action.RealTimeStateCommit,
                            RealTimeStateJson = JsonConvert.SerializeObject(realTimeState)
                        };
                        await responseStream.WriteAsync(realTimeStateCommit);

                        Logger.Debug($"Got all records for reload");
                    }

                    Logger.Info("Real time read initialized.");

                    while (!context.CancellationToken.IsCancellationRequested)
                    {
                        Logger.Debug(
                            $"Getting all records after sequence {JsonConvert.SerializeObject(realTimeState.LastJournalEntryIdMap, Formatting.Indented)}");

                        await conn.OpenAsync();
                        
                        // get all changes for each table since last sequence number
                        foreach (var table in realTimeSettings.TableInformation)
                        {
                            Logger.Debug(
                                $"Getting all records after sequence {table.GetTargetJournalAlias()} {realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()]}");

                            // get all changes for table since last sequence number
                            var cmd = connFactory.GetCommand(string.Format(JournalQuery, table.TargetJournalLibrary,
                                table.TargetJournalName,
                                realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()],
                                table.TargetTableLibrary, table.TargetTableName), conn);

                            IReader reader;
                            try
                            {
                                reader = await cmd.ExecuteReaderAsync();
                            }
                            catch (Exception e)
                            {
                                Logger.Error(e, e.Message);
                                break;
                            }

                            // check for changes to process
                            if (reader.HasRows())
                            {
                                Logger.Debug(
                                    $"Found changes to records after sequence {table.GetTargetJournalAlias()} {realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()]}");

                                while (await reader.ReadAsync())
                                {
                                    var libraryName = reader.GetValueById("JOLIB", '"').ToString();
                                    var tableName = reader.GetValueById("JOMBR", '"').ToString();
                                    var relativeRecordNumber = reader.GetValueById("JOCTRR", '"').ToString();
                                    var journalSequenceNumber = reader.GetValueById("JOSEQN", '"').ToString();
                                    var deleteFlag = reader.GetValueById("JOENTT", '"').ToString() == "DL";
                                    var recordId = $"{libraryName}_{tableName}_{relativeRecordNumber}";

                                    // update maximum sequence number
                                    if (Convert.ToInt64(journalSequenceNumber) >
                                        realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()])
                                    {
                                        realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()] =
                                            Convert.ToInt64(journalSequenceNumber);
                                    }

                                    if (deleteFlag)
                                    {
                                        Logger.Info($"Deleting record {recordId}");

                                        // handle record deletion
                                        var realtimeRecord =
                                            realtimeRecordsCollection.FindOne(r => r.Id == recordId);
                                        if (realtimeRecord == null)
                                        {
                                            continue;
                                        }

                                        realtimeRecordsCollection.DeleteMany(r =>
                                            r.Id == recordId);

                                        var record = new Record
                                        {
                                            Action = Record.Types.Action.Delete,
                                            DataJson = JsonConvert.SerializeObject(realtimeRecord.Data)
                                        };

                                        await responseStream.WriteAsync(record);
                                        recordsCount++;
                                    }
                                    else
                                    {
                                        Logger.Info($"Upserting record {recordId}");

                                        var wherePattern = @"\s[^[]?[wW][hH][eE][rR][eE][^]]?\s";
                                        var whereReg = new Regex(wherePattern);
                                        var whereMatch = whereReg.Matches(request.Schema.Query);

                                        ICommand cmdRrn;
                                        if (whereMatch.Count == 1)
                                        {
                                            cmdRrn = connFactory.GetCommand(
                                                string.Format(RrnQuery, request.Schema.Query, "AND",
                                                    table.GetTargetTableAlias(),
                                                    relativeRecordNumber),
                                                conn);
                                        }
                                        else
                                        {
                                            cmdRrn = connFactory.GetCommand(
                                                string.Format(RrnQuery, request.Schema.Query, "WHERE",
                                                    table.GetTargetTableAlias(),
                                                    relativeRecordNumber), conn);
                                        }

                                        // read actual row
                                        try
                                        {
                                            var readerRrn = await cmdRrn.ExecuteReaderAsync();

                                            if (readerRrn.HasRows())
                                            {
                                                while (await readerRrn.ReadAsync())
                                                {
                                                    var recordMap = new Dictionary<string, object>();
                                                    var recordKeysMap = new Dictionary<string, object>();
                                                    foreach (var property in schema.Properties)
                                                    {
                                                        try
                                                        {
                                                            switch (property.Type)
                                                            {
                                                                case PropertyType.String:
                                                                case PropertyType.Text:
                                                                case PropertyType.Decimal:
                                                                    recordMap[property.Id] =
                                                                        readerRrn.GetValueById(property.Id, '"')
                                                                            .ToString();
                                                                    if (property.IsKey)
                                                                    {
                                                                        recordKeysMap[property.Id] =
                                                                            readerRrn.GetValueById(property.Id, '"')
                                                                                .ToString();
                                                                    }

                                                                    break;
                                                                default:
                                                                    recordMap[property.Id] =
                                                                        readerRrn.GetValueById(property.Id, '"');
                                                                    if (property.IsKey)
                                                                    {
                                                                        recordKeysMap[property.Id] =
                                                                            readerRrn.GetValueById(property.Id, '"');
                                                                    }

                                                                    break;
                                                            }

                                                            // update local db
                                                            var realTimeRecord = new RealTimeRecord
                                                            {
                                                                Id = recordId,
                                                                Data = recordKeysMap
                                                            };

                                                            // upsert record into db
                                                            realtimeRecordsCollection.Upsert(realTimeRecord);
                                                        }
                                                        catch (Exception e)
                                                        {
                                                            Logger.Error(e,
                                                                $"No column with property Id: {property.Id}");
                                                            Logger.Error(e, e.Message);
                                                            recordMap[property.Id] = null;
                                                        }
                                                    }

                                                    var record = new Record
                                                    {
                                                        Action = Record.Types.Action.Upsert,
                                                        DataJson = JsonConvert.SerializeObject(recordMap)
                                                    };

                                                    await responseStream.WriteAsync(record);
                                                    recordsCount++;
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            Logger.Error(e, e.Message);
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        // commit state for last run
                        var realTimeStateCommit = new Record
                        {
                            Action = Record.Types.Action.RealTimeStateCommit,
                            RealTimeStateJson = JsonConvert.SerializeObject(realTimeState)
                        };
                        await responseStream.WriteAsync(realTimeStateCommit);

                        Logger.Info(
                            $"Got all records up to sequence {JsonConvert.SerializeObject(realTimeState.LastJournalEntryIdMap, Formatting.Indented)}");

                        await Task.Delay(realTimeSettings.PollingIntervalSeconds * (1000), context.CancellationToken);
                    }
                }
            }
            catch (TaskCanceledException e)
            {
                Logger.Info($"Operation cancelled {e.Message}");
                await conn.CloseAsync();
                return recordsCount;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }

            return recordsCount;
        }
    }
}