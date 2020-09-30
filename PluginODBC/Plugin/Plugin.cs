using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginODBC.API;
using PluginODBC.DataContracts;
using PluginODBC.Helper;
using PluginODBC.Interfaces;


namespace PluginODBC.Plugin
{
    public class Plugin : Publisher.PublisherBase
    {
        private readonly Func<Settings, IConnectionFactoryService> _connFactory;
        private readonly ServerStatus _server;
        private TaskCompletionSource<bool> _tcs;
        private IConnectionFactoryService _connService;

        public Plugin(Func<Settings, IConnectionFactoryService> connFactory = null)
        {
            _connFactory = connFactory ?? (s => new ConnectionFactoryService(s));
            _server = new ServerStatus();
        }
        
        /// <summary>
        /// Configures the plugin
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<ConfigureResponse> Configure(ConfigureRequest request, ServerCallContext context)
        {
            Logger.Debug("Got configure request");
            Logger.Debug(JsonConvert.SerializeObject(request, Formatting.Indented));
            
            // ensure all directories are created
            Directory.CreateDirectory(request.TemporaryDirectory);
            Directory.CreateDirectory(request.PermanentDirectory);
            Directory.CreateDirectory(request.LogDirectory);
            
            // configure logger
            Logger.SetLogLevel(request.LogLevel);
            Logger.Init(request.LogDirectory);

            _server.Config = request;

            return Task.FromResult(new ConfigureResponse());
        }

        /// <summary>
        /// Establishes a connection with an odbc data source
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>A message indicating connection success</returns>
        public override Task<ConnectResponse> Connect(ConnectRequest request, ServerCallContext context)
        {
            _server.Connected = false;

            Logger.Info("Connecting...");

            var settings = JsonConvert.DeserializeObject<Settings>(request.SettingsJson);

            // validate settings passed in
            try
            {
                _server.Settings = settings;
                _server.Settings.Validate();
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                return Task.FromResult(new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = "",
                    OauthError = "",
                    SettingsError = e.Message
                });
            }

            // attempt to connect to data source
            try
            {
                _connService = _connFactory(_server.Settings);
                var connection = _connService.MakeConnectionObject();
                connection.Open();
                connection.Close();
                _server.Connected = true;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                return Task.FromResult(new ConnectResponse
                {
                    OauthStateJson = request.OauthStateJson,
                    ConnectionError = e.Message,
                    OauthError = "",
                    SettingsError = ""
                });
            }

            Logger.Info("Connected.");

            return Task.FromResult(new ConnectResponse
            {
                OauthStateJson = "",
                ConnectionError = "",
                OauthError = "",
                SettingsError = ""
            });
        }

        /// <summary>
        /// Connects the session by forwarding requests to Connect
        /// </summary>
        /// <param name="request"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task ConnectSession(ConnectRequest request,
            IServerStreamWriter<ConnectResponse> responseStream, ServerCallContext context)
        {
            Logger.Info("Connecting session...");

            // create task to wait for disconnect to be called
            _tcs?.SetResult(true);
            _tcs = new TaskCompletionSource<bool>();

            // call connect method
            var response = await Connect(request, context);

            await responseStream.WriteAsync(response);

            Logger.Info("Session connected.");

            // wait for disconnect to be called
            await _tcs.Task;
        }


        /// <summary>
        /// Discovers schemas based on a query
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>Discovered schemas</returns>
        public override async Task<DiscoverSchemasResponse> DiscoverSchemas(DiscoverSchemasRequest request,
            ServerCallContext context)
        {
            Logger.SetLogPrefix("discovery_");
            Logger.Info("Discovering Schemas...");

            DiscoverSchemasResponse discoverSchemasResponse = new DiscoverSchemasResponse();

            // only return requested schemas if refresh mode selected
            if (request.Mode == DiscoverSchemasRequest.Types.Mode.All)
            {
                Logger.Info("Plugin does not support auto schema discovery.");
                return discoverSchemasResponse;
            }

            try
            {
                var refreshSchemas = request.ToRefresh;

                Logger.Info($"Refresh schemas attempted: {refreshSchemas.Count}");

                var tasks = refreshSchemas.Select(GetSchemaProperties)
                    .ToArray();

                await Task.WhenAll(tasks);

                discoverSchemasResponse.Schemas.AddRange(tasks.Where(x => x.Result != null).Select(x => x.Result));

                // return all schemas 
                Logger.Info($"Schemas returned: {discoverSchemasResponse.Schemas.Count}");
                return discoverSchemasResponse;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                return new DiscoverSchemasResponse();
            }
        }

        /// <summary>
        /// Publishes a stream of data for a given schema
        /// </summary>
        /// <param name="request"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task ReadStream(ReadRequest request, IServerStreamWriter<Record> responseStream,
            ServerCallContext context)
        {
            Logger.SetLogPrefix($"{request.JobId}_");
            var schema = request.Schema;
            var limit = request.Limit;
            var limitFlag = request.Limit != 0;

            Logger.Info($"Publishing records for schema: {schema.Name}");

            // pre publish query
            try
            {
                // Check if query is empty
                if (!string.IsNullOrWhiteSpace(_server.Settings.PrePublishQuery))
                {
                    // create new db connection and command
                    var connection = _connService.MakeConnectionObject();
                    var command = _connService.MakeCommandObject(_server.Settings.PrePublishQuery, connection);

                    // open the connection
                    connection.Open();

                    // execute query
                    var reader = command.ExecuteReader();

                    // close reader and connection
                    reader.Close();
                    connection.Close();
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                return;
            }

            try
            {
                var recordsCount = 0;

                // Check if query is empty
                if (string.IsNullOrWhiteSpace(schema.Query))
                {
                    Logger.Info("Query not defined.");
                    return;
                }

                // create new db connection and command
                var connection = _connService.MakeConnectionObject();
                var command = _connService.MakeCommandObject(schema.Query, connection);

                // open the connection
                connection.Open();

                // get a reader object for the query
                var reader = command.ExecuteReader();

                if (reader.HasRows)
                {
                    while (reader.Read() && _server.Connected)
                    {
                        // build record map
                        var recordMap = new Dictionary<string, object>();

                        foreach (var property in schema.Properties)
                        {
                            try
                            {
                                switch (property.Type)
                                {
                                    case PropertyType.String:
                                        recordMap[property.Id] = reader[property.Id].ToString();
                                        break;
                                    default:
                                        recordMap[property.Id] = reader[property.Id];
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                Logger.Error(e, $"No column with property Id: {property.Id}");
                                Logger.Error(e, e.Message);
                                recordMap[property.Id] = "";
                            }
                        }

                        // create record
                        var record = new Record
                        {
                            Action = Record.Types.Action.Upsert,
                            DataJson = JsonConvert.SerializeObject(recordMap)
                        };

                        // stop publishing if the limit flag is enabled and the limit has been reached or the server is disconnected
                        if ((limitFlag && recordsCount == limit) || !_server.Connected)
                        {
                            break;
                        }

                        // publish record
                        await responseStream.WriteAsync(record);
                        recordsCount++;
                    }
                }

                // close reader and connection
                reader.Close();
                connection.Close();

                Logger.Info($"Published {recordsCount} records");
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                return;
            }

            // post publish query
            try
            {
                // Check if query is empty
                if (!string.IsNullOrWhiteSpace(_server.Settings.PostPublishQuery))
                {
                    // create new db connection and command
                    var connection = _connService.MakeConnectionObject();
                    var command = _connService.MakeCommandObject(_server.Settings.PostPublishQuery, connection);

                    // open the connection
                    connection.Open();

                    // execute query
                    var reader = command.ExecuteReader();

                    // close reader and connection
                    reader.Close();
                    connection.Close();
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
            }
        }

        /// <summary>
        /// Creates a form and handles form updates for write backs
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<ConfigureWriteResponse> ConfigureWrite(ConfigureWriteRequest request,
            ServerCallContext context)
        {
            Logger.Info("Configuring write...");

            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {
                    "properties", new Dictionary<string, object>
                    {
                        {
                            "Query", new Dictionary<string, string>
                            {
                                {"type", "string"},
                                {"title", "Query"},
                                {"description", "Query to execute for write back with parameter place holders"},
                            }
                        },
                        {
                            "Parameters", new Dictionary<string, object>
                            {
                                {"type", "array"},
                                {"title", "Parameters"},
                                {"description", "Parameters to replace the place holders in the query"},
                                {
                                    "items", new Dictionary<string, object>
                                    {
                                        {"type", "object"},
                                        {
                                            "properties", new Dictionary<string, object>
                                            {
                                                {
                                                    "ParamName", new Dictionary<string, object>
                                                    {
                                                        {"type", "string"},
                                                        {"title", "Name"}
                                                    }
                                                },
                                                {
                                                    "ParamType", new Dictionary<string, object>
                                                    {
                                                        {"type", "string"},
                                                        {"title", "Type"},
                                                        {
                                                            "enum", new[]
                                                            {
                                                                "string", "bool", "int", "float", "decimal"
                                                            }
                                                        },
                                                        {
                                                            "enumNames", new[]
                                                            {
                                                                "String", "Bool", "Int", "Float", "Decimal"
                                                            }
                                                        },
                                                    }
                                                },
                                            }
                                        },
                                        {
                                            "required", new[]
                                            {
                                                "ParamName", "ParamType"
                                            }
                                        }
                                    }
                                }
                            }
                        },
                    }
                },
                {
                    "required", new[]
                    {
                        "Query"
                    }
                }
            };

            var uiJsonObj = new Dictionary<string, object>
            {
                {
                    "ui:order", new[]
                    {
                        "Query", "Parameters"
                    }
                },
                {
                    "Query", new Dictionary<string, object>
                    {
                        {"ui:widget", "textarea"}
                    }
                }
            };

            var schemaJson = JsonConvert.SerializeObject(schemaJsonObj);
            var uiJson = JsonConvert.SerializeObject(uiJsonObj);

            // if first call 
            if (request.Form == null || request.Form.DataJson == "")
            {
                return Task.FromResult(new ConfigureWriteResponse
                {
                    Form = new ConfigurationFormResponse
                    {
                        DataJson = "",
                        DataErrorsJson = "",
                        Errors = { },
                        SchemaJson = schemaJson,
                        UiJson = uiJson,
                        StateJson = ""
                    },
                    Schema = null
                });
            }

            try
            {
                // get form data
                var formData = JsonConvert.DeserializeObject<ConfigureWriteFormData>(request.Form.DataJson);

                // base schema to return
                var schema = new Schema
                {
                    Id = "",
                    Name = "",
                    Query = formData.Query,
                    DataFlowDirection = Schema.Types.DataFlowDirection.Write
                };

                // add parameters to properties
                foreach (var param in formData.Parameters)
                {
                    schema.Properties.Add(new Property
                    {
                        Id = param.ParamName,
                        Name = param.ParamName,
                        Type = GetWriteBackType(param.ParamType)
                    });
                }

                return Task.FromResult(new ConfigureWriteResponse
                {
                    Form = new ConfigurationFormResponse
                    {
                        DataJson = request.Form.DataJson,
                        Errors = { },
                        SchemaJson = schemaJson,
                        UiJson = uiJson,
                        StateJson = request.Form.StateJson
                    },
                    Schema = schema
                });
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                return Task.FromResult(new ConfigureWriteResponse
                {
                    Form = new ConfigurationFormResponse
                    {
                        DataJson = request.Form.DataJson,
                        Errors = {e.Message},
                        SchemaJson = schemaJson,
                        UiJson = uiJson,
                        StateJson = request.Form.StateJson
                    },
                    Schema = null
                });
            }
        }

        /// <summary>
        /// Prepares the plugin to handle a write request
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<PrepareWriteResponse> PrepareWrite(PrepareWriteRequest request, ServerCallContext context)
        {
            Logger.Info("Preparing write...");
            _server.WriteConfigured = false;

            var writeSettings = new WriteSettings
            {
                CommitSLA = request.CommitSlaSeconds,
                Schema = request.Schema
            };

            _server.WriteSettings = writeSettings;
            _server.WriteConfigured = true;

            Logger.Info("Write prepared.");
            return Task.FromResult(new PrepareWriteResponse());
        }

        /// <summary>
        /// Takes in records and writes them out then sends acks back to the client
        /// </summary>
        /// <param name="requestStream"></param>
        /// <param name="responseStream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task WriteStream(IAsyncStreamReader<Record> requestStream,
            IServerStreamWriter<RecordAck> responseStream, ServerCallContext context)
        {
            try
            {
                Logger.Info("Writing records...");
                var schema = _server.WriteSettings.Schema;
                var sla = _server.WriteSettings.CommitSLA;
                var inCount = 0;
                var outCount = 0;

                // get next record to publish while connected and configured
                while (await requestStream.MoveNext(context.CancellationToken) && _server.Connected &&
                       _server.WriteConfigured)
                {
                    var record = requestStream.Current;
                    inCount++;

                    Logger.Debug($"Got Record {record.DataJson}");

                    // send record to source system
                    // timeout if it takes longer than the sla
                    var task = Task.Run(() => PutRecord(schema, record));
                    if (task.Wait(TimeSpan.FromSeconds(sla)))
                    {
                        // send ack
                        var ack = new RecordAck
                        {
                            CorrelationId = record.CorrelationId,
                            Error = task.Result
                        };
                        await responseStream.WriteAsync(ack);

                        if (String.IsNullOrEmpty(task.Result))
                        {
                            outCount++;
                        }
                    }
                    else
                    {
                        // send timeout ack
                        var ack = new RecordAck
                        {
                            CorrelationId = record.CorrelationId,
                            Error = "timed out"
                        };
                        await responseStream.WriteAsync(ack);
                    }
                }

                Logger.Info($"Wrote {outCount} of {inCount} records.");
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
            }
        }

        /// <summary>
        /// Handles disconnect requests from the agent
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<DisconnectResponse> Disconnect(DisconnectRequest request, ServerCallContext context)
        {
            // clear connection
            _server.Connected = false;
            _server.Settings = null;
            _server.WriteSettings = null;

            // alert connection session to close
            if (_tcs != null)
            {
                _tcs.SetResult(true);
                _tcs = null;
            }

            Logger.Info("Disconnected");
            return Task.FromResult(new DisconnectResponse());
        }

        /// <summary>
        /// Gets a schema for a given query
        /// </summary>
        /// <param name="schema"></param>
        /// <returns>A schema or null</returns>
        private Task<Schema> GetSchemaProperties(Schema schema)
        {
            try
            {
                Logger.Info($"Getting schema for query:\n{schema.Query}");

                // Check if query is empty
                if (string.IsNullOrWhiteSpace(schema.Query))
                {
                    return null;
                }

                // create new db connection and command
                var connection = _connService.MakeConnectionObject();
                var command = _connService.MakeCommandObject(schema.Query, connection);

                // open the connection
                connection.Open();

                Logger.Info($"Opened connection for query:\n{schema.Query}");

                // get a reader object for the query
                var reader = command.ExecuteReader();

                Logger.Info($"Got reader for query:\n{schema.Query}");

                // get metadata table object for reader
                var schemaTable = reader.GetSchemaTable();

                if (schemaTable != null)
                {
                    Logger.Info($"Got schema table for query:\n{schema.Query}");
                    // counter for unknown columns with no name
                    var unnamedColIndex = 0;

                    // get each column and create a property for the column
                    foreach (DataRow row in schemaTable.Rows)
                    {
                        // get the column name
                        var colName = row["ColumnName"].ToString();
                        if (string.IsNullOrWhiteSpace(colName))
                        {
                            colName = $"UNKNOWN_{unnamedColIndex}";
                            unnamedColIndex++;
                        }

                        // create property
                        var property = new Property
                        {
                            Id = colName,
                            Name = colName,
                            Description = "",
                            Type = GetPropertyType(row),
                            TypeAtSource = row["DataType"].ToString(),
                            IsKey = Boolean.Parse(row["IsKey"].ToString()),
                            IsNullable = Boolean.Parse(row["AllowDBNull"].ToString()),
                            IsCreateCounter = false,
                            IsUpdateCounter = false,
                            PublisherMetaJson = ""
                        };

                        // add property to schema
                        schema.Properties.Add(property);
                    }
                }
                else
                {
                    Logger.Info($"Failed to get schema table for query:\n{schema.Query}");
                    Logger.Info($"Returning null schema for query:\n{schema.Query}");
                    schema = null;
                }

                // close reader and connection
                reader.Close();
                connection.Close();

                return Task.FromResult(schema);
            }
            catch (Exception e)
            {
                Logger.Info($"Error processing query:\n{schema?.Query}");
                Logger.Error(e, e.Message);
                Logger.Info($"Returning null schema for query:\n{schema?.Query}");
                return null;
            }
        }

        /// <summary>
        /// Gets the property type of a column
        /// </summary>
        /// <param name="col"></param>
        /// <returns></returns>
        private PropertyType GetPropertyType(DataRow col)
        {
            var type = Type.GetType(col["DataType"].ToString());
            switch (true)
            {
                case bool _ when type == typeof(bool):
                    return PropertyType.Bool;
                case bool _ when type == typeof(int):
                case bool _ when type == typeof(long):
                    return PropertyType.Integer;
                case bool _ when type == typeof(float):
                case bool _ when type == typeof(double):
                    return PropertyType.Float;
                case bool _ when type == typeof(DateTime):
                    return PropertyType.Datetime;
                case bool _ when type == typeof(string):
                    if (Int64.Parse(col["ColumnSize"].ToString()) > 1024)
                    {
                        return PropertyType.Text;
                    }

                    return PropertyType.String;
                default:
                    return PropertyType.String;
            }
        }

        /// <summary>
        /// Gets the property type for the provided write back type from form
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private PropertyType GetWriteBackType(string type)
        {
            switch (type)
            {
                case "string":
                    return PropertyType.String;
                case "bool":
                    return PropertyType.Bool;
                case "int":
                    return PropertyType.Integer;
                case "float":
                    return PropertyType.Float;
                case "decimal":
                    return PropertyType.Decimal;
                default:
                    return PropertyType.String;
            }
        }

        /// <summary>
        /// Writes a record out to source system
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <returns></returns>
        private Task<string> PutRecord(Schema schema, Record record)
        {
            try
            {
                // Check if query is empty
                if (string.IsNullOrWhiteSpace(schema.Query))
                {
                    return Task.FromResult("Query not defined.");
                }

                var recObj = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);

                // create new db connection and command
                var connection = _connService.MakeConnectionObject();
                var command = _connService.MakeCommandObject(schema.Query, connection);

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

                    var param = command.AddParameter(property.Id, type);
                    param.Value = recObj[property.Id];
                }

                // open the connection
                connection.Open();

                // get a reader object for the query
                command.Prepare();
                var reader = command.ExecuteReader();

                Logger.Info($"Modified {reader.RecordsAffected} record(s).");

                // close reader and connection
                reader.Close();
                connection.Close();

                return Task.FromResult("");
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                return Task.FromResult(e.Message);
            }
        }
    }
}