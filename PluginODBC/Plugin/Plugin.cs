using System;
using System.Collections;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Newtonsoft.Json;
using PluginODBC.API;
//using PluginODBC.DataContracts;
using PluginODBC.Helper;
using PluginODBC.Interfaces;
using Pub;

namespace PluginODBC.Plugin
{
    public class Plugin : Publisher.PublisherBase
    {
        private readonly Func<Settings, IConnectionService> _connFactory;
        private readonly ServerStatus _server;
        private TaskCompletionSource<bool> _tcs;
        private IConnectionService _connService;

        public Plugin(Func<Settings, IConnectionService> connFactory = null)
        {
            _connFactory = connFactory ?? (s => new ConnectionService(s));
            _server = new ServerStatus();
        }

        /// <summary>
        /// Establishes a connection with Zoho CRM. Creates an authenticated http client and tests it.
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
                Logger.Error(e.Message);
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
                _server.DB = _connService.MakeDbObject();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
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
        /// Discovers schemas located in the users Zoho CRM instance
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns>Discovered schemas</returns>
        public override async Task<DiscoverSchemasResponse> DiscoverSchemas(DiscoverSchemasRequest request,
            ServerCallContext context)
        {
            Logger.Info("Discovering Schemas...");

            DiscoverSchemasResponse discoverSchemasResponse = new DiscoverSchemasResponse();

            // only return requested schemas if refresh mode selected
            if (request.Mode == DiscoverSchemasRequest.Types.Mode.Refresh)
            {
                
            }

            return discoverSchemasResponse;
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
            var schema = request.Schema;
            var limit = request.Limit;
            var limitFlag = request.Limit != 0;

            Logger.Info($"Publishing records for schema: {schema.Name}");

            try
            {
                var recordsCount = 0;

                Logger.Info($"Published {recordsCount} records");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
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
            _server.WriteConfigured = false;

            var writeSettings = new WriteSettings
            {
                CommitSLA = request.CommitSlaSeconds,
                Schema = request.Schema
            };

            _server.WriteSettings = writeSettings;
            _server.WriteConfigured = true;

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
                while (await requestStream.MoveNext(CancellationToken.None) && _server.Connected &&
                       _server.WriteConfigured)
                {
                    var record = requestStream.Current;
                    inCount++;

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
                Logger.Error(e.Message);
                throw;
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
        /// Gets a schema for a given module
        /// </summary>
        /// <param name="module"></param>
        /// <returns>returns a schema or null if unavailable</returns>
        private Task<Schema> GetSchemaForModule(string module)
        {
            try
            {
                // base schema to be added to
                var schema = new Schema
                {
                    Id = module,
                    Name = module,
                    Description = "",
                    PublisherMetaJson = null,
                    DataFlowDirection = Schema.Types.DataFlowDirection.ReadWrite
                };

                return Task.FromResult(schema);
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return null;
            }
        }

        /// <summary>
        /// Gets the property type of a value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private PropertyType GetPropertyType(dynamic value)
        {
            try
            {
                // try datetime
                if (DateTime.TryParse(value, out DateTime d))
                {
                    return PropertyType.Date;
                }

                // try int
                if (Int32.TryParse(value, out int i))
                {
                    return PropertyType.Integer;
                }

                // try float
                if (float.TryParse(value, out float f))
                {
                    return PropertyType.Float;
                }

                // try boolean
                if (bool.TryParse(value, out bool b))
                {
                    return PropertyType.Bool;
                }

                // default return string
                return PropertyType.String;
            }
            catch (Exception e)
            {
                // try object or array
                if (value is IEnumerable)
                {
                    return PropertyType.Json;
                }

                Logger.Error(e.Message);
                throw;
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
            return Task.FromResult("");
        }
    }
}