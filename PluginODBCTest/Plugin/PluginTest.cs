using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Moq;
using Naveego.Sdk.Plugins;
using PluginODBC.Helper;
using PluginODBC.Interfaces;

using Xunit;
using Record = Naveego.Sdk.Plugins.Record;

namespace PluginODBCTest.Plugin
{
    public class PluginTest
    {
        private readonly Mock<IConnectionService> _mockOdbcConnection = new Mock<IConnectionService>();

        private ConnectRequest GetConnectSettings(bool mockConnection = true)
        {
            return mockConnection ? 
            new ConnectRequest
            {
                SettingsJson =
                    "{\"ConnectionString\":\"test connection\",\"Password\":\"password\",\"PrePublishQuery\":\"\",\"PostPublishQuery\":\"\"}",
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            } :
            // update the settings json for testing with real credentials
            new ConnectRequest
            {
                SettingsJson =
                    "{\"ConnectionString\":\"SET ME\",\"Password\":\"SET ME\",\"PrePublishQuery\":\"\",\"PostPublishQuery\":\"\"}",
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            } ;
        }

        private Func<Settings, IConnectionFactoryService> GetMockConnectionFactory()
        {
            return cs =>
            {
                var mockService = new Mock<IConnectionFactoryService>();

                mockService.Setup(m => m.MakeConnectionObject())
                    .Returns(_mockOdbcConnection.Object);

                mockService.Setup(m => m.MakeCommandObject("DiscoverSchemas", _mockOdbcConnection.Object))
                    .Returns(() =>
                    {
                        var mockOdbcCommand = new Mock<ICommandService>();

                        mockOdbcCommand.Setup(c => c.ExecuteReader())
                            .Returns(() =>
                            {
                                var mockReader = new Mock<IReaderService>();

                                mockReader.Setup(r => r.GetSchemaTable())
                                    .Returns(() =>
                                    {
                                        var mockSchemaTable = new DataTable();
                                        mockSchemaTable.Columns.AddRange(new[]
                                            {
                                                new DataColumn
                                                {
                                                    ColumnName = "ColumnName"
                                                },
                                                new DataColumn
                                                {
                                                    ColumnName = "DataType"
                                                },
                                                new DataColumn
                                                {
                                                    ColumnName = "IsKey"
                                                },
                                                new DataColumn
                                                {
                                                    ColumnName = "AllowDBNull"
                                                },
                                            }
                                        );

                                        var mockRow = mockSchemaTable.NewRow();
                                        mockRow["ColumnName"] = "TestCol";
                                        mockRow["DataType"] = "System.Int64";
                                        mockRow["IsKey"] = true;
                                        mockRow["AllowDBNull"] = false;

                                        mockSchemaTable.Rows.Add(mockRow);


                                        return mockSchemaTable;
                                    });

                                return mockReader.Object;
                            });

                        return mockOdbcCommand.Object;
                    });

                mockService.Setup(m => m.MakeCommandObject("ReadStream", _mockOdbcConnection.Object))
                    .Returns(() =>
                    {
                        var mockOdbcCommand = new Mock<ICommandService>();

                        mockOdbcCommand.Setup(c => c.ExecuteReader())
                            .Returns(() =>
                            {
                                var mockReader = new Mock<IReaderService>();

                                mockReader.Setup(r => r.HasRows)
                                    .Returns(true);

                                var readToggle = new List<bool> {true, true, false};
                                var readIndex = 0;
                                mockReader.Setup(r => r.Read())
                                    .Returns(() => readToggle[readIndex])
                                    .Callback(() => readIndex++);

                                mockReader.Setup(r => r["TestCol"])
                                    .Returns("data");

                                return mockReader.Object;
                            });

                        return mockOdbcCommand.Object;
                    });

                mockService.Setup(m => m.MakeCommandObject("WriteStream", _mockOdbcConnection.Object))
                    .Returns(() =>
                    {
                        var mockOdbcCommand = new Mock<ICommandService>();

                        mockOdbcCommand.Setup(c => c.ExecuteReader())
                            .Returns(() =>
                            {
                                var mockReader = new Mock<IReaderService>();

                                mockReader.Setup(r => r.RecordsAffected)
                                    .Returns(1);

                                return mockReader.Object;
                            });

                        mockOdbcCommand.Setup(c => c.AddParameter("TestCol", OdbcType.Int))
                            .Returns(new OdbcParameter());

                        return mockOdbcCommand.Object;
                    });

                return mockService.Object;
            };
        }

        private Schema GetTestSchema(string query)
        {
            return new Schema
            {
                Id = "test",
                Name = "test",
                Query = query
            };
        }

        private List<Record> GetTestRecords()
        {
            return new List<Record>
            {
                new Record
                {
                    CorrelationId = "test",
                    DataJson = "{\"TestCol\":\"\"}"
                },
                new Record
                {
                    CorrelationId = "more-test",
                    DataJson = "{\"TestCol\":\"\"}"
                }
            };
        }

        [Fact]
        public async Task ConnectSessionTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();
            var disconnectRequest = new DisconnectRequest();

            // act
            var response = client.ConnectSession(request);
            var responseStream = response.ResponseStream;
            var records = new List<ConnectResponse>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
                client.Disconnect(disconnectRequest);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConnectTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();

            // act
            var response = client.Connect(request);

            // assert
            Assert.IsType<ConnectResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasAllTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.All,
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Empty(response.Schemas);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {GetTestSchema("DiscoverSchemas")}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal("test", schema.Id);
            Assert.Equal("test", schema.Name);
            Assert.Single(schema.Properties);

            var property = schema.Properties[0];
            Assert.Equal("TestCol", property.Id);
            Assert.Equal("TestCol", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.Integer, property.Type);
            Assert.True(property.IsKey);
            Assert.False(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamIntegrationTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings(false);

            var discoverRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {GetTestSchema("select * from CRM")}
            };

            // act
            client.Connect(connectRequest);

            var discoverResponse = client.DiscoverSchemas(discoverRequest);

            var request = new ReadRequest()
            {
                Schema = discoverResponse.Schemas[0],
            };

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(426, records.Count);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new ReadRequest()
            {
                Schema = GetTestSchema("ReadStream")
            };

            // act
            client.Connect(connectRequest);
            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(2, records.Count);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamLimitTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new ReadRequest()
            {
                Schema = GetTestSchema("ReadStream"),
                Limit = 1
            };

            // act
            client.Connect(connectRequest);
            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task PrepareWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new PrepareWriteRequest
            {
                Schema = GetTestSchema("WriteStream"),
                CommitSlaSeconds = 1
            };

            // act
            client.Connect(connectRequest);
            var response = client.PrepareWrite(request);

            // assert
            Assert.IsType<PrepareWriteResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task WriteStreamTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var schema = GetTestSchema("WriteStream");
            schema.Properties.Add(new Property
            {
                Id = "TestCol",
                Name = "TestCol",
                Type = PropertyType.Integer
            });

            var prepareRequest = new PrepareWriteRequest()
            {
                Schema = schema,
                CommitSlaSeconds = 1
            };

            var records = GetTestRecords();

            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);
            client.PrepareWrite(prepareRequest);

            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }

                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Equal(2, recordAcks.Count);
            Assert.Equal("", recordAcks[0].Error);
            Assert.Equal("test", recordAcks[0].CorrelationId);
            Assert.Equal("", recordAcks[1].Error);
            Assert.Equal("more-test", recordAcks[1].CorrelationId);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConfigureWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginODBC.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var firstRequest = new ConfigureWriteRequest()
            {
                Form = new ConfigurationFormRequest
                {
                    DataJson = "",
                    StateJson = ""
                }
            };

            var secondRequest = new ConfigureWriteRequest()
            {
                Form = new ConfigurationFormRequest
                {
                    DataJson =
                        "{\"Query\":\"ConfigureWrite\",\"Parameters\":[{\"ParamName\":\"Name\",\"ParamType\":\"int\"}]}",
                    StateJson = ""
                }
            };

            // act
            client.Connect(connectRequest);
            var firstResponse = client.ConfigureWrite(firstRequest);
            var secondResponse = client.ConfigureWrite(secondRequest);

            // assert
            Assert.IsType<ConfigureWriteResponse>(firstResponse);
            Assert.NotNull(firstResponse.Form.SchemaJson);
            Assert.NotNull(firstResponse.Form.UiJson);
            Assert.Null(firstResponse.Schema);

            Assert.IsType<ConfigureWriteResponse>(secondResponse);
            Assert.NotNull(secondResponse.Form.SchemaJson);
            Assert.NotNull(secondResponse.Form.UiJson);
            Assert.NotNull(secondResponse.Schema);
            Assert.Equal("", secondResponse.Schema.Id);
            Assert.Equal("", secondResponse.Schema.Name);
            Assert.Equal("ConfigureWrite", secondResponse.Schema.Query);
            Assert.Equal(Schema.Types.DataFlowDirection.Write, secondResponse.Schema.DataFlowDirection);
            Assert.Single(secondResponse.Schema.Properties);

            var property = secondResponse.Schema.Properties[0];
            Assert.Equal("Name", property.Id);
            Assert.Equal("Name", property.Name);
            Assert.Equal(PropertyType.Integer, property.Type);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
    }
}