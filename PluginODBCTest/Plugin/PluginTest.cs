using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Moq;
using PluginODBC.Helper;
using PluginODBC.Interfaces;
using Pub;
using Xunit;
using Record = Pub.Record;

namespace PluginODBCTest.Plugin
{
    public class PluginTest
    {
        private readonly Mock<IConnectionService> _mockOdbcConnection = new Mock<IConnectionService>();
        
        private ConnectRequest GetConnectSettings()
        {
            return new ConnectRequest
            {
                SettingsJson =
                    "{\"ConnectionString\":\"test connection\",\"Password\":\"password\",\"PrePublishQuery\":\"\",\"PostPublishQuery\":\"\"}",
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            };
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
                                        
                                        var mockCol = new DataColumn
                                        {
                                            ColumnName = "TestCol",
                                            Caption = "Caption",
                                            Unique = true,
                                            AllowDBNull = false,
                                            DataType = Type.GetType("System.Int64")
                                        };
                                        mockSchemaTable.Columns.Add(mockCol);
                                        
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

                                var readToggle = true;
                                mockReader.Setup(r => r.Read())
                                    .Returns(() => readToggle)
                                    .Callback(() => readToggle = false);

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
                Query = query,
                Properties =
                {
                    new Property
                    {
                        Id = "TestCol"
                    }
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
    }
}