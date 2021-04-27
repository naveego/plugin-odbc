using System;
using System.Collections.Generic;
using System.Data;
using Google.Protobuf.Collections;
using Naveego.Sdk.Plugins;
using PluginODBC.API.Factory;

namespace PluginODBC.API.Discover
{
    public static partial class Discover
    {
        public static async IAsyncEnumerable<Schema> GetRefreshSchemas(IConnectionFactory connFactory,
            RepeatedField<Schema> refreshSchemas, int sampleSize = 5)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            foreach (var schema in refreshSchemas)
            {
                // if (string.IsNullOrWhiteSpace(schema.Query))
                // {
                //     yield return await GetRefreshSchemaForTable(connFactory, schema, sampleSize);
                //     continue;
                // }

                var cmd = connFactory.GetCommand(schema.Query, conn);

                var reader = await cmd.ExecuteReaderAsync();
                var schemaTable = reader.GetSchemaTable();

                var properties = new List<Property>();
                if (schemaTable != null)
                {
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
                            // Id = Utility.Utility.GetSafeName(colName, '"'),
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

                        // add property to properties
                        properties.Add(property);
                    }
                }

                // add only discovered properties to schema
                schema.Properties.Clear();
                schema.Properties.AddRange(properties);

                // get sample and count
                yield return await AddSampleAndCount(connFactory, schema, sampleSize);
            }

            await conn.CloseAsync();
        }
    }
}