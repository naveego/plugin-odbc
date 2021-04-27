using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginODBC.API.Factory;
using PluginODBC.DataContracts;

namespace PluginODBC.API.Write
{
    public static partial class Write
    {
        private static string ParamName = "PARMNAME";
        private static string DataType = "TYPENAME";

        private static string GetStoredProcedureParamsQuery = @"
select ""PARMNAME"", ""TYPENAME"", ""ORDINAL""
from ""SYSCAT"".""ROUTINES"" proc
left join ""SYSCAT"".""ROUTINEPARMS"" param
          on proc.""ROUTINESCHEMA"" = param.""ROUTINESCHEMA""
          and proc.SPECIFICNAME = param.SPECIFICNAME
where proc.""ROUTINESCHEMA"" = '{0}'
        and proc.""SPECIFICNAME"" = '{1}'
        order by ""ORDINAL"" ASC";

        public static async Task<Schema> GetSchemaForStoredProcedureAsync(IConnectionFactory connFactory,
            WriteStoredProcedure storedProcedure)
        {
            var schema = new Schema
            {
                Id = storedProcedure.GetId(),
                Name = storedProcedure.GetId(),
                Description = "",
                DataFlowDirection = Schema.Types.DataFlowDirection.Write,
                Query = storedProcedure.GetId()
            };

            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(
                string.Format(GetStoredProcedureParamsQuery, storedProcedure.SchemaName, storedProcedure.SpecificName),
                conn);
            var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var property = new Property
                {
                    Id = reader.GetValueById(ParamName).ToString(),
                    Name = reader.GetValueById(ParamName).ToString(),
                    Description = "",
                    Type = Discover.Discover.GetType(reader.GetValueById(DataType).ToString()),
                    TypeAtSource = reader.GetValueById(DataType).ToString()
                };

                schema.Properties.Add(property);
            }

            await conn.CloseAsync();

            return schema;
        }
    }
}