using System.Collections.Generic;
using System.Threading.Tasks;
using PluginODBC.API.Factory;
using PluginODBC.DataContracts;

namespace PluginODBC.API.Write
{
    public static partial class Write
    {
        private const string SchemaName = "ROUTINESCHEMA";
        private const string RoutineName = "ROUTINENAME";
        private const string SpecificName = "SPECIFICNAME";

        private static string GetAllStoredProceduresQuery = @"
select ""ROUTINESCHEMA"", ""ROUTINENAME"", ""SPECIFICNAME""
        from ""SYSCAT"".""ROUTINES""
        where ""ROUTINETYPE"" = 'P'
        and ""ROUTINESCHEMA"" not like 'SYS%'
        and ""OWNER"" not like 'SYS%'";
        
        public static async Task<List<WriteStoredProcedure>> GetAllStoredProceduresAsync(IConnectionFactory connFactory)
        {
            var storedProcedures = new List<WriteStoredProcedure>();
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(GetAllStoredProceduresQuery, conn);
            var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var storedProcedure = new WriteStoredProcedure
                {
                    SchemaName = reader.GetValueById(SchemaName).ToString(),
                    RoutineName = reader.GetValueById(RoutineName).ToString(),
                    SpecificName = reader.GetValueById(SpecificName).ToString()
                };
                
                storedProcedures.Add(storedProcedure);
            }
            
            await conn.CloseAsync();

            return storedProcedures;
        }
    }
}