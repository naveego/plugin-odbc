using System.Data.Odbc;
using PluginODBC.Interfaces;

namespace PluginODBC.API
{
    public class CommandService : ICommandService
    {
        private readonly OdbcCommand _command;

        public OdbcParameterCollection Parameters => _command.Parameters;

        public CommandService(OdbcCommand command)
        {
            _command = command;
        }

        public IReaderService ExecuteReader()
        {
            return new ReaderService(_command.ExecuteReader());
        }

        public OdbcParameter AddParameter(string parameterName, OdbcType odbcType)
        {
            return _command.Parameters.Add(parameterName, odbcType);
        }

        public void Prepare()
        {
            _command.Prepare();
        }
    }
}