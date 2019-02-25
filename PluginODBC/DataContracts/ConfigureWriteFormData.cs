namespace PluginODBC.DataContracts
{
    public class ConfigureWriteFormData
    {
        public string Query { get; set; }
        public Parameter[] Parameters { get; set; }
    }

    public class Parameter
    {
        public string ParamName { get; set; }
        public string ParamType { get; set; }
    }
}