using Naveego.Sdk.Plugins;

namespace PluginODBC.API.Write
{
    public static partial class Write
    {
        /// <summary>
        /// Gets the property type for the provided write back type from form
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static PropertyType GetWriteBackType(string type)
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
    }
}