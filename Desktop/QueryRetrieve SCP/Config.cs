using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryRetrieve_SCP
{
    static class Config
    {
        #region App.Config Keys
        public const string Key_Enable_DICOM_AutoValidate = "Enable_DICOM_AutoValidate";
        public const string Key_Node_DB = "Node_DB";
        public const string Key_CFIND_STUDY_LIMIT = "CFIND_STUDY_LIMIT";
        public const string Key_CFIND_SERIES_LIMIT = "CFIND_SERIES_LIMIT";
        public const string Key_CFIND_PATIENT_LIMIT = "CFIND_PATIENT_LIMIT";
        public const string Key_DEBUG_SHOW_SQL = "DEBUG_SHOW_SQL";
        #endregion

        #region App.Config ApplicationSettings
        //todo move to global static class all the settings routines
        public static int GetConfigIntValueByName(string name)
        {
            string configVal = GetConfigStringValueByName(name);
            int rtn = 0;
            Int32.TryParse(configVal, out rtn);
            return rtn;
        }
        public static bool GetConfigBoolValueByName(string name)
        {
            string configVal = GetConfigStringValueByName(name);
            bool rtn = false;
            bool.TryParse(configVal, out rtn);
            return rtn;
        }
        public static string GetConfigStringValueByName(string name)
        {
            // Assume failure.
            string returnValue = "";

            // Look for the name in the connectionStrings section.
            var settings = ConfigurationManager.AppSettings;

            // If found, return the connection string.
            if (settings[name] != null)
            {
                returnValue = settings[name];
            }
            else
            {
                throw new Exception($"Unable to find AppSettings configuration value:{name}");
            }

            return returnValue;
        }
        #endregion

        #region App.Config ConnectionStrings
        public static string GetConnectionStringByName(string name)
        {
            // Assume failure.
            string returnValue = null;

            // Look for the name in the connectionStrings section.
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings[name];

            // If found, return the connection string.
            if (settings != null)
            {
                returnValue = settings.ConnectionString;
            }
            else
            {
                throw new Exception($"Unable to find configuration value:{name}");
            }

            return returnValue;
        }
        #endregion
    }
}
