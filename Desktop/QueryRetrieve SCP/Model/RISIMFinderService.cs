// Copyright (c) 2012-2019 fo-dicom contributors.
// Licensed under the Microsoft Public License (MS-PL).

using Dicom;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using System.Linq;
using System.Data;
using System.Globalization;

namespace QueryRetrieve_SCP.Model
{
    public class RISIMFinderService : IDicomImageFinderService
    {
        const string ONE_EQ_ONE = " WHERE 1=1\n";
        public RISIMFinderService()
        {   //On instanciation get the connection string. Each instance
            //of the service  has it's own db. If that db does not exist 
            //create it now.

            string _connectionString = Config.GetConnectionStringByName(Config.Key_Node_DB);
            if (_connectionString == null)
            {
                Console.WriteLine("Error: Node_DB needs named connection in App.config");
            }
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();  //checked we can open the db
                //if not create the db for this Node
                connection.Close();
            }
        }

        #region IDicomImageFinderService implementations for FindPatientFiles,FindStudyFiles and FindSeriesFiles
        public DataTable FindPatientFiles(string PatientName, string PatientID)
        {
            int topLimit = Config.GetConfigIntValueByName(Config.Key_CFIND_PATIENT_LIMIT);
            string selectString = " SELECT ";
            if (topLimit >= 1) selectString += $" TOP ({topLimit})";
            selectString += " * FROM CFIND_PATIENT_VIEW \n";
            string whereString = ONE_EQ_ONE;

            DataTable dt = new DataTable();

            using (SqlConnection connection = new SqlConnection(Config.GetConnectionStringByName(Config.Key_Node_DB)))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (PatientName != "")
                    {
                        whereString += " AND PatientName LIKE '%' + @PatientName + '%' \n";
                        command.Parameters.AddWithValue("@PatientName", PatientName);
                    }
                    if (PatientID != "")
                    {
                        whereString += " AND PatientID=@PatientID \n";
                        command.Parameters.AddWithValue("@PatientID", PatientID);
                    }
                  
                    command.CommandText = selectString;
                    if (whereString != ONE_EQ_ONE) command.CommandText += whereString;

                    if (Config.GetConfigBoolValueByName(Config.Key_DEBUG_SHOW_SQL)) debugDisplaySQL(command);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        dt.Load(reader);
                    }
                    return dt;
                }
            }
        }

        public DataTable FindStudyFiles(string PatientName, string PatientID, string AccessionNbr, string StudyUID, string StudyDate, string StudyTime)
        {
            int topLimit = Config.GetConfigIntValueByName(Config.Key_CFIND_STUDY_LIMIT);
            string selectString = " SELECT ";
            if (topLimit >= 1) selectString += $" TOP ({topLimit})";
            selectString += " * FROM CFIND_STUDY_VIEW \n";
            string whereString = ONE_EQ_ONE;

            DataTable dt = new DataTable();

            using (SqlConnection connection = new SqlConnection(Config.GetConnectionStringByName(Config.Key_Node_DB)))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (PatientName != "") 
                    { 
                        whereString += " AND PatientName=@PatientName \n";
                        command.Parameters.AddWithValue("@PatientName", PatientName);
                    }
                    if (PatientID != "")
                    {
                        whereString += " AND PatientID=@PatientID \n";
                        command.Parameters.AddWithValue("@PatientID", PatientID);
                    }
                    if (AccessionNbr != "") {
                        whereString += " AND AccessionNumber=@AccessionNbr \n";
                        command.Parameters.AddWithValue("@AccessionNbr", AccessionNbr);
                    }
                    if (StudyUID != "") {
                        whereString += " AND StudyInstanceUID=@StudyUID \n";
                        command.Parameters.AddWithValue("@StudyUID", StudyUID);
                    }
                    //Currently we only accept date/time range if date is provided in a valid format
                    //remove -,space,:,/ we only accept YYYYMMDD or YYYYMMDDYYYYMMDD for date and HHMMSS or HHMMSSHHMMSS by removing known chars like spaces, slashes, hyphens we can also accept varients including these
                    StudyDate = StudyDate.Replace(" ", "").Replace("-", "").Replace("/", "");
                    StudyTime = StudyTime.Replace(":", "").Replace("-", "");
                    if (StudyDate != "")
                    {//a date value or range provided
                        DateTime startDateTime;
                        DateTime endDateTime;

                        if (StudyDate.Length == 8)
                        {//single date provided YYYYMMDD
                            whereString += " AND StudyDateTime BETWEEN @StartStudyDateTime AND @EndStudyDateTime \n";
                            if (StudyTime.Length==6)
                            {  //single time HHMMSS
                                    startDateTime = DateTime.ParseExact(StudyDate + StudyTime, "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                                    endDateTime = DateTime.ParseExact(StudyDate + StudyTime, "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                            }
                            else if (StudyTime.Length == 12)
                            {  //time range HHMMSSHHMMSS
                                    startDateTime = DateTime.ParseExact(StudyDate + StudyTime.Substring(0,6), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                                    endDateTime = DateTime.ParseExact(StudyDate + StudyTime.Substring(6, 6), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                            }
                            else //if (StudyTime == "" or StudyTime Invalid format ignore it)
                            {
                                    startDateTime = DateTime.ParseExact(StudyDate + "000000", "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                                    endDateTime = DateTime.ParseExact(StudyDate + "235959", "yyyyMMddHHmmss", CultureInfo.InvariantCulture);

                            }
                            command.Parameters.AddWithValue("@StartStudyDateTime", startDateTime);
                            command.Parameters.AddWithValue("@EndStudyDateTime", endDateTime);
                        }
                        else if ( StudyDate.Length == 16)
                        {//Assume Date Range YYYYMMDDYYYYMMDD 
                            whereString += " AND StudyDateTime BETWEEN @StartStudyDateTime AND @EndStudyDateTime \n";
                            if (StudyTime.Length == 6)
                            {  //single time HHMMSS
                                startDateTime = DateTime.ParseExact(StudyDate.Substring(0,8) + StudyTime, "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                                endDateTime = DateTime.ParseExact(StudyDate.Substring(8, 8) + StudyTime, "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                            }
                            else if (StudyTime.Length == 12)
                            {  //time range HHMMSSHHMMSS
                                startDateTime = DateTime.ParseExact(StudyDate.Substring(0, 8) + StudyTime.Substring(0, 6), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                                endDateTime = DateTime.ParseExact(StudyDate.Substring(8, 8) + StudyTime.Substring(6, 6), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                            }
                            else //if (StudyTime == "" or StudyTime Invalid format ignore it)
                            {
                                startDateTime = DateTime.ParseExact(StudyDate.Substring(0, 8) + "000000", "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                                endDateTime = DateTime.ParseExact(StudyDate.Substring(0, 8) + "235959", "yyyyMMddHHmmss", CultureInfo.InvariantCulture);

                            }
                            command.Parameters.AddWithValue("@StartStudyDateTime", startDateTime);
                            command.Parameters.AddWithValue("@EndStudyDateTime", endDateTime);
                        }
                        //else StudyDate == "" or invalid format ignore it
                    }

                    command.CommandText = selectString;
                    if (whereString != ONE_EQ_ONE) command.CommandText+= whereString;

                    if (Config.GetConfigBoolValueByName(Config.Key_DEBUG_SHOW_SQL)) debugDisplaySQL(command);
                
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        dt.Load(reader);
                    }
                    return dt;
                }
             
            }

        }


        public DataTable FindSeriesFiles(string PatientName, string PatientID, string AccessionNbr, string StudyUID, string SeriesUID, string Modality)
        {
            int topLimit = Config.GetConfigIntValueByName(Config.Key_CFIND_SERIES_LIMIT);
            string selectString = " SELECT ";
            if (topLimit >= 1) selectString += $" TOP ({topLimit})";
            selectString+= " * FROM CFIND_SERIES_VIEW \n"; 
            string orderBy = " ORDER BY StudyInstanceUID, SeriesNumber \n";
            string whereString = ONE_EQ_ONE;
            
            DataTable dt = new DataTable();

            using (SqlConnection connection = new SqlConnection(Config.GetConnectionStringByName(Config.Key_Node_DB)))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (PatientName != "")
                    {
                        whereString += " AND PatientName=@PatientName \n";
                        command.Parameters.AddWithValue("@PatientName", PatientName);
                    }
                    if (PatientID != "")
                    {
                        whereString += " AND PatientID=@PatientID \n";
                        command.Parameters.AddWithValue("@PatientID", PatientID);
                    }
                    if (AccessionNbr != "")
                    {
                        whereString += " AND AccessionNumber=@AccessionNbr \n";
                        command.Parameters.AddWithValue("@AccessionNbr", AccessionNbr);
                    }
                    if (StudyUID != "")
                    {
                        whereString += " AND StudyInstanceUID=@StudyUID \n";
                        command.Parameters.AddWithValue("@StudyUID", StudyUID);
                    }
                    if (SeriesUID != "")
                    {
                        whereString += " AND StudyInstanceUID=@SeriesUID \n";
                        command.Parameters.AddWithValue("@SeriesUID", SeriesUID);
                    }
                    if (Modality != "")
                    {
                        whereString += " AND StudyInstanceUID=@StudyUID \n";
                        command.Parameters.AddWithValue("@Modality", Modality);
                    }
                    command.CommandText = selectString;
                    if (whereString != ONE_EQ_ONE) command.CommandText += whereString;
                    command.CommandText += orderBy;

                    if (Config.GetConfigBoolValueByName(Config.Key_DEBUG_SHOW_SQL)) debugDisplaySQL(command);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        dt.Load(reader);
                    }
                    return dt;
                }

            }
        }
        #endregion

        #region private subs
        private void debugDisplaySQL(SqlCommand command)
        {//dump sql command text and params to console
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("DEBUG Mode SQL Generated:");
            Console.WriteLine(command.CommandText.ToString());
            Console.WriteLine("Parameter Values:");
            foreach (var param in command.Parameters)
            {//can't access the param.Value directly hence the strange missdirection
                Console.WriteLine(" " + param.ToString() + ':' + command.Parameters[param.ToString()].Value.ToString());
            }
            Console.ForegroundColor = ConsoleColor.White;
        }

        #endregion
        #region Code Stubs Currently  Out Of Scope - FindFilesByUID
        public DataTable FindFilesByUID(string PatientId, string StudyUID, string SeriesUID)
        {
            //NOT implemented - This is called by the CMOVE or CGET (ALSO not implemented) to obtain a physical DCM file
            //we do not need this functionality yet if ever for the test system. We do not want to actually have to store the files 
            //so this routine will need to create one on the fly. Probably grab dummy DCM file and set the DICOM tags to the matching data in the DB.

            return null;
        }
        #endregion

    }
}
