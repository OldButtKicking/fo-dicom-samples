﻿using System.Collections.Generic;
using System.Data;

namespace QueryRetrieve_SCP.Model
{

    public interface IDicomImageFinderService
    {

        /// <summary>
        /// Searches in a DICOM store for patient information. Returns a representative DICOM file per found patient
        /// </summary>
        DataTable FindPatientFiles(string PatientName, string PatientId);

        /// <summary>
        /// Searches in a DICOM store for study information. Returns a representative DICOM file per found study
        /// </summary>
        DataTable FindStudyFiles(string PatientName, string PatientId, string AccessionNbr, string StudyUID, string StudyDate, string StudyTime);

        /// <summary>
        /// Searches in a DICOM store for series information. Returns a representative DICOM file per found serie
        /// </summary>
        DataTable FindSeriesFiles(string PatientName, string PatientId, string AccessionNbr, string StudyUID, string SeriesUID, string Modality);

        /// <summary>
        /// Searches in a DICOM store for all files matching the given UIDs
        /// </summary>
        DataTable FindFilesByUID(string PatientId, string StudyUID, string SeriesUID);

    }
}
