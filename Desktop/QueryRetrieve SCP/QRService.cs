﻿using Dicom.Network;
using System;
using System.Collections.Generic;
using System.Text;
using Dicom.Log;
using Dicom;
using System.Reflection;
using System.Net;
using System.Net.Sockets;
using QueryRetrieve_SCP.Model;
using System.Threading;
using System.Threading.Tasks;

using DicomClient = Dicom.Network.Client.DicomClient;
using System.Data;


namespace QueryRetrieve_SCP
{

    public class QRService : DicomService, IDicomServiceProvider, IDicomCFindProvider, IDicomCEchoProvider, IDicomCMoveProvider, IDicomCGetProvider
    {


        private static readonly DicomTransferSyntax[] AcceptedTransferSyntaxes = new DicomTransferSyntax[]
            {
                DicomTransferSyntax.ExplicitVRLittleEndian,
                DicomTransferSyntax.ExplicitVRBigEndian,
                DicomTransferSyntax.ImplicitVRLittleEndian
            };


        private static readonly DicomTransferSyntax[] AcceptedImageTransferSyntaxes = new DicomTransferSyntax[]
            {
                // Lossless
                DicomTransferSyntax.JPEGLSLossless,
                DicomTransferSyntax.JPEG2000Lossless,
                DicomTransferSyntax.JPEGProcess14SV1,
                DicomTransferSyntax.JPEGProcess14,
                DicomTransferSyntax.RLELossless,

                // Lossy
                DicomTransferSyntax.JPEGLSNearLossless,
                DicomTransferSyntax.JPEG2000Lossy,
                DicomTransferSyntax.JPEGProcess1,
                DicomTransferSyntax.JPEGProcess2_4,

                // Uncompressed
                DicomTransferSyntax.ExplicitVRLittleEndian,
                DicomTransferSyntax.ExplicitVRBigEndian,
                DicomTransferSyntax.ImplicitVRLittleEndian
            };


        public string CallingAE { get; protected set; }
        public string CalledAE { get; protected set; }
        public IPAddress RemoteIP { get; private set; }


 
        public QRService(INetworkStream stream, Encoding fallbackEncoding, Logger log) : base(stream, fallbackEncoding, log)
        {
            var pi = stream.GetType().GetProperty("Socket", BindingFlags.NonPublic | BindingFlags.Instance);
            if (pi != null)
            {
                var endPoint = ((Socket)pi.GetValue(stream, null)).RemoteEndPoint as IPEndPoint;
                RemoteIP = endPoint.Address;
            }
            else
            {
                RemoteIP = new IPAddress(new byte[] { 127, 0, 0, 1 });
            }
         }


        public DicomCEchoResponse OnCEchoRequest(DicomCEchoRequest request)
        {
            Logger.Info($"Received verification request from AE {CallingAE} with IP: {RemoteIP}");
            return new DicomCEchoResponse(request, DicomStatus.Success);
        }


        public void OnConnectionClosed(Exception exception)
        {
            Clean();
        }


        public void OnReceiveAbort(DicomAbortSource source, DicomAbortReason reason)
        {
            //log the abort reason
            Logger.Error($"Received abort from {source}, reason is {reason}");
        }


        public Task OnReceiveAssociationReleaseRequestAsync()
        {
            Clean();
            return SendAssociationReleaseResponseAsync();
        }


        public Task OnReceiveAssociationRequestAsync(DicomAssociation association)
        {
            CallingAE = association.CallingAE;
            CalledAE = association.CalledAE;

            Logger.Info($"Received association request from AE: {CallingAE} with IP: {RemoteIP} ");

            if (QRServer.AETitle != CalledAE)
            {
                Logger.Error($"Association with {CallingAE} rejected since called aet {CalledAE} is unknown");
                return SendAssociationRejectAsync(DicomRejectResult.Permanent, DicomRejectSource.ServiceUser, DicomRejectReason.CalledAENotRecognized);
            }

            foreach (var pc in association.PresentationContexts)
            {
                if (pc.AbstractSyntax == DicomUID.Verification
                    || pc.AbstractSyntax == DicomUID.PatientRootQueryRetrieveInformationModelFIND
                    || pc.AbstractSyntax == DicomUID.PatientRootQueryRetrieveInformationModelMOVE
                    || pc.AbstractSyntax == DicomUID.StudyRootQueryRetrieveInformationModelFIND
                    || pc.AbstractSyntax == DicomUID.StudyRootQueryRetrieveInformationModelMOVE)
                {
                    pc.AcceptTransferSyntaxes(AcceptedTransferSyntaxes);
                }
                else if (pc.AbstractSyntax == DicomUID.PatientRootQueryRetrieveInformationModelGET
                    || pc.AbstractSyntax == DicomUID.StudyRootQueryRetrieveInformationModelGET)
                {
                    pc.AcceptTransferSyntaxes(AcceptedImageTransferSyntaxes);
                }
                else if (pc.AbstractSyntax.StorageCategory != DicomStorageCategory.None)
                {
                    pc.AcceptTransferSyntaxes(AcceptedImageTransferSyntaxes);
                }
                else
                {
                    Logger.Warn($"Requested abstract syntax {pc.AbstractSyntax} from {CallingAE} not supported");
                    pc.SetResult(DicomPresentationContextResult.RejectAbstractSyntaxNotSupported);
                }
            }

            Logger.Info($"Accepted association request from {CallingAE}");
            return SendAssociationAcceptAsync(association);
        }


        public IEnumerable<DicomCFindResponse> OnCFindRequest(DicomCFindRequest request)
        {
            var queryLevel = request.Level;

            DataTable resultsDt =null;
            IDicomImageFinderService finderService = QRServer.CreateFinderService;

            // a QR SCP has to define in a DICOM Conformance Statement for which dicom tags it can query
            // depending on the level of the query. Below there are only very few parameters evaluated.

            switch (queryLevel)
            {
                case DicomQueryRetrieveLevel.Patient:
                    {
                        var patname = request.Dataset.GetSingleValueOrDefault(DicomTag.PatientName, string.Empty);
                        var patid = request.Dataset.GetSingleValueOrDefault(DicomTag.PatientID, string.Empty);

                         resultsDt = finderService.FindPatientFiles(patname, patid);
                    }
                    break;

                case DicomQueryRetrieveLevel.Study:
                    {
                        var patname = request.Dataset.GetSingleValueOrDefault(DicomTag.PatientName, string.Empty);
                        var patid = request.Dataset.GetSingleValueOrDefault(DicomTag.PatientID, string.Empty);
                        var accNr = request.Dataset.GetSingleValueOrDefault(DicomTag.AccessionNumber, string.Empty);
                        var studyUID = request.Dataset.GetSingleValueOrDefault(DicomTag.StudyInstanceUID, string.Empty);
                        var StudyDate = request.Dataset.GetSingleValueOrDefault(DicomTag.StudyDate, string.Empty);
                        var StudyTime = request.Dataset.GetSingleValueOrDefault(DicomTag.StudyTime, string.Empty);

                        resultsDt = finderService.FindStudyFiles(patname, patid, accNr, studyUID, StudyDate, StudyTime);
                      
                    }
                    break;

                case DicomQueryRetrieveLevel.Series:
                    {
                        var patname = request.Dataset.GetSingleValueOrDefault(DicomTag.PatientName, string.Empty);
                        var patid = request.Dataset.GetSingleValueOrDefault(DicomTag.PatientID, string.Empty);
                        var accNr = request.Dataset.GetSingleValueOrDefault(DicomTag.AccessionNumber, string.Empty);
                        var studyUID = request.Dataset.GetSingleValueOrDefault(DicomTag.StudyInstanceUID, string.Empty);
                        var seriesUID = request.Dataset.GetSingleValueOrDefault(DicomTag.SeriesInstanceUID, string.Empty);
                        var modality = request.Dataset.GetSingleValueOrDefault(DicomTag.Modality, string.Empty);

                        resultsDt = finderService.FindSeriesFiles(patname, patid, accNr, studyUID, seriesUID, modality);
                    }
                    break;

                case DicomQueryRetrieveLevel.Image:
                    yield return new DicomCFindResponse(request, DicomStatus.QueryRetrieveUnableToProcess);
                    yield break;
            }
                 
            bool _autoValidateDICOM = Config.GetConfigBoolValueByName(Config.Key_Enable_DICOM_AutoValidate);
            var columns = new List<string>();
             foreach (DataColumn column in resultsDt.Columns)
            {
                columns.Add(column.ColumnName);
            }
            // now read the required dicomtags from the matching files and return as results
            foreach (DataRow row in resultsDt.Rows)
            {
                var rtnResult = new DicomDataset();
                rtnResult.AutoValidate = _autoValidateDICOM; //Not sure if we need this so se up as config value
                //rtnResult.Clear(); //Could clear existing rather than create new.
                foreach (var requestedTag in request.Dataset)
                {
                    // most of the requested DICOM tags are stored in the DICOM files and therefore saved into a database.
                    // you can fill the responses by selecting the values from the database.
                    // also be aware that there are some requested DicomTags like "ModalitiesInStudy" or "NumberOfStudyRelatedInstances" 
                    // or "NumberOfPatientRelatedInstances" and so on which have to be calculated and cannot be read from a DICOM file.
                    string columnName = requestedTag.Tag.DictionaryEntry.Keyword.ToString();
                    if (columns.Contains(columnName))
                    {
                       
                        rtnResult.Add(requestedTag.Tag, row[columnName].ToString());
                    }
                    else
                    {
                        rtnResult.Add(requestedTag);
                    }
                }
                yield return new DicomCFindResponse(request, DicomStatus.Pending) { Dataset = rtnResult };
            }
            resultsDt.Dispose();
            yield return new DicomCFindResponse(request, DicomStatus.Success);
        }


        public void Clean()
        {
            // cleanup, like cancel outstanding move- or get-jobs
        }

        # region Code Stubs Currently  Out Of Scope for OnCMoveRequest and OnCGetRequest
        public IEnumerable<DicomCMoveResponse> OnCMoveRequest(DicomCMoveRequest request)
        {
//sca currently not implemented so fail
            yield return new DicomCMoveResponse(request, DicomStatus.NoSuchActionType);
            yield return new DicomCMoveResponse(request, DicomStatus.ProcessingFailure);

/*
            // the c-move request contains the DestinationAE. the data of this AE should be configured somewhere.
            if (request.DestinationAE != "STORESCP")
            {
                yield return new DicomCMoveResponse(request, DicomStatus.QueryRetrieveMoveDestinationUnknown);
                yield return new DicomCMoveResponse(request, DicomStatus.ProcessingFailure);
                yield break;
            }

            // this data should come from some data storage!
            var destinationPort = 11112;
            var destinationIP = "localhost";

            IDicomImageFinderService finderService = QRServer.CreateFinderService;
            IEnumerable<string> matchingFiles = Enumerable.Empty<string>();

            switch (request.Level)
            {
                case DicomQueryRetrieveLevel.Patient:
                    matchingFiles = finderService.FindFilesByUID(request.Dataset.GetSingleValue<string>(DicomTag.PatientID), string.Empty, string.Empty);
                    break;

                case DicomQueryRetrieveLevel.Study:
                    matchingFiles = finderService.FindFilesByUID(string.Empty, request.Dataset.GetSingleValue<string>(DicomTag.StudyInstanceUID), string.Empty);
                    break;

                case DicomQueryRetrieveLevel.Series:
                    matchingFiles = finderService.FindFilesByUID(string.Empty, string.Empty, request.Dataset.GetSingleValue<string>(DicomTag.SeriesInstanceUID));
                    break;

                case DicomQueryRetrieveLevel.Image:
                    yield return new DicomCMoveResponse(request, DicomStatus.QueryRetrieveUnableToPerformSuboperations);
                    yield break;
            }

            var client = new DicomClient(destinationIP, destinationPort, false, QRServer.AETitle, request.DestinationAE);
            client.NegotiateAsyncOps();
            int storeTotal = matchingFiles.Count();
            int storeDone = 0; // this variable stores the number of instances that have already been sent
            int storeFailure = 0; // this variable stores the number of faulues returned in a OnResponseReceived
            foreach (string file in matchingFiles)
            {
                var storeRequest = new DicomCStoreRequest(file);
                // !!! there is a Bug in fo-dicom 3.0.2 that the OnResponseReceived handlers are invoked not until the DicomClient has already
                //     sent all the instances. So the counters are not increased image by image sent but only once in a bulk after all storage
                //     has been finished. This bug will be fixed hopefully soon.
                storeRequest.OnResponseReceived += (req, resp) =>
                {
                    if (resp.Status == DicomStatus.Success)
                    {
                        Logger.Info("Storage of image successfull");
                        storeDone++;
                    }
                    else
                    {
                        Logger.Error("Storage of image failed");
                        storeFailure++;
                    }
                };
                client.AddRequestAsync(storeRequest).Wait();
            }

            var sendTask = client.SendAsync();

            while (!sendTask.IsCompleted)
            {
                // while the send-task is runnin we inform the QR SCU every 2 seconds about the status and how many instances are remaining to send. 
                yield return new DicomCMoveResponse(request, DicomStatus.Pending) { Remaining = storeTotal - storeDone - storeFailure, Completed = storeDone };
                Thread.Sleep(TimeSpan.FromSeconds(2));
            }

            Logger.Info("..finished");
            yield return new DicomCMoveResponse(request, DicomStatus.Success);
            */
        }

  
        public IEnumerable<DicomCGetResponse> OnCGetRequest(DicomCGetRequest request)
        {
            //sca currently not implemented so fail
            yield return new DicomCGetResponse(request, DicomStatus.NoSuchActionType);
            yield return new DicomCGetResponse(request, DicomStatus.ProcessingFailure);

/*            IDicomImageFinderService finderService = QRServer.CreateFinderService;
            IEnumerable<string> matchingFiles = Enumerable.Empty<string>();

            switch (request.Level)
            {
                case DicomQueryRetrieveLevel.Patient:
                    matchingFiles = finderService.FindFilesByUID(request.Dataset.GetSingleValue<string>(DicomTag.PatientID), string.Empty, string.Empty);
                    break;

                case DicomQueryRetrieveLevel.Study:
                    matchingFiles = finderService.FindFilesByUID(string.Empty, request.Dataset.GetSingleValue<string>(DicomTag.StudyInstanceUID), string.Empty);
                    break;

                case DicomQueryRetrieveLevel.Series:
                    matchingFiles = finderService.FindFilesByUID(string.Empty, string.Empty, request.Dataset.GetSingleValue<string>(DicomTag.SeriesInstanceUID));
                    break;

                case DicomQueryRetrieveLevel.Image:
                    yield return new DicomCGetResponse(request, DicomStatus.QueryRetrieveUnableToPerformSuboperations);
                    yield break;
            }

            foreach (var matchingFile in matchingFiles)
            {
                var storeRequest = new DicomCStoreRequest(matchingFile);
                SendRequestAsync(storeRequest).Wait();
            }

            yield return new DicomCGetResponse(request, DicomStatus.Success);
            */
        }

        #endregion

    }
}
