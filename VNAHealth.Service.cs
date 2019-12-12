using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Data.Entity;

using NLog;

using Dicom;
using Dicom.Network;
using VNAHealth.Model;

namespace VNAHealth.Service
{

    public enum QueuePriority
    {
        Urgent = -1,
        Medium = 0,
        High = 1,
        Low = 2,
        Recheck = 3
    }

    class DicomClientScheduler : IManagedService
    {
        private volatile bool _running;
        private volatile bool _processing;
        private volatile bool _queueingCFinds;
        private object _lock = new object();
        private object _dataLock = new object();
        private DateTime _lastConfig = DateTime.MinValue;
        private DicomClientPoolManager _manager;

        public string Name
        {
            get { return "DICOMClientScheduler"; }
        }

        public bool IsRunning
        {
            get { return _running; }
        }

        public void Start(params string[] args)
        {

            _running = true;

            _manager = new DicomClientPoolManager();

            LogManager.GetLogger(this.Name).Info("Start");


            // Update the node configuration settings.
            OnUpdateNodeConfig(null, null);

            double scheduleJobsInterval = ConfigurationProperty.GetDouble("Application", "Schedule-Jobs-Interval", 120);
            Scheduler.AddJob(Name, "ScheduleJobs", OnScheduleJobs, TimeSpan.FromSeconds(scheduleJobsInterval));

            using (var db = new VHModelContainer(true))
            {
                // Reset all requests that were started but were never completed.
                foreach (var find in db.vh_cfind_request.AsNoTracking().Where(x => x.is_completed == -1).ToArray())
                {
                    find.is_completed = 0;
                    db.vh_cfind_request.Attach(find);
                    db.Entry(find).State = EntityState.Modified;
                }

                db.SaveChanges();
            }

        }

        public void Stop()
        {
            Scheduler.CancelGroup(Name);

            _manager.Dispose();
            _manager = null;

            _running = false;
        }

        private bool validateDateWindow(DateTime inventoryStart, DateTime start, DateTime end)
        {
            // Truncate the date window if it goes beyond the inventory start date.
            if (start < inventoryStart)
                start = inventoryStart;

            // If the end goes out of the programmed date range then the date window is invalid.
            if (end <= inventoryStart)
                return false;

            // The date window is valid.
            return true;

        }

        // Update the nodes in the dicom pool.
        private void OnUpdateNodeConfig(object sender, ScheduledEventArgs ea)
        {
            try
            {
                LogManager.GetLogger(this.Name).Info("OnUpdateNodeConfig");

                using (var db = new VHModelContainer(true))
                {
                    // Get a list of enabled nodes.
                    var nodes = db.vh_node.AsNoTracking().Where(i => i.node_status == vh_node.Enabled && i.deleted_on.HasValue == false).OrderBy(x => x.updated_on).ToList();

                    // If a node no longer exists, remove it from the dicom pool.
                    foreach (var key in _manager.GetConnectionKeys())
                    {
                        if (!nodes.Exists(x => x.vh_node_id == int.Parse(key)))
                            _manager.RemoveConnection(key);
                    }

                    // Add or update a connection in the dicom pool for any node whose configuration has changed.
                    foreach (var node in nodes.Where(x => x.updated_on > _lastConfig).ToList())
                    {
                        _manager.AddConnection(node.vh_node_id.ToString(), new DicomConnectionString(node.dicom_connection_string));
                        _lastConfig = node.updated_on;
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger(this.Name).Error(ex);
            }
            finally
            {
            }
        }


        private void OnScheduleJobs(object sender, ScheduledEventArgs ea)
        {
            try
            {
                LogManager.GetLogger(this.Name).Info("OnScheduleJobs");

                using (VHModelContainer db = new VHModelContainer(true))
                {

                    // UpdateNodeConfig
                    string processName = "UpdateNodeConfig";
                    double dbInterval = ConfigurationProperty.GetDouble("Dicom", "Update-Node-Config-Interval", 30);
                    double jobInterval = Scheduler.GetInterval(processName, "DICOMClientScheduler");

                    if (dbInterval != jobInterval)
                    {
                        Scheduler.CancelJob(Name, processName);
                        Scheduler.AddJob(Name, processName, OnUpdateNodeConfig, TimeSpan.FromSeconds(dbInterval));
                        LogManager.GetLogger(this.Name).Info("Scheduling " + processName);
                    }

                    // QueueCFind
                    processName = "QueueCFind";
                    string dbCronExpression = ConfigurationProperty.GetString("Dicom", "Queue-CFind-Interval");

                    string jobCronExpression = Scheduler.GetCronExpression(processName, "DICOMClientScheduler");

                    if (dbCronExpression != jobCronExpression)
                    {
                        Scheduler.CancelJob(Name, processName);
                        Scheduler.AddJob(Name, processName, OnQueueCFind, dbCronExpression);
                        LogManager.GetLogger(this.Name).Info("Scheduling " + processName);
                    }

                    // ProcessCFindCMove
                    processName = "ProcessCFindCMove";
                    dbInterval = ConfigurationProperty.GetDouble("Dicom", "Process-CFind-CMove-Interval", 3);
                    jobInterval = Scheduler.GetInterval(processName, "DICOMClientScheduler");

                    if (dbInterval != jobInterval)
                    {
                        Scheduler.CancelJob(Name, processName);
                        Scheduler.AddJob(Name, processName, OnProcessCFindCMove, TimeSpan.FromSeconds(dbInterval));
                        LogManager.GetLogger(this.Name).Info("Scheduling " + processName);
                    }

                    // QueryStudyRefresh
                    processName = "QueueStudyRefresh";
                    dbCronExpression = ConfigurationProperty.GetString("Dicom", "Queue-Study-Refresh-Interval");
                    jobCronExpression = Scheduler.GetCronExpression(processName, "DICOMClientScheduler");

                    if (dbCronExpression != jobCronExpression)
                    {
                        Scheduler.CancelJob(Name, processName);
                        Scheduler.AddJob(Name, processName, OnQueueStudyRefresh, dbCronExpression);
                        LogManager.GetLogger(this.Name).Info("Scheduling " + processName);
                    }

                    // ExecuteValidation
                    processName = "ExecuteValidation";
                    dbCronExpression = ConfigurationProperty.GetString("Validation", "Execute-Validation");
                    jobCronExpression = Scheduler.GetCronExpression(processName, "DICOMClientScheduler");

                    if (dbCronExpression != jobCronExpression)
                    {
                        Scheduler.CancelJob(Name, processName);
                        Scheduler.AddJob(Name, processName, OnExecuteValidation, dbCronExpression);
                        LogManager.GetLogger(this.Name).Info("Scheduling " + processName);
                    }

                    // ExecuteValidationLevelUpdate
                    processName = "ExecuteValidationLevelUpdate";
                    dbCronExpression = ConfigurationProperty.GetString("Validation", "Execute-Validation-Level-Update");
                    jobCronExpression = Scheduler.GetCronExpression(processName, "DICOMClientScheduler");

                    if (dbCronExpression != jobCronExpression)
                    {
                        Scheduler.CancelJob(Name, processName);
                        Scheduler.AddJob(Name, processName, OnExecuteValidationLevelUpdate, dbCronExpression);
                        LogManager.GetLogger(this.Name).Info("Scheduling " + processName);
                    }

                    // ExecuteDashboardRefresh
                    processName = "ExecuteDashboardRefresh";
                    dbCronExpression = ConfigurationProperty.GetString("Dashboard", "Execute-Dashboard-Refresh");
                    jobCronExpression = Scheduler.GetCronExpression(processName, "DICOMClientScheduler");

                    if (dbCronExpression != jobCronExpression)
                    {
                        Scheduler.CancelJob(Name, processName);
                        Scheduler.AddJob(Name, processName, OnExecuteDashboardRefresh, dbCronExpression);
                        LogManager.GetLogger(this.Name).Info("Scheduling " + processName);
                    }

                    // QueueCFindQueueHistory
                    processName = "QueueCFindQueueHistory";
                    dbCronExpression = ConfigurationProperty.GetString("Dicom", "Queue-CFind-Queue-History-Interval");
                    jobCronExpression = Scheduler.GetCronExpression(processName, "DICOMClientScheduler");

                    if (dbCronExpression != jobCronExpression)
                    {
                        Scheduler.CancelJob(Name, processName);
                        Scheduler.AddJob(Name, processName, OnQueueCFindQueueHistory, dbCronExpression);
                        LogManager.GetLogger(this.Name).Info("Scheduling " + processName);
                    }

                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger(this.Name).Error(ex);
            }
            finally
            {
            }
        }



        // Creates new CFind requests.
        private void OnQueueCFind(object sender, ScheduledEventArgs ea)
        {
            lock (_lock)
            {
                // Prevent scheduling CFinds from being re-entrant.
                if (_queueingCFinds)
                    return;
                _queueingCFinds = true;
            }

            try
            {
                using (var db = new VHModelContainer(true))
                {
                    // Process every node that is enabled.
                    foreach (vh_node node in db.vh_node.Where(i => i.node_status == vh_node.Enabled && i.deleted_on.HasValue == false).ToArray())
                    {
                        LogManager.GetLogger(this.Name).Info(string.Format("OnQueueCFind {0}", node.name));

                        // Look to see if there is a CFind date range for this node.
                        // Remove this line.
                        //vh_cfind_request request = db.vh_cfind_request.Where( i => i.vh_node_id == node.vh_node_id && i.level == (int)DicomQueryRetrieveLevel.Study && i.uid == null ).OrderBy( i => i.start_date ).FirstOrDefault();

                        if (node.cfind_start_date == null && node.cfind_end_date == null)
                        //if ( request == null )
                        {
                            // This is a new node with no scheduled CFinds, so prime the pump.  Determine the first date range window.
                            DateTime end = DateTime.Now.AddMinutes(-(double)node.latency_in_minutes).RoundUp(new TimeSpan(0, node.chunk_size_in_minutes, 0));
                            DateTime start = end.AddMinutes(-node.chunk_size_in_minutes);

                            node.cfind_start_date = start;
                            node.cfind_end_date = end;

                            // Create <cfind_limit> new CFind requests.
                            for (int count = 0; count < node.cfind_limit; count++)
                            {

                                // If the end goes out of the programmed date range then stop trying to create new requests.
                                if (end <= node.inventory_start)
                                    break;

                                // Do not let the start date go out of the programmed date range.
                                if (start < node.inventory_start)
                                    start = node.inventory_start;

                                vh_cfind_request request = new vh_cfind_request();
                                request.vh_node_id = node.vh_node_id;
                                request.created_on = DateTime.Now;
                                request.updated_on = DateTime.Now;
                                request.scheduled_on = DateTime.Now;
                                request.start_date = start;
                                request.end_date = end;
                                request.priority = (int)QueuePriority.High;
                                request.level = (int)DicomQueryRetrieveLevel.Study;
                                request.is_completed = 0;
                                request.response_count = 0;

                                db.vh_cfind_request.Add(request);

                                // Update the current date range window.
                                if (start < node.cfind_start_date)         // when a configuration has changed.
                                    node.cfind_start_date = start;
                                if (end > node.cfind_end_date)
                                    node.cfind_end_date = end;

                                // Determine the next date range window.
                                end = start;
                                start = start.AddMinutes(-node.chunk_size_in_minutes);


                            }

                            db.vh_node.Attach(node);
                            db.Entry(node).State = EntityState.Modified;

                            // Save changes to the database.
                            db.SaveChanges();
                        }
                        else
                        {

                            // Retry all of the requests that never received a response.
                            DateTime updatedTime = DateTime.Now.AddMinutes(-10);
                            foreach (var find in db.vh_cfind_request.AsNoTracking().Where(x => x.is_completed == -1 && x.updated_on < updatedTime).ToArray())
                            {
                                find.is_completed = 0;
                                db.vh_cfind_request.Attach(find);
                                db.Entry(find).State = EntityState.Modified;
                            }

                            db.SaveChanges();

                            // Determine the number of study level CFind requests in the queue for this node.                                                      
                            int queued = db.vh_cfind_request.Where(i => i.vh_node_id == node.vh_node_id && i.level == (int)DicomQueryRetrieveLevel.Study && i.is_completed == 0).Count();

                            //
                            // Go back in time to inventory old studies.
                            //

                            DateTime end = (DateTime)node.cfind_start_date;
                            DateTime start = end.AddMinutes(-node.chunk_size_in_minutes);

                            // Add requests until there are <study_queue_count> requests in the queue.  If the queue is full, then do not add any more.
                            while (queued < node.study_queue_count) // Need another way to break this loop ????
                            {
                                // If the end goes out of the programmed date range then stop trying to create new requests.
                                if (end <= node.inventory_start)
                                    break;

                                // Do not let the start date go out of the programmed date range.
                                if (start < node.inventory_start)
                                    start = node.inventory_start;

                                // Add a new request.
                                vh_cfind_request rq = new vh_cfind_request();
                                rq.vh_node_id = node.vh_node_id;
                                rq.created_on = DateTime.Now;
                                rq.updated_on = DateTime.Now;
                                rq.scheduled_on = DateTime.Now;
                                rq.start_date = start;
                                rq.end_date = end;
                                rq.priority = (int)QueuePriority.Low;
                                rq.level = (int)DicomQueryRetrieveLevel.Study;
                                rq.is_completed = 0;
                                rq.response_count = 0;

                                db.vh_cfind_request.Add(rq);

                                // Update the current CFind date range window.
                                if (start < node.cfind_start_date)
                                    node.cfind_start_date = start;
                                if (end > node.cfind_end_date)
                                    node.cfind_end_date = end;

                                // Get the next date window.
                                end = start;
                                start = start.AddMinutes(-node.chunk_size_in_minutes);

                                // Add to the requests in the queue count.
                                queued++;
                            }

                            //
                            // Create requests moving forward.
                            //

                            // Find the request with the newest date window.
                            //                           request = db.vh_cfind_request.Where( i => i.vh_node_id == node.vh_node_id && i.level == (int)DicomQueryRetrieveLevel.Study && i.uid == null ).OrderByDescending( i => i.end_date ).FirstOrDefault();

                            // Determine the cutoff time which is the current time minus the programmed latency time.
                            DateTime cutoff = DateTime.Now.RoundUp(new TimeSpan(0, node.chunk_size_in_minutes, 0));
                            cutoff = cutoff.AddMinutes(-(double)node.latency_in_minutes);

                            // Determine the next date window.
                            DicomDateRange range = new DicomDateRange((DateTime)node.cfind_end_date, ((DateTime)node.cfind_end_date).AddMinutes(Math.Abs(node.chunk_size_in_minutes)));

                            // Create requests until we reach the cutoff time.
                            while (range.Minimum < cutoff)
                            {
                                // Don't exceed the cutoff time.
                                if (range.Maximum > cutoff)
                                    range.Maximum = cutoff;

                                // Determine the number of pending requests that encompass this date window.
                                int pending = db.vh_cfind_request.Where(                // When would this ever be positive???? 
                                    i => i.vh_node_id == node.vh_node_id &&
                                    i.level == (int)DicomQueryRetrieveLevel.Study &&
                                    i.is_completed == 0 &&
                                    i.uid == null &&                  // new
                                    range.Minimum >= i.start_date &&
                                    range.Maximum <= i.end_date).Count();

                                // If no requests are pending for this date window, then add the request.  
                                if (pending == 0)
                                {
                                    vh_cfind_request rq = new vh_cfind_request();
                                    rq.vh_node_id = node.vh_node_id;
                                    rq.created_on = DateTime.Now;
                                    rq.updated_on = DateTime.Now;
                                    rq.scheduled_on = DateTime.Now;
                                    rq.start_date = range.Minimum;
                                    rq.end_date = range.Maximum;
                                    rq.priority = (int)QueuePriority.High;
                                    rq.level = (int)DicomQueryRetrieveLevel.Study;
                                    rq.is_completed = 0;
                                    rq.response_count = 0;

                                    db.vh_cfind_request.Add(rq);

                                    if (range.Minimum < node.cfind_start_date)
                                        node.cfind_start_date = range.Minimum;
                                    if (range.Maximum > node.cfind_end_date)
                                        node.cfind_end_date = range.Maximum;

                                }

                                // Find the next date window.
                                range = new DicomDateRange(range.Maximum, range.Maximum.AddMinutes(Math.Abs(node.chunk_size_in_minutes)));
                            }

                            db.vh_node.Attach(node);
                            db.Entry(node).State = EntityState.Modified;

                            // Save changes to the database.
                            db.SaveChanges();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger(this.Name).Error(ex);
            }
            finally
            {
                lock (_lock)
                    _queueingCFinds = false;
            }
        }

        // Schedule individual refreshes.
        private void OnQueueStudyRefresh(object sender, ScheduledEventArgs ea)
        {
            try
            {
                using (var db = new VHModelContainer(true))
                {
                    try
                    {
                        LogManager.GetLogger(this.Name).Info("OnQueueStudyRefresh");

                        db.Database.CommandTimeout = 0;

                        // Send studies marked for refreshed to the cfind queue.
                        string command = "EXEC vh_cfind_request_refresh_insert";
                        db.Database.ExecuteSqlCommand(command);

                    }
                    catch (Exception e)
                    {
                        LogManager.GetLogger(this.Name).Error("Error processing studies marked for refresh: " + e.ToString());
                    }
                }
            }
            finally
            {
            }
        }

        // Update validaiton level.
        private void OnExecuteValidationLevelUpdate(object sender, ScheduledEventArgs ea)
        {
            try
            {
                LogManager.GetLogger(this.Name).Info("OnExecuteValidationLevelUpdate");


                using (VHModelContainer db = new VHModelContainer(true))
                {


                    db.Database.CommandTimeout = 0;

                    // Update the validation levels.
                    string command = "EXEC validation_level_update";
                    db.Database.ExecuteSqlCommand(command);

                }

            }
            catch (Exception ex)
            {
                LogManager.GetLogger(this.Name).Error(ex);
            }
            finally
            {
            }
        }


        // Queue CFind queue history
        private void OnQueueCFindQueueHistory(object sender, ScheduledEventArgs ea)
        {
            try
            {
                LogManager.GetLogger(this.Name).Info("OnQueueCFindQueueHistory");

                using (VHModelContainer db = new VHModelContainer(true))
                {

                    db.Database.CommandTimeout = 0;

                    // Queue the cfind queue history for each node.
                    string command = "EXEC vh_cfind_request_queue_history_insert";
                    db.Database.ExecuteSqlCommand(command);

                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger(this.Name).Error(ex);
            }
            finally
            {
            }
        }

        // Execute dashboard refresh.
        private void OnExecuteDashboardRefresh(object sender, ScheduledEventArgs ea)
        {
            try
            {
                LogManager.GetLogger(this.Name).Info("OnDashboardRefresh");

                using (VHModelContainer db = new VHModelContainer(true))
                {

                    db.Database.CommandTimeout = 0;

                    // Refresh the dashboard. 
                    string command = "EXEC dashboard_snapshot_refresh";
                    db.Database.ExecuteSqlCommand(command);

                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger(this.Name).Error(ex);
            }
            finally
            {
            }
        }

        // Run the validation stored procedure.
        private void OnExecuteValidation(object sender, ScheduledEventArgs ea)
        {
            try
            {
                LogManager.GetLogger(this.Name).Info("Execute Validation");


                using (var db = new VHModelContainer(true))
                {

                    LogManager.GetLogger(this.Name).Info(string.Format("Process Validation"));


                    // Run the stored procedure.
                    string sql = string.Format("EXEC vh_validation_failure_insert");
                    db.Database.CommandTimeout = 0;
                    db.Database.ExecuteSqlCommand(
                        sql
                    );

                    //                    validationRule.last_run = DateTime.Now;

                    // Save the last run time to the database.
                    //                    db.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger(this.Name).Error(ex);
            }
            finally
            {

            }
        }

        private class RequestState
        {
            public string Device { get; set; }
            public long Id { get; set; }
            public string UID { get; set; }
        }

        // Process scheduled CFind and CMove requests.
        private void OnProcessCFindCMove(object sender, ScheduledEventArgs ea)
        {

            lock (_lock)
            {
                if (_processing)
                    return;
                _processing = true;
            }

            try
            {
                using (var db = new VHModelContainer(true))
                {
                    // Process every node that is enabled and not deleted.
                    foreach (vh_node device in db.vh_node.Where(i => i.node_status == vh_node.Enabled && i.deleted_on.HasValue == false).ToArray())
                    {
                        DicomClientPool client = null;
                        try
                        {
                            client = _manager.GetConnection(device.vh_node_id.ToString());
                        }
                        catch
                        {
                            // config hasn't been loaded?
                            LogManager.GetLogger(this.Name).Error("Config Not Loaded for {0}", device.name);
                            continue;
                        }

                        // Determine the number of requests currently processing on this node.  
                        int count = client.CountPendingRequests(false, true, false);

                        LogManager.GetLogger(this.Name).Info(string.Format("Process Scheduled Requests for Node {0} Pending Requests:{1}", device.name, count));

                        // Don't exceed the limit for CFind requests.
                        if (count < device.cfind_limit)
                        {
                            // Determine the number of CFind requests that we can add.
                            var take = device.cfind_limit - count;
                            var priority = (int)QueuePriority.High;

                            lock (_dataLock)
                            {
                                LogManager.GetLogger(this.Name).Info(string.Format("Creating {0} Requests Pending Requests:{1}", take, count));

                                // Create each new request.
                                while (take > 0)
                                {
                                    // Get the next <take> requests in the queue for this node.   // x.scheduled_on < DateTime.Now ????  When would anything get scheduled in the future?
                                    foreach (var find in db.vh_cfind_request.AsNoTracking().Where(x => x.vh_node_id == device.vh_node_id && x.is_completed == 0 && x.priority == priority && x.scheduled_on < DateTime.Now).OrderBy(x => x.scheduled_on).Take(take).ToArray())
                                    {
                                        DicomCFindRequest rq = null;
                                        // We are processing a study.
                                        if (find.level == (int)DicomQueryRetrieveLevel.Study)
                                        {
                                            if (!String.IsNullOrEmpty(find.uid))
                                            {
                                                // Create a study query for a specific study.
                                                LogManager.GetLogger(this.Name).Info(string.Format("Creating Study Request for CFind {0}, UID {1}", find.vh_cfind_request_id, find.uid));
                                                rq = DicomCFindRequest.CreateStudyQuery(studyInstanceUid: find.uid);
                                            }
                                            else
                                            {
                                                // Create a study query for a date window.  Don't include the next date window by subracting one second from the end date.
                                                LogManager.GetLogger(this.Name).Info(string.Format("Creating Study Request for CFind {0}, {1} to {2}", find.vh_cfind_request_id, find.start_date, find.end_date));
                                                rq = DicomCFindRequest.CreateStudyQuery(studyDateTime: new DicomDateRange(find.start_date, find.end_date.AddSeconds(-1)));
                                                if (device.include_study_time == false)
                                                    rq.Dataset.Add(DicomTag.StudyTime, String.Empty);
                                            }
                                        }
                                        // We are processing a series.
                                        else if (find.level == (int)DicomQueryRetrieveLevel.Series)
                                        {
                                            // Create a series query.
                                            rq = DicomCFindRequest.CreateSeriesQuery(find.uid);

                                            // additional Q/R fields; DICOM requires sending blank values for requested fields
                                            if (device.include_institution == true)
                                                rq.Dataset.Add(DicomTag.InstitutionName, String.Empty);
                                            rq.Dataset.Add(DicomTag.StationName, String.Empty);
                                            rq.Dataset.Add(DicomTag.ProtocolName, String.Empty);
                                            LogManager.GetLogger(this.Name).Info(string.Format("Creating Series Request for CFind {0}, UID {1}", find.vh_cfind_request_id, find.uid));
                                        }
                                        if (rq != null)
                                        {
                                            // Add the request.
                                            rq.Priority = (QueuePriority)find.priority;
                                            rq.OnResponseReceived = OnResponseCFind;
                                            rq.UserState = new RequestState { Device = device.vh_node_id.ToString(), Id = find.vh_cfind_request_id, UID = find.uid };
                                            _manager.AddRequest(device.vh_node_id.ToString(), rq);
                                        }

                                        // Reduce the count of requests.
                                        take--;

                                        // Mark the CFind as updated since it is being processed.
                                        find.updated_on = DateTime.Now;

                                        // Mark the CFind as pending, but not completed.
                                        find.is_completed = -1;
                                        db.vh_cfind_request.Attach(find);
                                        db.Entry(find).State = EntityState.Modified;

                                    }
                                    // If we are done processing the urgent priority requests, move down to high priority.
                                    if (priority == (int)QueuePriority.Urgent)
                                        priority = (int)QueuePriority.High;
                                    // If we are done processing the high priority requests, move down to medium priority.
                                    else if (priority == (int)QueuePriority.High)
                                        priority = (int)QueuePriority.Medium;
                                    // If we done processing the medium priority requests, move down to the low priority.
                                    else if (priority == (int)QueuePriority.Medium)
                                        priority = (int)QueuePriority.Low;
                                    // If we done processing the low priority requests, move down to the recheck priority.
                                    else if (priority == (int)QueuePriority.Low)
                                        priority = (int)QueuePriority.Recheck;
                                    // If we are done processing the recheck priority requests, then we are done.
                                    else if (priority == (int)QueuePriority.Recheck)
                                        break;
                                }

                                // Update changes to the database.
                                db.SaveChanges();


                            }
                        }

                        // Count of the number of requests that are pending on this node.
                        count = client.CountPendingRequests(false, false, true);

                        // Don't exceed the limit for CMove requests.
                        if (count < device.cmove_limit)
                        {
                            // Determine the number of CMove requests that we can add.
                            var take = device.cmove_limit - count;
                            var priority = QueuePriority.High;

                            lock (_dataLock)
                            {
                                // Create each new request.
                                while (take > 0)
                                {
                                    // Get the next <take> requests in the queue for this node.   // x.scheduled_on < DateTime.Now ????  When would anything get scheduled in the future?
                                    foreach (var move in db.vh_cmove_request.AsNoTracking().Where(x => x.vh_source_node_id == device.vh_node_id && !x.is_completed && x.priority == (int)priority && x.scheduled_on < DateTime.Now).OrderBy(x => x.scheduled_on).Take(take).ToArray())
                                    {
                                        // Determine the destination node.
                                        var dest = _manager.GetConnection(move.vh_destination_node_id.ToString());

                                        // Add the request.
                                        DicomCMoveRequest rq = new DicomCMoveRequest(dest.ConnectionString.MoveAE, move.uid, (QueuePriority)move.priority);
                                        rq.OnResponseReceived = OnResponseCMove;
                                        rq.UserState = new RequestState { Device = device.vh_node_id.ToString(), Id = move.vh_cmove_request_id, UID = move.uid };
                                        _manager.AddRequest(device.vh_node_id.ToString(), rq);

                                        // Reduce the count of requests.
                                        take--;
                                    }

                                    // If we are done processing the urgent priority requests, move down to the high priority.
                                    if (priority == QueuePriority.Urgent)
                                        priority = QueuePriority.High;
                                    // If we are done processing the high priority requests, move down to medium priority.
                                    else if (priority == QueuePriority.High)
                                        priority = QueuePriority.Medium;
                                    // If we done processing the medium priority requests, move down to the low priority.
                                    else if (priority == QueuePriority.Medium)
                                        priority = QueuePriority.Low;
                                    // If we are done processing the low priority requests, move down to the recheck priority.
                                    else if (priority == QueuePriority.Low)
                                        priority = QueuePriority.Recheck;
                                    // If we are done processing the recheck priority requests, then we are done.
                                    else if (priority == QueuePriority.Recheck)
                                        break;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger(this.Name).Error(ex);
            }
            finally
            {
                lock (_lock)
                    _processing = false;
            }
        }

        // Process a CFind response.
        private void OnResponseCFind(DicomCFindRequest request, DicomCFindResponse response)
        {
            try
            {
                var state = request.UserState as RequestState;

                LogManager.GetLogger(this.Name).Info(string.Format("Process CFind Response Id {0} UID {1} State {2}", state.Id, state.UID, response.Status.State));

                VHModelContainer db = new VHModelContainer(true);

                // Find the associated CFind request in the queue.
                vh_cfind_request find = db.vh_cfind_request.FirstOrDefault(x => x.vh_cfind_request_id == state.Id);

                // A CFind request was returned that doesn't exist in the queue.
                if (find == null)
                {
                    throw new InvalidOperationException("Attempted to mark C-Find request completed but none exists with Id=" + state.Id.ToString());
                }


                // Mark the CFind request as completed.
                if (response.Status.State == DicomState.Success)
                {


                    db.Database.CommandTimeout = 0;

                    var sql = "EXEC vh_cfind_request_mark_completed @requestID";

                    db.Database.ExecuteSqlCommand(
                        sql,
                        new SqlParameter("@requestID", state.Id));

                    //db.SaveChanges();
                    // Mark as completed if the state is success or cancel.
                    /*
                                        find.is_completed = 1;
                                        find.updated_on = DateTime.Now;
                                        find.dicom_status = Convert.ToInt32(response.Status.State);
                                        find.response_count++;
                                        db.SaveChanges();
                    */
                    return;
                }
                else if (response.Status.State == DicomState.Warning || response.Status.State == DicomState.Failure || response.Status.State == DicomState.Cancel)
                {
                    find.dicom_status = Convert.ToInt32(response.Status.State);
                    find.response_count++;

                    find.is_completed = 1;
                    find.updated_on = DateTime.Now;
                    if (response.Status.State != DicomState.Cancel)
                        find.error_message = response.Status.ToString();
                    db.SaveChanges();

                    return;
                }

                find.dicom_status = Convert.ToInt32(response.Status.State);
                find.response_count++;


                // Get the UID.
                var uid = state.UID;
                if (String.IsNullOrEmpty(uid))
                    uid = response.Dataset.Get<string>(DicomTag.StudyInstanceUID);

                // Something is wrong if there is not a UID.
                if (String.IsNullOrEmpty(uid))
                {
                    LogManager.GetLogger("DICOM").Error("CFind Response No Study UID Found.");
                    return;
                }

                var ds = response.Dataset;

                // because there are systems that don't properly understand date/time etiquette we have to serialize study record creation ?????
                lock (_dataLock)
                {

                    // Find the study associated with the given node and UID.
                    var study = db.vh_study.FirstOrDefault(x => x.vh_node.vh_node_id.ToString() == state.Device && x.uid == uid);

                    //LogManager.GetLogger(this.Name).Info(string.Format("CFind Response Study:  node {0} UID {1} Series {2} StudyID {3}", study.vh_node_id, study.uid, study.vh_series.Count(), study.vh_study_id));


                    // This is a response to a study request and either no study exists currently for this response or the request was for an individual study.
                    if (request.Level == DicomQueryRetrieveLevel.Study && (study == null || !String.IsNullOrEmpty(state.UID)))
                    {

                        LogManager.GetLogger(this.Name).Info("Processing Pending C{0} {1}", response.Status.State, find.response_count);

                        // Get the associated node.
                        var node = db.vh_node.FirstOrDefault(x => x.vh_node_id.ToString() == state.Device);

                        // How are we querying for studies against a node that doesn't exist?
                        if (node == null)
                            return;

                        // No study exists in the database for this response.  Therefore, create a new study.
                        if (study == null)
                        {
                            LogManager.GetLogger(this.Name).Info(string.Format("Creating New Study for CFind Id {0} UID {1}", state.Id, state.UID));

                            study = new vh_study();
                            study.created_on = DateTime.Now;
                            //                            study.deleted_on = null;
                            study.vh_node = node;
                            study.uid = uid;
                            //                           study.validated_on = null;

                            db.vh_study.Add(study);
                            find.insert_row_count++;
                        }
                        // The study is found and is being updated.
                        else
                        {
                            find.update_row_count++;
                            LogManager.GetLogger(this.Name).Info(string.Format("Update Study for CFind Id {0} UID {1}", state.Id, state.UID));
                        }

                        DateTime birthDate = ds.Get<DateTime>(DicomTag.PatientBirthDate, DateTime.MinValue);

                        if (birthDate < Convert.ToDateTime("1753-01-01 00:00:00.000"))
                        {
                            LogManager.GetLogger(this.Name).Info(string.Format("Out-of-Range Birthdate {0} Id {1} UID {2}", birthDate, state.Id, state.UID));
                            birthDate = Convert.ToDateTime("1753-01-01 00:00:00.000");
                        }

                        study.updated_on = DateTime.Now;
                        study.validation_requested_for = DateTime.Now;
                        study.response_on = DateTime.Now;

                        study.patient_id = ds.Get<string>(DicomTag.PatientID);
                        study.patient_issuer = ds.Get<string>(DicomTag.IssuerOfPatientID);
                        //study.patient_other_ids = ds.Get<string>( DicomTag.OtherPatientIDs );
                        study.patient_firstname = PersonName.Parse(ds.Get<string>(DicomTag.PatientName, String.Empty)).First;
                        study.patient_lastname = PersonName.Parse(ds.Get<string>(DicomTag.PatientName, String.Empty)).Last;
                        study.patient_middlename = PersonName.Parse(ds.Get<string>(DicomTag.PatientName, String.Empty)).Middle;
                        study.patient_prefix = PersonName.Parse(ds.Get<string>(DicomTag.PatientName, String.Empty)).Prefix;
                        study.patient_suffix = PersonName.Parse(ds.Get<string>(DicomTag.PatientName, String.Empty)).Suffix;
                        study.patient_sex = ds.Get<string>(DicomTag.PatientSex);
                        study.patient_birthdate = birthDate;

                        if (study.patient_birthdate.Value == DateTime.MinValue)
                            study.patient_birthdate = null;

                        study.modalities_in_study = ds.Get<string>(DicomTag.ModalitiesInStudy, -1);

                        if (!String.IsNullOrEmpty(study.modalities_in_study))
                        {
                            study.modality = study.modalities_in_study.Split('\\', '/').OrderBy(x => x, ModalityComparer.Instance).FirstOrDefault();
                        }

                        if (String.IsNullOrEmpty(study.modality))
                        {
                            study.modality = ds.Get<string>(DicomTag.Modality, "UN");

                            if (String.IsNullOrEmpty(study.modalities_in_study))
                                study.modalities_in_study = study.modality;
                        }

                        study.date_time = ds.GetDateTime(DicomTag.StudyDate, DicomTag.StudyTime);
                        study.accession_number = ds.Get<string>(DicomTag.AccessionNumber);
                        study.study_id = ds.Get<string>(DicomTag.StudyID);
                        study.description = ds.Get<string>(DicomTag.StudyDescription);
                        study.number_of_instances = ds.Get<int>(DicomTag.NumberOfStudyRelatedInstances, 0, 0);

                        // Remove the series associated with this study.
                        List<vh_series> seriesList = study.vh_series.ToList();
                        foreach (vh_series s in seriesList)
                            db.vh_series.Remove(s);

                        // Send changes to the database.
                        db.SaveChanges();

                        // If this is a response to a CMove request, mark the CMove request as verified.
                        foreach (var move in db.vh_cmove_request.Where(x => x.vh_destination_node_id.ToString() == state.Device && x.uid == uid && !x.is_verified))
                            move.is_verified = true;

                        // Send move changes to the database.
                        db.SaveChanges();

                        // Create a request for the series associated with this study.
                        var sfind = new vh_cfind_request();
                        sfind.vh_node_id = find.vh_node_id;
                        sfind.created_on = DateTime.Now;
                        sfind.updated_on = DateTime.Now;
                        sfind.scheduled_on = DateTime.Now;
                        sfind.priority = (int)request.Priority;
                        sfind.level = (int)DicomQueryRetrieveLevel.Series;
                        sfind.uid = uid;
                        sfind.start_date = (DateTime)SqlDateTime.MinValue;
                        sfind.end_date = (DateTime)SqlDateTime.MaxValue;
                        sfind.is_completed = 0;
                        sfind.response_count = 0;
                        db.vh_cfind_request.Add(sfind);

                        // Send the series request to the database.
                        db.SaveChanges();

                        return;
                    }
                    else if (request.Level == DicomQueryRetrieveLevel.Study)
                    {
                        study.response_on = DateTime.Now;
                        db.SaveChanges();
                    }

                    // Return if the study could not be found. 
                    if (study == null)
                    {
                        LogManager.GetLogger("DICOM").Error("CFind Response Exit No Study Found.");
                        return;
                    }

                    // Process a series level request.
                    if (request.Level == DicomQueryRetrieveLevel.Series)
                    {
                        var info = new vh_series
                        {
                            created_on = DateTime.Now,
                            updated_on = DateTime.Now,
                            uid = ds.Get<string>(DicomTag.SeriesInstanceUID),
                            number = ds.Get<string>(DicomTag.SeriesNumber),
                            description = ds.Get<string>(DicomTag.SeriesDescription),
                            modality = ds.Get<string>(DicomTag.Modality),
                            station_name = ds.Get<string>(DicomTag.StationName),
                            institution_name = ds.Get<string>(DicomTag.InstitutionName),
                            protocol_name = ds.Get<string>(DicomTag.ProtocolName),
                            number_of_instances = ds.Get<int>(DicomTag.NumberOfSeriesRelatedInstances, 0, 0)
                        };

                        bool newSeries = true;
                        // Find the series associated with this study.
                        var series = study.vh_series.Where(x => x.uid == info.uid).ToList();

                        //LogManager.GetLogger(this.Name).Info(string.Format("Series Count {0} Series Count with UID {1}", study.vh_series.Count(), series.Count()));

                        foreach (vh_series s in series)
                        {
                            // Update the current series by removing the existing one.  The new series is added below.
                            db.vh_series.Remove(s);

                            newSeries = false;
                            find.update_row_count++;
                            LogManager.GetLogger(this.Name).Info(string.Format("Update Series for CFind Id {0} Study UID {1} Series UID {2}", state.Id, state.UID, info.uid));
                        }

                        // Send changes to the database.
                        //db.SaveChanges();

                        if (newSeries)
                        {
                            find.insert_row_count++;
                            LogManager.GetLogger(this.Name).Info(string.Format("Creating New Series for CFind Id {0} Study UID {1} Series UID {2}", state.Id, state.UID, info.uid));
                        }

                        // Add the new series.  Mark the associated study for validation.
                        study.vh_series.Add(info);
                        study.validation_requested_for = DateTime.Now;

                        // Send new series to the database.
                        db.SaveChanges();
                    }
                }
            }
            catch (Exception e)
            {
                LogManager.GetLogger(Name).Error(e);
                throw;
            }
        }

        // Process a CMove response.
        private void OnResponseCMove(DicomCMoveRequest request, DicomCMoveResponse response)
        {
            try
            {
                var state = request.UserState as RequestState;

                if (response.Status.State != DicomState.Pending)
                {
                    using (var db = new VHModelContainer(true))
                    {

                        // Find the associated CFind request in the queue.
                        var move = db.vh_cmove_request.FirstOrDefault(x => x.vh_cmove_request_id == state.Id);

                        // Make sure that the CMove request was found.
                        if (move != null)
                        {
                            // If the request was unsuccessful then record the failure in the queue.
                            if (response.Status.State != DicomState.Success)
                            {
                                move.is_failed = response.Status.State == DicomState.Failure;
                                move.error_message = response.Status.ToString();
                            }

                            // Mark the move as completed and update to the database.
                            move.is_completed = true;
                            db.SaveChanges();

                            // Find the destination node.
                            var dest = db.vh_node.FirstOrDefault(x => x.vh_node_id == move.vh_destination_node_id);

                            // Return if we can't find the destination node.
                            if (dest == null)
                            {
                                // ??? how did we end up here???  Do we need to log something here????
                                return;
                            }

                            // Check to see if we verify CMoves on the destination node.
                            if (dest.dicom_verify_cmoves)
                            {
                                // Create a CFind request for this study on the destination node.
                                var find = new vh_cfind_request();
                                find.vh_node_id = (int)move.vh_destination_node_id;
                                find.created_on = DateTime.Now;
                                find.updated_on = DateTime.Now;
                                find.scheduled_on = DateTime.Now;
                                find.priority = (int)QueuePriority.Medium;
                                find.level = (int)DicomQueryRetrieveLevel.Study;
                                find.uid = move.uid;
                                find.start_date = (DateTime)SqlDateTime.MinValue;
                                find.end_date = (DateTime)SqlDateTime.MaxValue;
                                find.is_completed = 0;
                                find.response_count = 0;

                                // Add the CFind request.
                                db.vh_cfind_request.Add(find);
                                db.SaveChanges();
                            }
                        }

                        return;
                    }
                }
            }
            catch (Exception e)
            {
                LogManager.GetLogger(Name).Error(e);
                throw;
            }
        }
    }
}
