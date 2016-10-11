using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;

using Quartz.Impl.Matchers;
using Quartz.Spi;
using Quartz.Simpl;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Microsoft.Extensions.Logging;
using Raven.Client.Document;

namespace Quartz.Impl.RavenDB
{
    /// <summary> 
    /// An implementation of <see cref="IJobStore" /> to use ravenDB as a persistent Job Store.
    /// Mostly based on RAMJobStore logic with changes to support persistent storage.
    /// Provides an <see cref="IJob" />
    /// and <see cref="ITrigger" /> storage mechanism for the
    /// <see cref="QuartzScheduler" />'s use.
    /// </summary>
    /// <remarks>
    /// Storage of <see cref="IJob" /> s and <see cref="ITrigger" /> s should be keyed
    /// on the combination of their name and group for uniqueness.
    /// </remarks>
    /// <seealso cref="QuartzScheduler" />
    /// <seealso cref="IJobStore" />
    /// <seealso cref="ITrigger" />
    /// <seealso cref="IJob" />
    /// <seealso cref="IJobDetail" />
    /// <seealso cref="JobDataMap" />
    /// <seealso cref="ICalendar" />
    /// <author>Iftah Ben Zaken</author>
    public class RavenJobStore : IJobStore
    {
        private TimeSpan misfireThreshold = TimeSpan.FromSeconds(5);
        private ISchedulerSignaler signaler;
        private static long ftrCtr = SystemTime.UtcNow().Ticks;

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 100;
        public bool Clustered => false;

        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        public static string Url { get; set; }
        public static string DefaultDatabase { get; set; }
        public static string ApiKey { get; set; }
        private readonly IDocumentStore _store;

        public RavenJobStore()
        {
            _store = new DocumentStore() { Url = "http://localhost:9001/databases/test" };
            _store.Initialize();

            InstanceName = "UnitTestScheduler";
            InstanceId = "instance_two";

            new TriggerIndex().Execute(_store);
            new JobIndex().Execute(_store);
        }

        /// <summary>
        /// Called by the QuartzScheduler before the <see cref="IJobStore" /> is
        /// used, in order to give it a chance to Initialize.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler s)
        {
            await Task.FromResult(0);
            signaler = s;
        }

        /// <summary>
        /// Sets the schedulers's state
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task SetSchedulerState(string state)
        {
            using (var session = _store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName);
                sched.State = state;
                await session.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
        /// the scheduler has started.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task SchedulerStarted()
        {
            var cmds = _store.AsyncDatabaseCommands;
            var docMetaData = await cmds.HeadAsync(InstanceName);
            if (docMetaData != null)
            {
                // Scheduler with same instance name already exists, recover persistent data
                try
                {
                    await RecoverSchedulerData();
                }
                catch (SchedulerException se)
                {
                    throw new SchedulerConfigException("Failure occurred during job recovery.", se);
                }
                return;
            }

            // If scheduler doesn't exist create new empty scheduler and store it
            var schedToStore = new Scheduler
            {
                InstanceName = InstanceName,
                LastCheckinTime = DateTimeOffset.MinValue,
                CheckinInterval = DateTimeOffset.MinValue,
                Calendars = new Dictionary<string, ICalendar>(),
                PausedJobGroups = new HashSet<string>(),
                BlockedJobs = new HashSet<string>(),
                State = "Started"
            };

            using (var session = _store.OpenAsyncSession())
            {
                await session.StoreAsync(schedToStore, InstanceName);
                await session.SaveChangesAsync();
            }            
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        /// the scheduler has been paused.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task SchedulerPaused()
        {
            await SetSchedulerState("Paused");
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        /// the scheduler has resumed after being paused.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task SchedulerResumed()
        {
            await SetSchedulerState("Resumed");
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
        /// it should free up all of it's resources because the scheduler is
        /// shutting down.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task Shutdown()
        {
            await SetSchedulerState("Shutdown");
        }

        /// <summary>
        /// Will recover any failed or misfired jobs and clean up the data store as
        /// appropriate.
        /// </summary>
        /// <exception cref="JobPersistenceException">Condition.</exception>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        protected virtual async Task RecoverSchedulerData()
        {
            try
            {
                //_logger.LogInformation("Trying to recover persisted scheduler data for" + InstanceName);

                // update inconsistent states
                using (var session = _store.OpenAsyncSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });

                    var queryResult = await session.Query<Trigger, TriggerIndex>()
                        .Where(t => (t.Scheduler == InstanceName) && (t.State == InternalTriggerState.Acquired || t.State == InternalTriggerState.Blocked)).ToListAsync();
                    foreach (var trigger in queryResult)
                    {
                        var triggerToUpdate = await session.LoadAsync<Trigger>(trigger.Key);
                        triggerToUpdate.State = InternalTriggerState.Waiting;
                    }
                    await session.SaveChangesAsync();
                }

                //_logger.LogInformation("Freed triggers from 'acquired' / 'blocked' state.");
                
                // recover jobs marked for recovery that were not fully executed
                IList<IOperableTrigger> recoveringJobTriggers = new List<IOperableTrigger>();

                using (var session = _store.OpenAsyncSession())
                {
                    var queryResultJobs = await session.Query<Job, JobIndex>()
                        .Where(j => (j.Scheduler == InstanceName) && j.RequestsRecovery).ToListAsync();

                    foreach (var job in queryResultJobs)
                    {
                        recoveringJobTriggers.AddRange(await GetTriggersForJob(new JobKey(job.Name, job.Group)));
                    }
                }

                //_logger.LogInformation("Recovering " + recoveringJobTriggers.Count +
                //         " jobs that were in-progress at the time of the last shut-down.");

                foreach (IOperableTrigger trigger in recoveringJobTriggers)
                {
                    if (await CheckExists(trigger.JobKey))
                    {
                        trigger.ComputeFirstFireTimeUtc(null);
                        await StoreTrigger(trigger, true);
                    }
                }
                //_logger.LogInformation("Recovery complete.");

                // remove lingering 'complete' triggers...
                //_logger.LogInformation("Removing 'complete' triggers...");
                IList<Trigger> triggersInStateComplete;

                using (var session = _store.OpenAsyncSession())
                {
                    triggersInStateComplete = await session.Query<Trigger, TriggerIndex>()
                        .Where(t => (t.Scheduler == InstanceName) && (t.State == InternalTriggerState.Complete)).ToListAsync();
                }

                foreach (var trigger in triggersInStateComplete)
                {
                    await RemoveTrigger(new TriggerKey(trigger.Name, trigger.Group));
                }

                using (var session = _store.OpenAsyncSession())
                {
                    var schedToUpdate = await session.LoadAsync<Scheduler>(InstanceName);
                    schedToUpdate.State = "Started";
                    await session.SaveChangesAsync();
                }
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't recover jobs: " + e.Message, e);
            }
        }

        /// <summary>
        /// Gets the fired trigger record id.
        /// </summary>
        /// <returns>The fired trigger record id.</returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        protected virtual string GetFiredTriggerRecordId()
        {
            var value = Interlocked.Increment(ref ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Store the given <see cref="IJobDetail" /> and <see cref="ITrigger" />.
        /// </summary>
        /// <param name="newJob">The <see cref="IJobDetail" /> to be stored.</param>
        /// <param name="newTrigger">The <see cref="ITrigger" /> to be stored.</param>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            await StoreJob(newJob, true);
            await StoreTrigger(newTrigger, true);
        }

        /// <summary>
        /// returns true if the given JobGroup is paused
        /// </summary>
        /// <param name="groupName"></param>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> IsJobGroupPaused(string groupName)
        {
            using (var session = _store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName);
                return sched.PausedJobGroups.Contains(groupName);
            }
        }

        /// <summary>
        /// returns true if the given TriggerGroup
        /// is paused
        /// </summary>
        /// <param name="groupName"></param>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> IsTriggerGroupPaused(string groupName)
        {
            return (await GetPausedTriggerGroups()).Contains(groupName);

        }

        /// <summary>
        /// Store the given <see cref="IJobDetail" />.
        /// </summary>
        /// <param name="newJob">The <see cref="IJobDetail" /> to be stored.</param>
        /// <param name="replaceExisting">
        /// If <see langword="true" />, any <see cref="IJob" /> existing in the
        /// <see cref="IJobStore" /> with the same name and group should be
        /// over-written.
        /// </param>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            if (await CheckExists(newJob.Key))
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }
            }

            var job = new Job(newJob, InstanceName);

            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new JobIndex().IndexName });
                // Store() overwrites if job id already exists
                await session.StoreAsync(job, job.Key);
                await session.SaveChangesAsync();
            }
        }

        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task StoreJobsAndTriggers(IDictionary<IJobDetail, ISet<ITrigger>> triggersAndJobs, bool replace)
        {
            using (var bulkInsert = _store.BulkInsert(options: new BulkInsertOptions() { OverwriteExisting = replace }))
            {
                foreach (var pair in triggersAndJobs)
                {
                    // First store the current job
                    bulkInsert.Store(new Job(pair.Key, InstanceName), pair.Key.Key.Name + "/" + pair.Key.Key.Group);
                    
                    // Storing all triggers for the current job
                    foreach (var trig in pair.Value)
                    {
                        var operTrig = trig as IOperableTrigger;
                        if (operTrig == null)
                        {
                            continue;
                        }
                        var trigger = new Trigger(operTrig, InstanceName);

                        if ((await GetPausedTriggerGroups()).Contains(operTrig.Key.Group) || (await GetPausedJobGroups()).Contains(operTrig.JobKey.Group))
                        {
                            trigger.State = InternalTriggerState.Paused;
                            if ((await GetBlockedJobs()).Contains(operTrig.JobKey.Name + "/" + operTrig.JobKey.Group))
                            {
                                trigger.State = InternalTriggerState.PausedAndBlocked;
                            }
                        }
                        else if ((await GetBlockedJobs()).Contains(operTrig.JobKey.Name + "/" + operTrig.JobKey.Group))
                        {
                            trigger.State = InternalTriggerState.Blocked;
                        }

                        bulkInsert.Store(trigger, trigger.Key);
                    }
                }
            } // bulkInsert is disposed - same effect as session.SaveChanges()
            await Task.FromResult(0);
        }

        /// <summary>
        /// Remove (delete) the <see cref="IJob" /> with the given
        /// key, and any <see cref="ITrigger" /> s that reference
        /// it.
        /// </summary>
        /// <returns>
        /// 	<see langword="true" /> if a <see cref="IJob" /> with the given name and
        /// group was found and removed from the store.
        /// </returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> RemoveJob(JobKey jobKey)
        {
            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new JobIndex().IndexName });
                if (!await CheckExists(jobKey))
                {
                    return false;
                }

                session.Advanced.Defer(new DeleteCommandData
                {
                    Key = jobKey.Name + "/" + jobKey.Group
                });
                await session.SaveChangesAsync();
            }
            return true;
        }

        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> RemoveJobs(IList<JobKey> jobKeys)
        {
            // Returns false in case at least one job removal fails
            var result = true;
            foreach (var key in jobKeys)
            {
                result &= await RemoveJob(key);
            }
            return result;
        }

        /// <summary>
        /// Retrieve the <see cref="IJobDetail" /> for the given
        /// <see cref="IJob" />.
        /// </summary>
        /// <returns>
        /// The desired <see cref="IJob" />, or null if there is no match.
        /// </returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IJobDetail> RetrieveJob(JobKey jobKey)
        {
            using (var session = _store.OpenAsyncSession())
            {
                var job = await session.LoadAsync<Job>(jobKey.Name + "/" + jobKey.Group);

                return job?.Deserialize();
            }
        }

        /// <summary>
        /// Store the given <see cref="ITrigger" />.
        /// </summary>
        /// <param name="newTrigger">The <see cref="ITrigger" /> to be stored.</param>
        /// <param name="replaceExisting">If <see langword="true" />, any <see cref="ITrigger" /> existing in
        /// the <see cref="IJobStore" /> with the same name and group should
        /// be over-written.</param>
        /// <throws>  ObjectAlreadyExistsException </throws>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            if (await CheckExists(newTrigger.Key))
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newTrigger);
                }
            }

            if (!await CheckExists(newTrigger.JobKey))
            {
                throw new JobPersistenceException("The job (" + newTrigger.JobKey + ") referenced by the trigger does not exist.");
            }

            var trigger = new Trigger(newTrigger, InstanceName);

            // make sure trigger group is not paused and that job is not blocked
            if ((await GetPausedTriggerGroups()).Contains(newTrigger.Key.Group) || (await GetPausedJobGroups()).Contains(newTrigger.JobKey.Group))
            {
                trigger.State = InternalTriggerState.Paused;
                if ((await GetBlockedJobs()).Contains(newTrigger.JobKey.Name + "/" + newTrigger.JobKey.Group))
                {
                    trigger.State = InternalTriggerState.PausedAndBlocked;
                }
            }
            else if ((await GetBlockedJobs()).Contains(newTrigger.JobKey.Name + "/" + newTrigger.JobKey.Group))
            {
                trigger.State = InternalTriggerState.Blocked;
            }

            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });
                // Overwrite if exists
                await session.StoreAsync(trigger, trigger.Key);
                await session.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="ITrigger" /> with the given key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If removal of the <see cref="ITrigger" /> results in an 'orphaned' <see cref="IJob" />
        /// that is not 'durable', then the <see cref="IJob" /> should be deleted
        /// also.
        /// </para>
        /// </remarks>
        /// <returns>
        /// <see langword="true" /> if a <see cref="ITrigger" /> with the given
        /// name and group was found and removed from the store.
        /// </returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> RemoveTrigger(TriggerKey triggerKey)
        {
            if (!await CheckExists(triggerKey))
            {
                return false;
            }
            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName, new JobIndex().IndexName });
                var trigger = await session.LoadAsync<Trigger>(triggerKey.Name + "/" + triggerKey.Group);
                var job = await RetrieveJob(new JobKey(trigger.JobName, trigger.Group));

                // Delete trigger
                session.Advanced.Defer(new DeleteCommandData
                {
                    Key = triggerKey.Name + "/" + triggerKey.Group
                });
                await session.SaveChangesAsync();

                // Remove the trigger's job if it is not associated with any other triggers
                var trigList = await GetTriggersForJob(job.Key);
                if ((trigList == null || trigList.Count == 0) && !job.Durable)
                {
                    if (await RemoveJob(job.Key))
                    {
                        await signaler.NotifySchedulerListenersJobDeleted(job.Key);
                    }
                }
            }
            return true;
        }

        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            // Returns false in case at least one trigger removal fails
            var result = true;
            foreach (var key in triggerKeys)
            {
                result &= await RemoveTrigger(key);
            }
            return result;
        }

        /// <summary>
        /// Remove (delete) the <see cref="ITrigger" /> with the
        /// given name, and store the new given one - which must be associated
        /// with the same job.
        /// </summary>
        /// <param name="triggerKey">The <see cref="ITrigger"/> to be replaced.</param>
        /// <param name="newTrigger">The new <see cref="ITrigger" /> to be stored.</param>
        /// <returns>
        /// 	<see langword="true" /> if a <see cref="ITrigger" /> with the given
        /// name and group was found and removed from the store.
        /// </returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            if (!await CheckExists(triggerKey))
            {
                return false;
            }
            var wasRemoved = await RemoveTrigger(triggerKey);
            if (wasRemoved)
            {
                await StoreTrigger(newTrigger, true);
            }
            return wasRemoved;
        }

        /// <summary>
        /// Retrieve the given <see cref="ITrigger" />.
        /// </summary>
        /// <returns>
        /// The desired <see cref="ITrigger" />, or null if there is no
        /// match.
        /// </returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey)
        {
            if (!await CheckExists(triggerKey))
            {
                return null;
            }

            using (var session = _store.OpenAsyncSession())
            {
                var trigger = await session.LoadAsync<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                return trigger?.Deserialize();
            }
        }

        /// <summary>
        /// Determine whether a <see cref="ICalendar" /> with the given identifier already
        /// exists within the scheduler.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="calName">the identifier to check for</param>
        /// <returns>true if a calendar exists with the given identifier</returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> CalendarExists(string calName)
        {
            bool answer;
            using (var session = _store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName);
                if (sched == null) return false;
                try
                {
                    answer = sched.Calendars.ContainsKey(calName);
                }
                catch (ArgumentNullException argumentNullException)
                {
                    //_logger.LogError("Calendars collection is null.", argumentNullException);
                    answer = false;
                }
            }
            return answer;
        }

        /// <summary>
        /// Determine whether a <see cref="IJob" /> with the given identifier already
        /// exists within the scheduler.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="jobKey">the identifier to check for</param>
        /// <returns>true if a job exists with the given identifier</returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> CheckExists(JobKey jobKey)
        {
            var cmds = _store.AsyncDatabaseCommands;
            var docMetaData = await cmds.HeadAsync(jobKey.Name + "/" + jobKey.Group);
            return docMetaData != null;
        }

        /// <summary>
        /// Determine whether a <see cref="ITrigger" /> with the given identifier already
        /// exists within the scheduler.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="triggerKey">the identifier to check for</param>
        /// <returns>true if a trigger exists with the given identifier</returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> CheckExists(TriggerKey triggerKey)
        {
            var cmds = _store.AsyncDatabaseCommands;
            var docMetaData = await cmds.HeadAsync(triggerKey.Name + "/" + triggerKey.Group);
            return docMetaData != null;
        }

        /// <summary>
        /// Clear (delete!) all scheduling data - all <see cref="IJob"/>s, <see cref="ITrigger" />s
        /// <see cref="ICalendar" />s.
        /// </summary>
        /// <remarks>
        /// </remarks>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task ClearAllSchedulingData()
        {
            await _store.AsyncDatabaseCommands.DeleteByIndexAsync("Raven/DocumentsByEntityName", new IndexQuery(), new BulkOperationOptions() { AllowStale = true });
        }

        /// <summary>
        /// Store the <see cref="ICalendar" /> with the given identifier.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="name">the identifier for the calendar</param>
        /// <param name="calendar">the name of the calendar</param>
        /// <param name="replaceExisting">should replace existing calendar</param>
        /// <param name="updateTriggers">should update triggers</param>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            var calendarCopy = (ICalendar)calendar.Clone();

            using (var session = _store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName);

                if (sched?.Calendars == null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                }

                if (await CalendarExists(name) && !replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(string.Format(CultureInfo.InvariantCulture, "Calendar with name '{0}' already exists.", name));
                }

                // add or replace calendar
                sched.Calendars[name] = calendarCopy;

                if (!updateTriggers)
                {
                    return;
                }

                var triggersKeysToUpdate = await session
                    .Query<Trigger, TriggerIndex>()
                    .Where(t => t.CalendarName == name)
                    .Select(t => t.Key)
                    .ToListAsync();

                if (triggersKeysToUpdate.Any())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });
                    foreach (var triggerKey in triggersKeysToUpdate)
                    {
                        var triggerToUpdate = await session.LoadAsync<Trigger>(triggerKey);
                        var trigger = triggerToUpdate.Deserialize();
                        trigger.UpdateWithNewCalendar(calendarCopy, misfireThreshold);
                        triggerToUpdate.UpdateFireTimes(trigger);
                    }
                }

                await session.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="ICalendar" /> with the
        /// given name.
        /// </summary>
        /// <remarks>
        /// If removal of the <see cref="ICalendar" /> would result in
        /// <see cref="ITrigger" />s pointing to non-existent calendars, then a
        /// <see cref="JobPersistenceException" /> will be thrown.
        /// </remarks>
        /// <param name="calName">The name of the <see cref="ICalendar" /> to be removed.</param>
        /// <returns>
        /// <see langword="true" /> if a <see cref="ICalendar" /> with the given name
        /// was found and removed from the store.
        /// </returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<bool> RemoveCalendar(string calName)
        {
            if (await RetrieveCalendar(calName) == null)
            {
                return false;
            }
            var calCollection = await RetrieveCalendarCollection();
            calCollection.Remove(calName);

            using (var session = _store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName);
                sched.Calendars = calCollection;
                await session.SaveChangesAsync();
            }
            return true;
        }

        /// <summary>
        /// Retrieve the given <see cref="ITrigger" />.
        /// </summary>
        /// <param name="calName">The name of the <see cref="ICalendar" /> to be retrieved.</param>
        /// <returns>
        /// The desired <see cref="ICalendar" />, or null if there is no
        /// match.
        /// </returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<ICalendar> RetrieveCalendar(string calName)
        {
            var callCollection = await RetrieveCalendarCollection();
            return callCollection.ContainsKey(calName) ? callCollection[calName] : null;
        }

        /// <summary>
        /// Get the <see cref="ICalendar" />s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<Dictionary<string, ICalendar>> RetrieveCalendarCollection()
        {
            using (var session = _store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName);
                if (sched == null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                }
                if (sched.Calendars == null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Calendar collection in '{0}' is null", InstanceName));
                }
                return sched.Calendars;
            }
        }

        /// <summary>
        /// Get the number of <see cref="IJob" />s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<int> GetNumberOfJobs()
        {
            using (var session = _store.OpenAsyncSession())
            {
                return await session.Query<Job, JobIndex>().CountAsync();
            }
        }

        /// <summary>
        /// Get the number of <see cref="ITrigger" />s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<int> GetNumberOfTriggers()
        {
            using (var session = _store.OpenAsyncSession())
            {
                return await session.Query<Trigger, TriggerIndex>().CountAsync();
            }
        }

        /// <summary>
        /// Get the number of <see cref="ICalendar" /> s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<int> GetNumberOfCalendars()
        {
            return (await RetrieveCalendarCollection()).Count;
        }

        /// <summary>
        /// Get the names of all of the <see cref="IJob" /> s that
        /// have the given group name.
        /// <para>
        /// If there are no jobs in the given group name, the result should be a
        /// zero-length array (not <see langword="null" />).
        /// </para>
        /// </summary>
        /// <param name="matcher"></param>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<ISet<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new HashSet<JobKey>();

            using (var session = _store.OpenAsyncSession())
            {
                var allJobs = await session.Advanced.LoadStartingWithAsync<Job>("job/", null, 0, 1024);

                foreach (var job in allJobs)
                {
                    if (op.Evaluate(job.Group, compareToValue))
                    {
                        result.Add(new JobKey(job.Name, job.Group));
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Get the names of all of the <see cref="ITrigger" />s
        /// that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        /// zero-length array (not <see langword="null" />).
        /// </para>
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<ISet<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new HashSet<TriggerKey>();

            using (var session = _store.OpenAsyncSession())
            {
                var allTriggers = await session.Query<Trigger, TriggerIndex>().ToListAsync();

                foreach (var trigger in allTriggers)
                {
                    if (op.Evaluate(trigger.Group, compareToValue))
                    {
                        result.Add(new TriggerKey(trigger.Name, trigger.Group));
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Get the names of all of the <see cref="IJob" />
        /// groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        /// array (not <see langword="null" />).
        /// </para>
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IReadOnlyList<string>> GetJobGroupNames()
        {
            using (var session = _store.OpenAsyncSession())
            {
                return (await session.Advanced.LoadStartingWithAsync<Job>("job/", null, 0, 1024)).Select(t => t.Group).Distinct().ToList();
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="ITrigger" />
        /// groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        /// array (not <see langword="null" />).
        /// </para>
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IReadOnlyList<string>> GetTriggerGroupNames()
        {
            using (var session = _store.OpenAsyncSession())
            {
                try
                {
                    var result = await session.Query<Trigger, TriggerIndex>()
                        .Select(t => t.Group)
                        .Distinct()
                        .ToListAsync();
                    return result.ToList();
                }
                catch (ArgumentNullException)
                {
                    return new List<string>();
                }
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="ICalendar" /> s
        /// in the <see cref="IJobStore" />.
        /// <para>
        /// If there are no Calendars in the given group name, the result should be
        /// a zero-length array (not <see langword="null" />).
        /// </para>
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IReadOnlyList<string>> GetCalendarNames()
        {
            return (await RetrieveCalendarCollection()).Keys.ToList();
        }

        /// <summary>
        /// Get all of the Triggers that are associated to the given Job.
        /// </summary>
        /// <remarks>
        /// If there are no matches, a zero-length array should be returned.
        /// </remarks>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IReadOnlyList<IOperableTrigger>> GetTriggersForJob(JobKey jobKey)
        {
            using (var session = _store.OpenAsyncSession())
            {
                try
                {
                    var result = (await session
                        .Query<Trigger, TriggerIndex>()
                        .Where(t => Equals(t.JobName, jobKey.Name) && Equals(t.Group, jobKey.Group))
                        .ToListAsync())
                        .Select(trigger => trigger.Deserialize()).ToList();
                    return result;
                }
                catch(NullReferenceException)
                {
                    return new List<IOperableTrigger>();
                }
            }
        }

        /// <summary>
        /// Get the current state of the identified <see cref="ITrigger" />.
        /// </summary>
        /// <seealso cref="TriggerState" />
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<TriggerState> GetTriggerState(TriggerKey triggerKey)
        {
            Trigger trigger;
            using (var session = _store.OpenAsyncSession())
            {
                trigger = await session.LoadAsync<Trigger>(triggerKey.Name + "/" + triggerKey.Group);
            }

            if (trigger == null)
            {
                return TriggerState.None;
            }

            switch (trigger.State)
            {
                case InternalTriggerState.Complete:
                    return TriggerState.Complete;
                case InternalTriggerState.Paused:
                    return TriggerState.Paused;
                case InternalTriggerState.PausedAndBlocked:
                    return TriggerState.Paused;
                case InternalTriggerState.Blocked:
                    return TriggerState.Blocked;
                case InternalTriggerState.Error:
                    return TriggerState.Error;
                default:
                    return TriggerState.Normal;
            }
        }

        /// <summary>
        /// Pause the <see cref="ITrigger" /> with the given key.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task PauseTrigger(TriggerKey triggerKey)
        {
            if (!await CheckExists(triggerKey))
            {
                return;
            }

            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });
                var trig = await session.LoadAsync<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                // if the trigger doesn't exist or is "complete" pausing it does not make sense...
                if (trig == null)
                {
                    return;
                }
                if (trig.State == InternalTriggerState.Complete)
                {
                    return;
                }

                trig.State = trig.State == InternalTriggerState.Blocked ? InternalTriggerState.PausedAndBlocked : InternalTriggerState.Paused;
                await session.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Pause all of the <see cref="ITrigger" />s in the
        /// given group.
        /// </summary>
        /// <remarks>
        /// The JobStore should "remember" that the group is paused, and impose the
        /// pause on any new triggers that are added to the group while the group is
        /// paused.
        /// </remarks>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<ISet<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            var pausedGroups = new HashSet<string>();

            var triggerKeysForMatchedGroup = await GetTriggerKeys(matcher);
            foreach (var triggerKey in triggerKeysForMatchedGroup)
            {
                await PauseTrigger(triggerKey);
                pausedGroups.Add(triggerKey.Group);
            }
            return new HashSet<string>(pausedGroups);
        }

        /// <summary>
        /// Pause the <see cref="IJob" /> with the given key - by
        /// pausing all of its current <see cref="ITrigger" />s.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task PauseJob(JobKey jobKey)
        {
            var triggersForJob = await GetTriggersForJob(jobKey);
            foreach (var trigger in triggersForJob)
            {
                await PauseTrigger(trigger.Key);
            }
        }

        /// <summary>
        /// Pause all of the <see cref="IJob" />s in the given
        /// group - by pausing all of their <see cref="ITrigger" />s.
        /// <para>
        /// The JobStore should "remember" that the group is paused, and impose the
        /// pause on any new jobs that are added to the group while the group is
        /// paused.
        /// </para>
        /// </summary>
        /// <seealso cref="string">
        /// </seealso>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IReadOnlyList<string>> PauseJobs(GroupMatcher<JobKey> matcher)
        {

            var pausedGroups = new List<string>();

            var jobKeysForMatchedGroup = await GetJobKeys(matcher);
            foreach (var jobKey in jobKeysForMatchedGroup)
            {
                await PauseJob(jobKey);
                pausedGroups.Add(jobKey.Group);

                using (var session = _store.OpenAsyncSession())
                {
                    var sched = await session.LoadAsync<Scheduler>(InstanceName);
                    sched.PausedJobGroups.Add(matcher.CompareToValue);
                    await session.SaveChangesAsync();
                }
            }

            return pausedGroups;
        }

        /// <summary>
        /// Resume the <see cref="ITrigger" /> with the
        /// given key.
        /// 
        /// <para>
        /// If the <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="string">
        /// </seealso>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task ResumeTrigger(TriggerKey triggerKey)
        {
            if (!await CheckExists(triggerKey))
            {
                return;
            }
            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });
                var trigger = await session.LoadAsync<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                // if the trigger is not paused resuming it does not make sense...
                if (trigger.State != InternalTriggerState.Paused && trigger.State != InternalTriggerState.PausedAndBlocked)
                    return;
                
                trigger.State = (await GetBlockedJobs()).Contains(trigger.JobKey) ? InternalTriggerState.Blocked : InternalTriggerState.Waiting;

                await ApplyMisfire(trigger);

                await session.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Resume all of the <see cref="ITrigger" />s
        /// in the given group.
        /// <para>
        /// If any <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IReadOnlyList<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            ISet<string> resumedGroups = new HashSet<string>();
            ISet<TriggerKey> keys = await GetTriggerKeys(matcher);

            foreach (TriggerKey triggerKey in keys)
            {
                await ResumeTrigger(triggerKey);
                resumedGroups.Add(triggerKey.Group);
            }

            return new List<string>(resumedGroups);
        }

        /// <summary>
        /// Gets the paused trigger groups.
        /// </summary>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<ISet<string>> GetPausedTriggerGroups()
        {
            using (var session = _store.OpenAsyncSession())
            {
                await Task.FromResult(0);
                return new HashSet<string>(
                    (await session.Query<Trigger, TriggerIndex>()
                        .Where(t => t.State == InternalTriggerState.Paused || t.State == InternalTriggerState.PausedAndBlocked)
                        .Distinct()
                        .Select(t => t.Group).ToListAsync())
                        .ToHashSet()
                );
            }
        }

        /// <summary>
        /// Gets the paused job groups.
        /// </summary>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<ISet<string>> GetPausedJobGroups()
        {
            using (var session = _store.OpenAsyncSession())
            {
                return (await session.LoadAsync<Scheduler>(InstanceName)).PausedJobGroups;
            }
        }

        /// <summary>
        /// Gets the blocked jobs set.
        /// </summary>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<ISet<string>> GetBlockedJobs()
        {
            using (var session = _store.OpenAsyncSession())
            {
                return (await session.LoadAsync<Scheduler>(InstanceName)).BlockedJobs;
            }
        }

        /// <summary> 
        /// Resume the <see cref="IJob" /> with the
        /// given key.
        /// <para>
        /// If any of the <see cref="IJob" />'s<see cref="ITrigger" /> s missed one
        /// or more fire-times, then the <see cref="ITrigger" />'s misfire
        /// instruction will be applied.
        /// </para>
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task ResumeJob(JobKey jobKey)
        {
            var triggersForJob = await GetTriggersForJob(jobKey);
            foreach (var trigger in triggersForJob)
            {
                await ResumeTrigger(trigger.Key);
            }
        }

        /// <summary>
        /// Resume all of the <see cref="IJob" />s in
        /// the given group.
        /// <para>
        /// If any of the <see cref="IJob" /> s had <see cref="ITrigger" /> s that
        /// missed one or more fire-times, then the <see cref="ITrigger" />'s
        /// misfire instruction will be applied.
        /// </para> 
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<ISet<string>> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            ISet<string> resumedGroups = new HashSet<string>();

            ISet<JobKey> keys = await GetJobKeys(matcher);

            using (var session = _store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName);

                foreach (var pausedJobGroup in sched.PausedJobGroups)
                {
                    if (matcher.CompareWithOperator.Evaluate(pausedJobGroup, matcher.CompareToValue))
                    {
                        resumedGroups.Add(pausedJobGroup);
                    }
                }

                foreach (var resumedGroup in resumedGroups)
                {
                    sched.PausedJobGroups.Remove(resumedGroup);
                }
                await session.SaveChangesAsync();
            }

            foreach (JobKey key in keys)
            {
                var triggers = await GetTriggersForJob(key);
                foreach (var trigger in triggers)
                {
                    await ResumeTrigger(trigger.Key);
                }
            }

            return resumedGroups;
        }

        /// <summary>
        /// Pause all triggers - equivalent of calling <see cref="PauseTriggers" />
        /// on every group.
        /// <para>
        /// When <see cref="ResumeAll" /> is called (to resume), trigger misfire
        /// instructions WILL be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="ResumeAll" />
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task PauseAll()
        {
            var triggerGroupNames = await GetTriggerGroupNames();

            foreach (var groupName in triggerGroupNames)
            {
                await PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
            }
        }

        /// <summary>
        /// Resume all triggers - equivalent of calling <see cref="ResumeTriggers" />
        /// on every group.
        /// <para>
        /// If any <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </para>
        /// 
        /// </summary>
        /// <seealso cref="PauseAll" />
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task ResumeAll()
        {
            using (var session = _store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName);

                sched.PausedJobGroups.Clear();

                var triggerGroupNames = await GetTriggerGroupNames();

                foreach (var groupName in triggerGroupNames)
                {
                    await ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
        }

        protected virtual DateTimeOffset MisfireTime
        {
            //[MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                DateTimeOffset misfireTime = SystemTime.UtcNow();
                if (MisfireThreshold > TimeSpan.Zero)
                {
                    misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
                }

                return misfireTime;
            }
        }

        /// <summary>
        /// Applies the misfire.
        /// </summary>
        /// <param name="trigger">The trigger wrapper.</param>
        /// <returns></returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        protected virtual async Task<bool> ApplyMisfire(Trigger trigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? tnft = trigger.NextFireTimeUtc;
            if (!tnft.HasValue || tnft.Value > misfireTime
                || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = await RetrieveCalendar(trigger.CalendarName);
            }

            // Deserialize to an IOperableTrigger to apply original methods on the trigger
            var trig = trigger.Deserialize();
            await signaler.NotifyTriggerListenersMisfired(trig);
            trig.UpdateAfterMisfire(cal);
            trigger.UpdateFireTimes(trig);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                await signaler.NotifySchedulerListenersFinalized(trig);
                trigger.State = InternalTriggerState.Complete;

            }
            else if (tnft.Equals(trig.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Get a handle to the next trigger to be fired, and mark it as 'reserved'
        /// by the calling scheduler.
        /// </summary>
        /// <param name="noLaterThan">If &gt; 0, the JobStore should only return a Trigger
        /// that will fire no later than the time represented in this value as
        /// milliseconds.</param>
        /// <param name="maxCount"></param>
        /// <param name="timeWindow"></param>
        /// <returns></returns>
        /// <seealso cref="ITrigger">
        /// </seealso>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async virtual Task<IReadOnlyList<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            List<IOperableTrigger> result = new List<IOperableTrigger>();
            ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
            DateTimeOffset? firstAcquiredTriggerFireTime = null;

            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });
                var triggersQuery = await session.Query<Trigger, TriggerIndex>()
                    .Where(t => (t.State == InternalTriggerState.Waiting) && (t.NextFireTimeUtc <= (noLaterThan + timeWindow).UtcDateTime))
                    .OrderBy(t => t.NextFireTimeTicks)
                    .ThenByDescending(t => t.Priority)
                    .ToListAsync();

                var triggers = new SortedSet<Trigger>(triggersQuery, new TriggerComparator());

                while (true)
                {
                    // return empty list if store has no such triggers.
                    if (!triggers.Any())
                    {
                        return result;
                    }

                    var candidateTrigger = triggers.First();
                    if (candidateTrigger == null)
                    {
                        break;
                    }
                    if (!triggers.Remove(candidateTrigger))
                    {
                        break;
                    }
                    if (candidateTrigger.NextFireTimeUtc == null)
                    {
                        continue;
                    }

                    if (await ApplyMisfire(candidateTrigger))
                    {
                        if (candidateTrigger.NextFireTimeUtc != null)
                        {
                            triggers.Add(candidateTrigger);
                        }
                        continue;
                    }

                    if (candidateTrigger.NextFireTimeUtc > noLaterThan + timeWindow)
                    {
                        break;
                    }

                    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                    // put it back into the timeTriggers set and continue to search for next trigger.
                    JobKey jobKey = new JobKey(candidateTrigger.JobName, candidateTrigger.Group);
                    Job job = await session.LoadAsync<Job>(candidateTrigger.JobKey);

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            continue; // go to next trigger in store.
                        }
                        acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                    }

                    candidateTrigger.State = InternalTriggerState.Acquired;
                    candidateTrigger.FireInstanceId = GetFiredTriggerRecordId();

                    result.Add(candidateTrigger.Deserialize());

                    if (firstAcquiredTriggerFireTime == null)
                    {
                        firstAcquiredTriggerFireTime = candidateTrigger.NextFireTimeUtc;
                    }

                    if (result.Count == maxCount)
                    {
                        break;
                    }
                }
                await session.SaveChangesAsync();
            }
            return result;
        }

        /// <summary> 
        /// Inform the <see cref="IJobStore" /> that the scheduler no longer plans to
        /// fire the given <see cref="ITrigger" />, that it had previously acquired
        /// (reserved).
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task ReleaseAcquiredTrigger(IOperableTrigger trig)
        {
            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });
                var trigger = await session.LoadAsync<Trigger>(trig.Key.Name + "/" + trig.Key.Group);
                if ((trigger == null) || (trigger.State != InternalTriggerState.Acquired))
                {
                    return;
                }
                trigger.State = InternalTriggerState.Waiting;
                await session.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> that the scheduler is now firing the
        /// given <see cref="ITrigger" /> (executing its associated <see cref="IJob" />),
        /// that it had previously acquired (reserved).
        /// </summary>
        /// <returns>
        /// May return null if all the triggers or their calendars no longer exist, or
        /// if the trigger was not successfully put into the 'executing'
        /// state.  Preference is to return an empty list if none of the triggers
        /// could be fired.
        /// </returns>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task<IReadOnlyList<TriggerFiredResult>> TriggersFired(IList<IOperableTrigger> triggers)
        {
            List<TriggerFiredResult> results = new List<TriggerFiredResult>();
            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });
                foreach (IOperableTrigger tr in triggers)
                {
                    // was the trigger deleted since being acquired?
                    var trigger = await session.LoadAsync<Trigger>(tr.Key.Name + "/" + tr.Key.Group);

                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    if (trigger?.State != InternalTriggerState.Acquired)
                    {
                        continue;
                    }

                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = await RetrieveCalendar(trigger.CalendarName);
                        if (cal == null)
                        {
                            continue;
                        }
                    }
                    DateTimeOffset? prevFireTime = trigger.PreviousFireTimeUtc;

                    var trig = trigger.Deserialize();
                    trig.Triggered(cal);

                    TriggerFiredBundle bndle = new TriggerFiredBundle(await RetrieveJob(trig.JobKey),
                        trig,
                        cal,
                        false, SystemTime.UtcNow(),
                        trig.GetPreviousFireTimeUtc(), prevFireTime,
                        trig.GetNextFireTimeUtc());

                    IJobDetail job = bndle.JobDetail;

                    trigger.UpdateFireTimes(trig);
                    trigger.State = InternalTriggerState.Waiting;

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        var trigs = session.Query<Trigger, TriggerIndex>()
                            .Where(t => Equals(t.Group, job.Key.Group) && Equals(t.JobName, job.Key.Name));

                        foreach (var t in trigs)
                        {
                            if (t.State == InternalTriggerState.Waiting)
                            {
                                t.State = InternalTriggerState.Blocked;
                            }
                            if (t.State == InternalTriggerState.Paused)
                            {
                                t.State = InternalTriggerState.PausedAndBlocked;
                            }
                        }
                        var sched = await session.LoadAsync<Scheduler>(InstanceName);
                        sched.BlockedJobs.Add(job.Key.Name + "/" + job.Key.Group);
                    }

                    results.Add(new TriggerFiredResult(bndle));
                }
                await session.SaveChangesAsync();
            }
            return results;

        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> that the scheduler has completed the
        /// firing of the given <see cref="ITrigger" /> (and the execution its
        /// associated <see cref="IJob" />), and that the <see cref="JobDataMap" />
        /// in the given <see cref="IJobDetail" /> should be updated if the <see cref="IJob" />
        /// is stateful.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        public async Task TriggeredJobComplete(IOperableTrigger trig, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName, new JobIndex().IndexName });
                var trigger = await session.LoadAsync<Trigger>(trig.Key.Name + "/" + trig.Key.Group);
                var sched = await session.LoadAsync<Scheduler>(InstanceName);

                // It's possible that the job or trigger is null if it was deleted during execution
                var job = await session.LoadAsync<Job>(trig.JobKey.Name + "/" + trig.JobKey.Group);

                if (job != null)
                {
                    if (jobDetail.PersistJobDataAfterExecution)
                    {
                        job.JobDataMap = jobDetail.JobDataMap;

                    }
                    if (job.ConcurrentExecutionDisallowed)
                    {
                        sched.BlockedJobs.Remove(job.Key);

                        var trigs = await session.Query<Trigger, TriggerIndex>()
                            .Where(t => Equals(t.Group, job.Group) && Equals(t.JobName, job.Name))
                            .ToListAsync();

                        foreach (Trigger t in trigs)
                        {
                            var triggerToUpdate = await session.LoadAsync<Trigger>(t.Key);
                            if (t.State == InternalTriggerState.Blocked)
                            {
                                triggerToUpdate.State = InternalTriggerState.Waiting;
                            }
                            if (t.State == InternalTriggerState.PausedAndBlocked)
                            {
                                triggerToUpdate.State = InternalTriggerState.Paused;
                            }
                        }

                        signaler.SignalSchedulingChange(null);
                    }
                }
                else
                {
                    // even if it was deleted, there may be cleanup to do
                    sched.BlockedJobs.Remove(jobDetail.Key.Name + "/" + jobDetail.Key.Group);
                }

                // check for trigger deleted during execution...
                if (trigger != null)
                {
                    if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                    {
                        // Deleting triggers
                        DateTimeOffset? d = trig.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            // double check for possible reschedule within job 
                            // execution, which would cancel the need to delete...
                            d = trigger.NextFireTimeUtc;
                            if (!d.HasValue)
                            {
                                await RemoveTrigger(trig.Key);
                            }
                            else
                            {
                                //Deleting canceled - trigger still active
                            }
                        }
                        else
                        {
                            await RemoveTrigger(trig.Key);
                            signaler.SignalSchedulingChange(null);
                        }
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                    {
                        trigger.State = InternalTriggerState.Complete;
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                    {
                        trigger.State = InternalTriggerState.Error;
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                    {
                        await SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Error);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                    {
                        await SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Complete);
                        signaler.SignalSchedulingChange(null);
                    }
                }
                await session.SaveChangesAsync();
            }
        }


        /// <summary>
        /// Sets the State of all triggers of job to specified State.
        /// </summary>
        //[MethodImpl(MethodImplOptions.Synchronized)]
        protected virtual async Task SetAllTriggersOfJobToState(JobKey jobKey, InternalTriggerState state)
        {
            using (var session = _store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { new TriggerIndex().IndexName });
                var trigs = session.Query<Trigger, TriggerIndex>()
                    .Where(t => Equals(t.Group, jobKey.Group) && Equals(t.JobName, jobKey.Name));

                foreach (var trig in trigs)
                {
                    var triggerToUpdate = await session.LoadAsync<Trigger>(trig.Key);
                    triggerToUpdate.State = state;
                }
                await session.SaveChangesAsync();
            }
        }

        /// <summary> 
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
        {
            //[MethodImpl(MethodImplOptions.Synchronized)]
            get { return misfireThreshold; }
            //[MethodImpl(MethodImplOptions.Synchronized)]
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("MisfireThreshold must be larger than 0");
                }
                misfireThreshold = value;
            }
        }
    }
}