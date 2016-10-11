#region License

/* 
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at 
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0 
 *   
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations 
 * under the License.
 * 
 */

#endregion

using System;
using System.Collections.Generic;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Job;
using Quartz.Simpl;
using Quartz.Spi;
using System.Collections.Specialized;
using System.Globalization;
using System.Threading;
using Xunit;
using System.Threading.Tasks;
using Quartz.Impl.RavenDB;
using Raven.Client.Document;
using Microsoft.Extensions.Logging;

namespace Quartz.Impl.RavenDB.Facts
{
    /// <summary>
    /// Unit Facts for RavenJobStore, based on the RAMJobStore Facts with minor changes
    /// (originally submitted by Johannes Zillmann)
    /// </summary>
    public class RavenJobStoreUnitFacts
    {
        private IJobStore fJobStore;
        private JobDetailImpl fJobDetail;
        private SampleSignaler fSignaler;

        public RavenJobStoreUnitFacts()
        {
            
            fJobStore = new RavenJobStore();
            fJobStore.ClearAllSchedulingData();
            Thread.Sleep(1000);
        }

        private async Task InitJobStore()
        {
            fSignaler = new SampleSignaler();
            await fJobStore.Initialize(null, fSignaler);
            await fJobStore.SchedulerStarted();

            fJobDetail = new JobDetailImpl("job1", "jobGroup1", typeof(NoOpJob));
            fJobDetail.Durable = true;
            await fJobStore.StoreJob(fJobDetail, true);
        }

        [Fact]
        public async Task FactAcquireNextTrigger()
        {
            await InitJobStore();

            DateTimeOffset d = DateBuilder.EvenMinuteDateAfterNow();
            IOperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddSeconds(200), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddSeconds(50), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger3 = new SimpleTriggerImpl("trigger1", "triggerGroup2", fJobDetail.Name, fJobDetail.Group, d.AddSeconds(100), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));

            trigger1.ComputeFirstFireTimeUtc(null);
            trigger2.ComputeFirstFireTimeUtc(null);
            trigger3.ComputeFirstFireTimeUtc(null);
            await fJobStore.StoreTrigger(trigger1, false);
            await fJobStore.StoreTrigger(trigger2, false);
            await fJobStore.StoreTrigger(trigger3, false);

            DateTimeOffset firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            Assert.Equal(0, (await fJobStore.AcquireNextTriggers(d.AddMilliseconds(10), 1, TimeSpan.Zero)).Count);
            Assert.Equal(trigger2, (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero))[0]);
            Assert.Equal(trigger3, (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero))[0]);
            Assert.Equal(trigger1, (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero))[0]);
            Assert.Equal(0, (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero)).Count);


            // release trigger3
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
            Assert.Equal(trigger3, (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1)))[0]);
        }

        [Fact]
        public async Task FactAcquireNextTriggerBatch()
        {
            await InitJobStore();

            DateTimeOffset d = DateBuilder.EvenMinuteDateAfterNow();
            
            IOperableTrigger early = new SimpleTriggerImpl("early", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d, d.AddMilliseconds(5), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(200000), d.AddMilliseconds(200005), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(200100), d.AddMilliseconds(200105), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger3 = new SimpleTriggerImpl("trigger3", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(200200), d.AddMilliseconds(200205), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger4 = new SimpleTriggerImpl("trigger4", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(200300), d.AddMilliseconds(200305), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger10 = new SimpleTriggerImpl("trigger10", "triggerGroup2", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(500000), d.AddMilliseconds(700000), 2, TimeSpan.FromSeconds(2));

            early.ComputeFirstFireTimeUtc(null);
            trigger1.ComputeFirstFireTimeUtc(null);
            trigger2.ComputeFirstFireTimeUtc(null);
            trigger3.ComputeFirstFireTimeUtc(null);
            trigger4.ComputeFirstFireTimeUtc(null);
            trigger10.ComputeFirstFireTimeUtc(null);
            await fJobStore.StoreTrigger(early, false);
            await fJobStore.StoreTrigger(trigger1, false);
            await fJobStore.StoreTrigger(trigger2, false);
            await fJobStore.StoreTrigger(trigger3, false);
            await fJobStore.StoreTrigger(trigger4, false);
            await fJobStore.StoreTrigger(trigger10, false);

            DateTimeOffset firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            var acquiredTriggers = await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 4, TimeSpan.FromSeconds(1));
            Assert.Equal(4, acquiredTriggers.Count);
            Assert.Equal(early.Key, acquiredTriggers[0].Key);
            Assert.Equal(trigger1.Key, acquiredTriggers[1].Key);
            Assert.Equal(trigger2.Key, acquiredTriggers[2].Key);
            Assert.Equal(trigger3.Key, acquiredTriggers[3].Key);
            await fJobStore.ReleaseAcquiredTrigger(early);
      		await fJobStore.ReleaseAcquiredTrigger(trigger1);
        	await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
			
            acquiredTriggers = await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 5, TimeSpan.FromMilliseconds(1000));
            Assert.Equal(5, acquiredTriggers.Count);
            Assert.Equal(early.Key, acquiredTriggers[0].Key);
            Assert.Equal(trigger1.Key, acquiredTriggers[1].Key);
            Assert.Equal(trigger2.Key, acquiredTriggers[2].Key);
            Assert.Equal(trigger3.Key, acquiredTriggers[3].Key);
            Assert.Equal(trigger4.Key, acquiredTriggers[4].Key);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
            await fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers = await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 6, TimeSpan.FromSeconds(1));
            Assert.Equal(5, acquiredTriggers.Count);
            Assert.Equal(early.Key, acquiredTriggers[0].Key);
            Assert.Equal(trigger1.Key, acquiredTriggers[1].Key);
            Assert.Equal(trigger2.Key, acquiredTriggers[2].Key);
            Assert.Equal(trigger3.Key, acquiredTriggers[3].Key);
            Assert.Equal(trigger4.Key, acquiredTriggers[4].Key);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
            await fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers = await fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(1), 5, TimeSpan.Zero);
            Assert.Equal(2, acquiredTriggers.Count);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);

            acquiredTriggers = await fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(250), 5, TimeSpan.FromMilliseconds(199));
            Assert.Equal(5, acquiredTriggers.Count);
            await fJobStore.ReleaseAcquiredTrigger(early); 
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
            await fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers = await fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(150), 5, TimeSpan.FromMilliseconds(50L));
            Assert.Equal(4, acquiredTriggers.Count);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
        }

        [Fact]
        public async Task FactTriggerStates()
        {
            await InitJobStore();

            IOperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, DateTimeOffset.Now.AddSeconds(100), DateTimeOffset.Now.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            trigger.ComputeFirstFireTimeUtc(null);
            Assert.Equal(TriggerState.None, await fJobStore.GetTriggerState(trigger.Key));
            await fJobStore.StoreTrigger(trigger, false);
            Assert.Equal(TriggerState.Normal, await fJobStore.GetTriggerState(trigger.Key));

            await fJobStore.PauseTrigger(trigger.Key);
            Assert.Equal(TriggerState.Paused, await fJobStore.GetTriggerState(trigger.Key));

            await fJobStore.ResumeTrigger(trigger.Key);
            Assert.Equal(TriggerState.Normal, await fJobStore.GetTriggerState(trigger.Key));

            trigger = (await fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1)))[0];
            Assert.NotNull(trigger);
            await fJobStore.ReleaseAcquiredTrigger(trigger);
            trigger = (await fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1)))[0];
            Assert.NotNull(trigger);
            Assert.Equal(0, (await fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1))).Count);
        }

        [Fact]
        public async Task FactRemoveCalendarWhenTriggersPresent()
        {
            await InitJobStore();

            // QRTZNET-29

            IOperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, DateTimeOffset.Now.AddSeconds(100), DateTimeOffset.Now.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            trigger.ComputeFirstFireTimeUtc(null);
            ICalendar cal = new MonthlyCalendar();
            await fJobStore.StoreTrigger(trigger, false);
            await fJobStore.StoreCalendar("cal", cal, false, true);

            await fJobStore.RemoveCalendar("cal");
        }

        [Fact]
        public async Task FactStoreTriggerReplacesTrigger()
        {
            await InitJobStore();

            string jobName = "StoreJobReplacesJob";
            string jobGroup = "StoreJobReplacesJobGroup";
            JobDetailImpl detail = new JobDetailImpl(jobName, jobGroup, typeof (NoOpJob));
            await fJobStore.StoreJob(detail, false);

            string trName = "StoreTriggerReplacesTrigger";
            string trGroup = "StoreTriggerReplacesTriggerGroup";
            IOperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, DateTimeOffset.Now);
            tr.JobKey = new JobKey(jobName, jobGroup);
            tr.CalendarName = null;

            await fJobStore.StoreTrigger(tr, false);
            Assert.Equal(tr, await fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup)));

            tr.CalendarName = "NonExistingCalendar";
            await fJobStore.StoreTrigger(tr, true);
            Assert.Equal(tr, await fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup)));
            Assert.Equal(tr.CalendarName, (await fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup))).CalendarName);

            bool exceptionRaised = false;
            try
            {
                await fJobStore.StoreTrigger(tr, false);
            }
            catch (ObjectAlreadyExistsException)
            {
                exceptionRaised = true;
            }
            Assert.True(exceptionRaised, "an attempt to store duplicate trigger succeeded");
        }

        [Fact]
        public async Task PauseJobGroupPausesNewJob()
        {
            await InitJobStore();

            string jobName1 = "PauseJobGroupPausesNewJob";
            string jobName2 = "PauseJobGroupPausesNewJob2";
            string jobGroup = "PauseJobGroupPausesNewJobGroup";
            JobDetailImpl detail = new JobDetailImpl(jobName1, jobGroup, typeof (NoOpJob));
            detail.Durable = true;
            await fJobStore.StoreJob(detail, false);
            await fJobStore.PauseJobs(GroupMatcher<JobKey>.GroupEquals(jobGroup));

            detail = new JobDetailImpl(jobName2, jobGroup, typeof (NoOpJob));
            detail.Durable = true;
            await fJobStore.StoreJob(detail, false);

            string trName = "PauseJobGroupPausesNewJobTrigger";
            string trGroup = "PauseJobGroupPausesNewJobTriggerGroup";
            IOperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, DateTimeOffset.UtcNow);
            tr.JobKey = new JobKey(jobName2, jobGroup);
            await fJobStore.StoreTrigger(tr, false);
            Assert.Equal(TriggerState.Paused, await fJobStore.GetTriggerState(tr.Key));
        }

        [Fact]
        public async Task FactRetrieveJob_NoJobFound()
        {
            await InitJobStore();
            
            IJobDetail job = await fJobStore.RetrieveJob(new JobKey("not", "existing"));
            Assert.Null(job);
        }

        [Fact]
        public async Task FactRetrieveTrigger_NoTriggerFound()
        {
            await InitJobStore();

            IOperableTrigger trigger = await fJobStore.RetrieveTrigger(new TriggerKey("not", "existing"));
            Assert.Null(trigger);
        }

        [Fact]
        public async Task FactStoreAndRetrieveJobs()
        {
            await InitJobStore();

            // Store jobs.
            for (int i = 0; i < 10; i++)
            {
                IJobDetail job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                await fJobStore.StoreJob(job, false);
            }
            // Retrieve jobs.
            for (int i = 0; i < 10; i++)
            {
                JobKey jobKey = JobKey.Create("job" + i);
                IJobDetail storedJob = await fJobStore.RetrieveJob(jobKey);
                Assert.Equal(jobKey, storedJob.Key);
            }
        }

        [Fact]
        public async Task FactStoreAndRetrieveTriggers()
        {
            await InitJobStore();
            
            await fJobStore.SchedulerStarted();

            // Store jobs and triggers.
            for (int i = 0; i < 10; i++)
            {
                IJobDetail job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                await fJobStore.StoreJob(job, true);
                SimpleScheduleBuilder schedule = SimpleScheduleBuilder.Create();
                ITrigger trigger = TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job).Build();
                await fJobStore.StoreTrigger((IOperableTrigger) trigger, true);
            }
            // Retrieve job and trigger.
            for (int i = 0; i < 10; i++)
            {
                JobKey jobKey = JobKey.Create("job" + i);
                IJobDetail storedJob = await fJobStore.RetrieveJob(jobKey);
                Assert.Equal(jobKey, storedJob.Key);

                TriggerKey triggerKey = new TriggerKey("trigger" + i);
                ITrigger storedTrigger = await fJobStore.RetrieveTrigger(triggerKey);
                Assert.Equal(triggerKey, storedTrigger.Key);
            }
        }

        [Fact]
        public async Task FactAcquireTriggers()
        {
            await InitJobStore();

            ISchedulerSignaler schedSignaler = new SampleSignaler();
            ITypeLoadHelper loadHelper = new SimpleTypeLoadHelper();
            loadHelper.Initialize();

            await fJobStore.Initialize(loadHelper, schedSignaler);
            await fJobStore.SchedulerStarted();

            // Setup: Store jobs and triggers.
            DateTime startTime0 = DateTime.UtcNow.AddMinutes(1).ToUniversalTime(); // a min from now.
            for (int i = 0; i < 10; i++)
            {
                DateTime startTime = startTime0.AddMinutes(i*1); // a min apart
                IJobDetail job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                SimpleScheduleBuilder schedule = SimpleScheduleBuilder.RepeatMinutelyForever(2);
                IOperableTrigger trigger = (IOperableTrigger) TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job).StartAt(startTime).Build();

                // Manually trigger the first fire time computation that scheduler would do. Otherwise 
                // the store.acquireNextTriggers() will not work properly.
                DateTimeOffset? fireTime = trigger.ComputeFirstFireTimeUtc(null);
                Assert.Equal(true, fireTime != null);

                await fJobStore.StoreJobAndTrigger(job, trigger);
            }

            // Fact acquire one trigger at a time
            for (int i = 0; i < 10; i++)
            {
                DateTimeOffset noLaterThan = startTime0.AddMinutes(i);
                int maxCount = 1;
                TimeSpan timeWindow = TimeSpan.Zero;
                var triggers = await fJobStore.AcquireNextTriggers(noLaterThan, maxCount, timeWindow);
                Assert.Equal(1, triggers.Count);
                Assert.Equal("trigger" + i, triggers[0].Key.Name);

                // Let's remove the trigger now.
                await fJobStore.RemoveJob(triggers[0].JobKey);
            }
        }

        [Fact]
        public async Task FactAcquireTriggersInBatch()
        {
            await InitJobStore();

            ISchedulerSignaler schedSignaler = new SampleSignaler();
            ITypeLoadHelper loadHelper = new SimpleTypeLoadHelper();
            loadHelper.Initialize();

            await fJobStore.Initialize(loadHelper, schedSignaler);

            // Setup: Store jobs and triggers.
            DateTimeOffset startTime0 = DateTimeOffset.UtcNow.AddMinutes(1); // a min from now.
            for (int i = 0; i < 10; i++)
            {
                DateTimeOffset startTime = startTime0.AddMinutes(i); // a min apart
                IJobDetail job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                SimpleScheduleBuilder schedule = SimpleScheduleBuilder.RepeatMinutelyForever(2);
                IOperableTrigger trigger = (IOperableTrigger) TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job).StartAt(startTime).Build();

                // Manually trigger the first fire time computation that scheduler would do. Otherwise 
                // the store.acquireNextTriggers() will not work properly.
                DateTimeOffset? fireTime = trigger.ComputeFirstFireTimeUtc(null);
                Assert.Equal(true, fireTime != null);

                await fJobStore.StoreJobAndTrigger(job, trigger);
            }

            // Fact acquire batch of triggers at a time
            DateTimeOffset noLaterThan = startTime0.AddMinutes(10);
            int maxCount = 7;
            TimeSpan timeWindow = TimeSpan.FromMinutes(8);
            var triggers = await fJobStore.AcquireNextTriggers(noLaterThan, maxCount, timeWindow);
            Assert.Equal(7, triggers.Count);
            for (int i = 0; i < 7; i++)
            {
                Assert.Equal("trigger" + i, triggers[i].Key.Name);
            }
        }

        [Fact]
        public async Task FactBasicStorageFunctions()
        {
            var sched = await CreateScheduler("FactBasicStorageFunctions", 2);
            await sched.Start();

            // Fact basic storage functions of scheduler...

            IJobDetail job = JobBuilder.Create()
                                       .OfType<FactJob>()
                                       .WithIdentity("j1")
                                       .StoreDurably()
                                       .Build();

            Assert.False(await sched.CheckExists(new JobKey("j1")), "Unexpected existence of job named 'j1'.");

            await sched.AddJob(job, false);

            Assert.True(await sched.CheckExists(new JobKey("j1")), "Expected existence of job named 'j1' but checkExists return false.");

            job = await sched.GetJobDetail(new JobKey("j1"));

            Assert.NotNull(job);

            await sched.DeleteJob(new JobKey("j1"));

            ITrigger trigger = TriggerBuilder.Create()
                                             .WithIdentity("t1")
                                             .ForJob(job)
                                             .StartNow()
                                             .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                                             .Build();

            Assert.False(await sched.CheckExists(new TriggerKey("t1")), "Unexpected existence of trigger named '11'.");

            await sched.ScheduleJob(job, trigger);

            Assert.True(await sched.CheckExists(new TriggerKey("t1")));

            job = await sched.GetJobDetail(new JobKey("j1"));

            Assert.NotNull(job);

            trigger = await sched.GetTrigger(new TriggerKey("t1"));

            Assert.NotNull(trigger);

            job = JobBuilder.Create()
                            .OfType<FactJob>()
                            .WithIdentity("j2", "g1")
                            .Build();

            trigger = TriggerBuilder.Create()
                                    .WithIdentity("t2", "g1")
                                    .ForJob(job)
                                    .StartNow()
                                    .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                                    .Build();

            await sched.ScheduleJob(job, trigger);

            job = JobBuilder.Create()
                            .OfType<FactJob>()
                            .WithIdentity("j3", "g1")
                            .Build();

            trigger = TriggerBuilder.Create()
                                    .WithIdentity("t3", "g1")
                                    .ForJob(job)
                                    .StartNow()
                                    .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                                    .Build();

            await sched.ScheduleJob(job, trigger);


            var jobGroups = await sched.GetJobGroupNames();
            var triggerGroups = await sched.GetTriggerGroupNames();

            Assert.Equal(2, jobGroups.Count);
            Assert.Equal(2, triggerGroups.Count);

            ISet<JobKey> jobKeys = await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
            ISet<TriggerKey> triggerKeys = await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

            Assert.Equal(1, jobKeys.Count);
            Assert.Equal(1, triggerKeys.Count);

            jobKeys = await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
            triggerKeys = await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            Assert.Equal(2, jobKeys.Count);
            Assert.Equal(2, triggerKeys.Count);


            TriggerState s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.Equal(TriggerState.Normal, s);

            await sched.PauseTrigger(new TriggerKey("t2", "g1"));
            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.Equal(TriggerState.Paused, s);

            await sched.ResumeTrigger(new TriggerKey("t2", "g1"));
            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.Equal(TriggerState.Normal, s);

            ISet<string> pausedGroups = await sched.GetPausedTriggerGroups();
            Assert.Equal(0, pausedGroups.Count);

            await sched.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            // Fact that adding a trigger to a paused group causes the new trigger to be paused also... 
            job = JobBuilder.Create()
                            .OfType<FactJob>()
                            .WithIdentity("j4", "g1")
                            .Build();

            trigger = TriggerBuilder.Create()
                                    .WithIdentity("t4", "g1")
                                    .ForJob(job)
                                    .StartNow()
                                    .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                                    .Build();

            await sched.ScheduleJob(job, trigger);

            pausedGroups = await sched.GetPausedTriggerGroups();
            Assert.Equal(1, pausedGroups.Count);

            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.Equal(TriggerState.Paused, s);

            s = await sched.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.Equal(TriggerState.Paused, s);

            await sched.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.Equal(TriggerState.Normal, s);

            s = await sched.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.Equal(TriggerState.Normal, s);

            pausedGroups = await sched.GetPausedTriggerGroups();
            Assert.Equal(0, pausedGroups.Count);


            Assert.False(await sched.UnscheduleJob(new TriggerKey("foasldfksajdflk")));

            Assert.True(await sched.UnscheduleJob(new TriggerKey("t3", "g1")));

            jobKeys = await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
            triggerKeys = await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            Assert.Equal(2, jobKeys.Count); // job should have been deleted also, because it is non-durable
            Assert.Equal(2, triggerKeys.Count);

            Assert.True(await sched.UnscheduleJob(new TriggerKey("t1")), "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
            triggerKeys = await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

            Assert.Equal(1, jobKeys.Count); // job should have been left in place, because it is non-durable
            Assert.Equal(0, triggerKeys.Count);

            await sched.Shutdown();
        }

        private async Task<IScheduler> CreateScheduler(string name, int threadCount)
        {
            NameValueCollection properties = new NameValueCollection
            {
                // Setting some scheduler properties
                ["quartz.scheduler.instanceName"] = name + "Scheduler",
                ["quartz.scheduler.instanceId"] = "AUTO",
                ["quartz.threadPool.threadCount"] = threadCount.ToString(CultureInfo.InvariantCulture),
                ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
                ["quartz.serializer.type"] = "json",
                // Setting RavenDB as the persisted JobStore
                ["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB",
            };

            IScheduler sched = await new StdSchedulerFactory(properties).GetScheduler();
            return sched;
        }

        private const string Barrier = "BARRIER";
        private const string DaFactamps = "DATE_STAMPS";
        [DisallowConcurrentExecution]
        [PersistJobDataAfterExecution]
        public class FactStatefulJob : IJob
        {
            public async Task Execute(IJobExecutionContext context)
            {
                await Task.FromResult(0);
            }
        }

        public class FactJob : IJob
        {
            public async Task Execute(IJobExecutionContext context)
            {
                await Task.FromResult(0);
            }
        }

        private static readonly TimeSpan FactTimeout = TimeSpan.FromSeconds(125);

        public class FactJobWithSync : IJob
        {
            public async Task Execute(IJobExecutionContext context)
            {
                try
                {
                    List<DateTime> jobExecTimestamps = (List<DateTime>) context.Scheduler.Context.Get(DaFactamps);
                    Barrier barrier = (Barrier) context.Scheduler.Context.Get(Barrier);

                    jobExecTimestamps.Add(DateTime.UtcNow);

                    barrier.SignalAndWait(FactTimeout);
                }
                catch (Exception e)
                {
                    Console.Write(e);
                }
            }
        }

        [DisallowConcurrentExecution]
        [PersistJobDataAfterExecution]
        public class FactAnnotatedJob : IJob
        {
            public async Task Execute(IJobExecutionContext context)
            {

            }
        }
        [Fact]
        public async Task FactAbilityToFireImmediatelyWhenStartedBefore()
        {
            List<DateTime> jobExecTimestamps = new List<DateTime>();
            Barrier barrier = new Barrier(2);

            IScheduler sched = await CreateScheduler("FactAbilityToFireImmediatelyWhenStartedBefore", 5);
            sched.Context.Put(Barrier, barrier);
            sched.Context.Put(DaFactamps, jobExecTimestamps);
            await sched.Start();

            IJobDetail job1 = JobBuilder.Create<FactJobWithSync>()
                .WithIdentity("job1")
                .Build();

            ITrigger trigger1 = TriggerBuilder.Create()
                .ForJob(job1)
                .Build();

            DateTime sTime = DateTime.UtcNow;

            await sched.ScheduleJob(job1, trigger1);

            barrier.SignalAndWait(FactTimeout);

            await sched.Shutdown(false);

            DateTime fTime = jobExecTimestamps[0];

            Assert.True(fTime - sTime < TimeSpan.FromSeconds(60));
        }

        [Fact]
        public async Task FactAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob()
        {
            List<DateTime> jobExecTimestamps = new List<DateTime>();
            Barrier barrier = new Barrier(2);

            IScheduler sched = await CreateScheduler("FactAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob", 5);
            await sched.Clear();

            sched.Context.Put(Barrier, barrier);
            sched.Context.Put(DaFactamps, jobExecTimestamps);

            await sched.Start();
            
            IJobDetail job1 = JobBuilder.Create<FactJobWithSync>()
                .WithIdentity("job1").
                StoreDurably().Build();
            await sched.AddJob(job1, false);

            DateTime sTime = DateTime.UtcNow;

            await sched.TriggerJob(job1.Key);

            barrier.SignalAndWait(FactTimeout);

            await sched.Shutdown(false);

            DateTime fTime = jobExecTimestamps[0];

            Assert.True(fTime - sTime < TimeSpan.FromSeconds(60)); // This is dangerously subjective!  but what else to do?
        }

        [Fact]
        public async Task FactAbilityToFireImmediatelyWhenStartedAfter()
        {
            List<DateTime> jobExecTimestamps = new List<DateTime>();

            Barrier barrier = new Barrier(2);

            IScheduler sched = await CreateScheduler("FactAbilityToFireImmediatelyWhenStartedAfter", 5);

            sched.Context.Put(Barrier, barrier);
            sched.Context.Put(DaFactamps, jobExecTimestamps);

            IJobDetail job1 = JobBuilder.Create<FactJobWithSync>().WithIdentity("job1").Build();
            ITrigger trigger1 = TriggerBuilder.Create().ForJob(job1).Build();

            DateTime sTime = DateTime.UtcNow;

            await sched.Start();
            await sched.ScheduleJob(job1, trigger1);

            barrier.SignalAndWait(FactTimeout);

            await sched.Shutdown(false);

            DateTime fTime = jobExecTimestamps[0];

            Assert.True((fTime - sTime < TimeSpan.FromSeconds(60))); // This is dangerously subjective!  but what else to do?
        }

        [Fact]
        public async Task FactScheduleMultipleTriggersForAJob()
        {
            IJobDetail job = JobBuilder.Create<FactJob>().WithIdentity("job1", "group1").Build();
            ITrigger trigger1 = TriggerBuilder.Create()
                .WithIdentity("trigger1", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
                .Build();
            ITrigger trigger2 = TriggerBuilder.Create()
                .WithIdentity("trigger2", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
                .Build();

            ISet<ITrigger> triggersForJob = new HashSet<ITrigger>();
            triggersForJob.Add(trigger1);
            triggersForJob.Add(trigger2);

            IScheduler sched = await CreateScheduler("FactScheduleMultipleTriggersForAJob", 5);
            await sched.Start();


            await sched.ScheduleJob(job, triggersForJob, true);

            var triggersOfJob = await sched.GetTriggersOfJob(job.Key);
            Assert.Equal(2, triggersOfJob.Count);

            await sched.Shutdown(false);
        }

        [Fact]
        public async Task FactDurableStorageFunctions()
        {
            IScheduler sched = await CreateScheduler("FactDurableStorageFunctions", 2);
            await sched.Clear();

            // Fact basic storage functions of scheduler...

            IJobDetail job = JobBuilder.Create<FactJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            Assert.False(await sched.CheckExists(new JobKey("j1")));

            await sched.AddJob(job, false);

            Assert.True(await sched.CheckExists(new JobKey("j1")));

            IJobDetail nonDurableJob = JobBuilder.Create<FactJob>()
                .WithIdentity("j2")
                .Build();

            try
            {
                await sched.AddJob(nonDurableJob, false);
                Assert.True(false);
            }
            catch (SchedulerException)
            {
                Assert.False(await sched.CheckExists(new JobKey("j2")));
            }

            await sched.AddJob(nonDurableJob, false, true);

            Assert.True(await sched.CheckExists(new JobKey("j2")));
        }

        [Fact]
        public async Task FactShutdownWithoutWaitIsUnclean()
        {
            List<DateTime> jobExecTimestamps = new List<DateTime>();
            Barrier barrier = new Barrier(2);
            IScheduler scheduler = await CreateScheduler("FactShutdownWithoutWaitIsUnclean", 8);
            try
            {
                scheduler.Context.Put(Barrier, barrier);
                scheduler.Context.Put(DaFactamps, jobExecTimestamps);
                await scheduler.Start();
                string jobName = Guid.NewGuid().ToString();
                await scheduler.AddJob(JobBuilder.Create<FactJobWithSync>().WithIdentity(jobName).StoreDurably().Build(), false);
                await scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await scheduler.GetCurrentlyExecutingJobs()).Count == 0)
                {
                    Thread.Sleep(50);
                }
            }
            finally
            {
                await scheduler.Shutdown(false);
            }

            barrier.SignalAndWait(FactTimeout);
        }

        [Fact]
        public async Task FactShutdownWithWaitIsClean()
        {
            bool shutdown = false;
            List<DateTime> jobExecTimestamps = new List<DateTime>();
            Barrier barrier = new Barrier(2);
            IScheduler scheduler = await CreateScheduler("FactShutdownWithoutWaitIsUnclean", 8);
            try
            {
                scheduler.Context.Put(Barrier, barrier);
                scheduler.Context.Put(DaFactamps, jobExecTimestamps);
                await scheduler.Start();
                string jobName = Guid.NewGuid().ToString();
                await scheduler.AddJob(JobBuilder.Create<FactJobWithSync>().WithIdentity(jobName).StoreDurably().Build(), false);
                await scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await scheduler.GetCurrentlyExecutingJobs()).Count == 0)
                {
                    Thread.Sleep(50);
                }
            }
            finally
            {
                ThreadStart threadStart = () =>
                                          {
                                              try
                                              {
                                                  scheduler.Shutdown(true);
                                                  shutdown = true;
                                              }
                                              catch (SchedulerException ex)
                                              {
                                                  throw new Exception("exception: " + ex.Message, ex);
                                              }
                                          };

                var t = new Thread(threadStart);
                t.Start();
                Thread.Sleep(1000);
                Assert.False(shutdown);
                barrier.SignalAndWait(FactTimeout);
                t.Join();
            }
        }

        public class SampleSignaler : ISchedulerSignaler
        {
            internal int fMisfireCount = 0;

            public async Task NotifyTriggerListenersMisfired(ITrigger trigger)
            {
                fMisfireCount++;
                await Task.FromResult(0);
            }

            public async Task NotifySchedulerListenersFinalized(ITrigger trigger)
            {
                await Task.FromResult(0);
            }

            public void SignalSchedulingChange(DateTimeOffset? candidateNewNextFireTimeUtc)
            {
            }

            public async Task NotifySchedulerListenersError(string message, SchedulerException jpe)
            {
                await Task.FromResult(0);
            }

            public async Task NotifySchedulerListenersJobDeleted(JobKey jobKey)
            {
                await Task.FromResult(0);
            }
        }
    }
}