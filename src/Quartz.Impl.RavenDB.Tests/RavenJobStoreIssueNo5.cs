using System;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Xunit;

namespace Quartz.Impl.RavenDB.Tests
{
    public class RavenJobStoreIssueNo5
    {
        private readonly JobListener listener = new JobListener();

        private readonly ITrigger trigger = TriggerBuilder.Create()
            .WithIdentity("trigger", "g1")
            .StartAt(DateTime.Now - TimeSpan.FromHours(1))
            .WithSimpleSchedule(s => s.WithMisfireHandlingInstructionFireNow())
            .Build();

        private readonly IJobDetail job = JobBuilder.Create<TestJob>()
            .WithIdentity("job", "g1")
            .Build();

        private async Task ScheduleTestJobAndWaitForExecution(IScheduler scheduler)
        {
            scheduler.ListenerManager.AddJobListener(listener);
            await scheduler.Start();
            await scheduler.ScheduleJob(job, trigger);
            while (!await scheduler.CheckExists(job.Key)) ;
            await scheduler.Shutdown(true);
        }

        [Fact]
        public async Task InMemory()
        {
            var scheduler = await StdSchedulerFactory.GetDefaultScheduler();

            await ScheduleTestJobAndWaitForExecution(scheduler);

            Assert.True(listener.WasExecuted);
        }

        [Fact]
        public async Task InRavenDB()
        {
             NameValueCollection properties = new NameValueCollection
            {
                // Normal scheduler properties
                ["quartz.scheduler.instanceName"] = "TestScheduler",
                ["quartz.scheduler.instanceId"] = "instance_one",
                // RavenDB JobStore property
                ["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB"
            };

            ISchedulerFactory sf = new StdSchedulerFactory(properties);
            IScheduler scheduler = await sf.GetScheduler();

            await ScheduleTestJobAndWaitForExecution(scheduler);

            Assert.True(listener.WasExecuted);
        }
        public class TestJob : IJob
        {
            public async Task Execute(IJobExecutionContext context)
            {
                await Task.FromResult(0);
            }
        }

        public class JobListener : IJobListener
        {
            public async Task JobToBeExecuted(IJobExecutionContext context)
            {
                await Task.FromResult(0);
            }

            public async Task JobExecutionVetoed(IJobExecutionContext context)
            {
                await Task.FromResult(0);
            }

            public async Task JobWasExecuted(IJobExecutionContext context, JobExecutionException jobException)
            {
                WasExecuted = true;
                await Task.FromResult(0);
            }

            public bool WasExecuted { get; private set; }

            public string Name => "JobListener";
        }
    }
}
