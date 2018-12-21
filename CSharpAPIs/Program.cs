using System;
using Microsoft.ServiceFabric.Client;
using Microsoft.ServiceFabric.Common;
using Microsoft.ServiceFabric.Client.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Globalization;

namespace TestingClientApis
{
    class Program
    {
        static void Main(string[] args)
        {

            string folderPath = @"<FolderPath>";


            string connectionString = File.ReadAllText(folderPath + "connectionString.txt");
            string containerName = File.ReadAllLines(folderPath + "containerName.txt")[0];

            // A simple local test for application with one service and one partition.
            string backupPolicyName = "bp2";
            DateTime startDateTime = DateTime.Now - TimeSpan.FromMinutes(1);

            DateTime endDateTime = DateTime.Now + TimeSpan.FromMinutes(4);
            string[] backupPolicyNameList = new string[] { "bp0", "bp1", "bp2", "bp3" };
            Uri clusterUrl = new Uri(@"http://azureicmspydev.fareast.corp.microsoft.com:19080");
            var client = new ServiceFabricHttpClient(clusterUrl);

            ApplicationName appName = new ApplicationName("fabric:/Stateful1Application");
            string appId = "Stateful1Application";
            string serviceId = "Stateful1Application/Stateful1";
            ServiceName serviceName = new ServiceName("fabric:/Stateful1Application/Stateful1");
            EnableBackupDescription enableBackupDescription = new EnableBackupDescription(backupPolicyName);
            List<string> partitionIdList = new List<string>();
            var serviceInfoList = client.Services.GetServiceInfoListAsync(appId).GetAwaiter().GetResult();
            foreach (var serviceInfo in serviceInfoList.Data)
            {
                Console.WriteLine("{0}: FindingThe partitions for service {1}", DateTime.UtcNow, serviceInfo.Id);
                var servicePartitionInfoList = client.Partitions.GetPartitionInfoListAsync(serviceInfo.Id).GetAwaiter().GetResult();
                foreach (var servicePartitionInfo in servicePartitionInfoList.Data)
                {
                    partitionIdList.Add(servicePartitionInfo.PartitionInformation.Id.ToString());
                }
            }

            PartitionId partitionId = new PartitionId(new Guid(partitionIdList[0]));
            
            try
            {
                // Create a storage account and add the connection string  and container name here.
                Task<BackupProgressInfo> backupProgressInfoTask = client.BackupRestore.GetPartitionBackupProgressAsync(partitionId);

                List<DateTime?> timeSpanList = new List<DateTime?>();
                timeSpanList.Add(DateTime.UtcNow);
                TimeSpan.Parse("12:20:00");
                FrequencyBasedBackupScheduleDescription newBackupScheduleDescription = new FrequencyBasedBackupScheduleDescription(TimeSpan.FromMinutes(1));
                TimeBasedBackupScheduleDescription timebasedBackupSchedule = new TimeBasedBackupScheduleDescription(BackupScheduleFrequencyType.Daily, (IEnumerable<DateTime?>)timeSpanList);
                FileShareBackupStorageDescription backupStorageDescription = new FileShareBackupStorageDescription("\\\\bpc1\\backupPolicy2", "fr1");
                AzureBlobBackupStorageDescription azureBlobStorageDescription = new AzureBlobBackupStorageDescription(connectionString, containerName, "fr2");
                int counter = 0;
                foreach (var bpn in backupPolicyNameList)
                {
                    if (counter % 2 != 0)
                    {
                        BackupPolicyDescription addingBackupPolicyDescription = new BackupPolicyDescription(bpn, true, 10, timebasedBackupSchedule, backupStorageDescription);
                        try
                        {
                            client.BackupRestore.CreateBackupPolicyAsync(addingBackupPolicyDescription).GetAwaiter().GetResult();
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message == "Fabric Backup Policy already exists.")
                            {
                                continue;
                            }
                            else
                            {
                                throw ex;
                            }
                        }
                    }
                    else
                    {
                        BasicRetentionPolicyDescription rtd = new BasicRetentionPolicyDescription(TimeSpan.FromHours(2));
                        BackupPolicyDescription addingBackupPolicyDescription = new BackupPolicyDescription(bpn, true, 10, newBackupScheduleDescription, azureBlobStorageDescription, rtd);
                        try
                        {
                            client.BackupRestore.CreateBackupPolicyAsync(addingBackupPolicyDescription).GetAwaiter().GetResult();
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message == "Fabric Backup Policy already exists.")
                            {
                                continue;
                            }
                            else
                            {
                                throw ex;
                            }
                        }
                    }
                    counter++;
                }


                // Updating bp1 with retention.
                BasicRetentionPolicyDescription rtdnew = new BasicRetentionPolicyDescription(TimeSpan.FromHours(4));

                BackupPolicyDescription addingBackupPolicyDescriptionWithRetention = new BackupPolicyDescription("bp1", true, 10, timebasedBackupSchedule, backupStorageDescription, rtdnew);
                client.BackupRestore.UpdateBackupPolicyAsync(addingBackupPolicyDescriptionWithRetention, "bp1").GetAwaiter().GetResult();

                var backupPolicybp1 = client.BackupRestore.GetBackupPolicyByNameAsync("bp1").GetAwaiter().GetResult();
                Console.WriteLine("backupPolicybp1 is {0}", backupPolicybp1.Name);
                Console.WriteLine("backupPolicybp1's retentiontype is {0}", backupPolicybp1.RetentionPolicy.RetentionPolicyType.ToString());

                var backupPolicylist = client.BackupRestore.GetBackupPolicyListAsync().GetAwaiter().GetResult();
                if (backupPolicylist.Data.Count() != 4)
                {
                    throw new Exception("Backup Policy count should be 4");
                }
                client.BackupRestore.DeleteBackupPolicyAsync("bp1").GetAwaiter().GetResult();
                backupPolicylist = client.BackupRestore.GetBackupPolicyListAsync().GetAwaiter().GetResult();
                if (backupPolicylist.Data.Count() != 3)
                {
                    throw new Exception("Backup Policy count should be 3");
                }
                client.BackupRestore.EnableApplicationBackupAsync(appId, enableBackupDescription).GetAwaiter().GetResult();
                enableBackupDescription = new EnableBackupDescription(backupPolicyNameList[0]);
                client.BackupRestore.EnableServiceBackupAsync(serviceId, enableBackupDescription).GetAwaiter().GetResult();
                enableBackupDescription = new EnableBackupDescription(backupPolicyNameList[2]);
                Task.Delay(TimeSpan.FromMinutes(2));

                var allBackupEnablements = client.BackupRestore.GetAllEntitiesBackedUpByPolicyAsync(backupPolicyName).GetAwaiter().GetResult();
                if (allBackupEnablements.Data.Count() != 2)
                {
                    throw new Exception("Here all the enablements should be 2.");
                }
                client.BackupRestore.EnablePartitionBackupAsync(partitionId, enableBackupDescription).GetAwaiter().GetResult();
                allBackupEnablements = client.BackupRestore.GetAllEntitiesBackedUpByPolicyAsync(backupPolicyName).GetAwaiter().GetResult();
                if (allBackupEnablements.Data.Count() != 3)
                {
                    throw new Exception("Here all the enablements should be 3.");
                }
                System.Threading.Thread.Sleep(20000);
                var getPartitionBackupProgress = client.BackupRestore.GetPartitionBackupProgressAsync(partitionId).GetAwaiter().GetResult();
                Console.WriteLine("BackupState of partitionID {0}, is {1}", partitionId, getPartitionBackupProgress.BackupState);

                var bpZero = client.BackupRestore.GetBackupPolicyByNameAsync("bp3").GetAwaiter().GetResult();
                if (bpZero.Schedule.ScheduleKind != BackupScheduleKind.TimeBased)
                {
                    throw new Exception("Schedule kind must be Time based.");
                }
                var getServiceBackupListAsync = client.BackupRestore.GetServiceBackupListAsync(serviceId).GetAwaiter().GetResult();
                var getPartitionBackupListAsync = client.BackupRestore.GetPartitionBackupListAsync(partitionId).GetAwaiter().GetResult();
                BackupPolicyDescription addingNewBackupPolicyDescription = new BackupPolicyDescription("bp0", true, 20, newBackupScheduleDescription, backupStorageDescription);
                client.BackupRestore.UpdateBackupPolicyAsync(addingNewBackupPolicyDescription, "bp0").GetAwaiter().GetResult();
                bpZero = client.BackupRestore.GetBackupPolicyByNameAsync("bp0").GetAwaiter().GetResult();
                if (bpZero.Schedule.ScheduleKind != BackupScheduleKind.FrequencyBased)
                {
                    throw new Exception("Schedule kind must be frequency based.");
                }
                var getPartitionBackupConfiguationInfo = client.BackupRestore.GetPartitionBackupConfigurationInfoAsync(partitionId).GetAwaiter().GetResult();
                if (getPartitionBackupConfiguationInfo.PolicyName != backupPolicyName)
                {
                    throw new Exception("Configured policy name is not correct.");
                }
                client.BackupRestore.SuspendPartitionBackupAsync(partitionId).GetAwaiter().GetResult();
                var getParititionBackupConfiguationInfo = client.BackupRestore.GetPartitionBackupConfigurationInfoAsync(partitionId).GetAwaiter().GetResult();

                if (getParititionBackupConfiguationInfo.SuspensionInfo.IsSuspended == null || getParititionBackupConfiguationInfo.SuspensionInfo.IsSuspended == false)
                {
                    throw new Exception("The partition is not suspended till now which is not correct.");
                }
                client.BackupRestore.ResumePartitionBackupAsync(partitionId).GetAwaiter().GetResult();
                getParititionBackupConfiguationInfo = client.BackupRestore.GetPartitionBackupConfigurationInfoAsync(partitionId).GetAwaiter().GetResult();

                if (getParititionBackupConfiguationInfo.SuspensionInfo.IsSuspended != null && getParititionBackupConfiguationInfo.SuspensionInfo.IsSuspended == true)
                {
                    throw new Exception("We have resumed the partition backup. but it is not reflected here.");
                }

                client.BackupRestore.SuspendApplicationBackupAsync(appId).GetAwaiter().GetResult();

                var getServiceBackupConfiguationInfo = client.BackupRestore.GetServiceBackupConfigurationInfoAsync(serviceId).GetAwaiter().GetResult();
                if (getServiceBackupConfiguationInfo.Data.Count() != 2)
                {
                    throw new Exception("Expected service backup mapping is 2");
                }
                if (getServiceBackupConfiguationInfo.Data.First().SuspensionInfo.IsSuspended != true)
                {
                    throw new Exception("backup suspension inherited from application level. Which is not working here.");
                }


                var getAppBackupConfiguationInfo = client.BackupRestore.GetApplicationBackupConfigurationInfoAsync(appId).GetAwaiter().GetResult();
                if (getAppBackupConfiguationInfo.Data.Count() != 3)
                {
                    throw new Exception("Expected service backup mapping is 3");
                }
                if (getAppBackupConfiguationInfo.Data.First().SuspensionInfo.IsSuspended != true)
                {
                    throw new Exception("backup suspension inherited from application level. Which is not working here.");
                }
                client.BackupRestore.ResumeApplicationBackupAsync(appId).GetAwaiter().GetResult();


                client.BackupRestore.SuspendServiceBackupAsync(serviceId).GetAwaiter().GetResult();

                getServiceBackupConfiguationInfo = client.BackupRestore.GetServiceBackupConfigurationInfoAsync(serviceId).GetAwaiter().GetResult();
                if (getServiceBackupConfiguationInfo.Data.Count() != 2)
                {
                    throw new Exception("Expected service backup mapping is 2");
                }


                client.BackupRestore.ResumeServiceBackupAsync(serviceId).GetAwaiter().GetResult();
                client.BackupRestore.ResumePartitionBackupAsync(partitionId).GetAwaiter().GetResult();
                DisableBackupDescription disableBackupDescription = new DisableBackupDescription(false);
                client.BackupRestore.DisablePartitionBackupAsync(partitionId).GetAwaiter().GetResult();
                allBackupEnablements = client.BackupRestore.GetAllEntitiesBackedUpByPolicyAsync(backupPolicyName).GetAwaiter().GetResult();
                if (allBackupEnablements.Data.Count() != 2)
                {
                    throw new Exception("Here all the enablements should be 2.");
                }
                ApplicationBackupEntity applicationBackupEntity = new ApplicationBackupEntity(appName);
                GetBackupByStorageQueryDescription getBackupByStorageQueryDescription = new GetBackupByStorageQueryDescription(backupStorageDescription, applicationBackupEntity);
                var getBackupFromBackupLocations = client.BackupRestore.GetBackupsFromBackupLocationAsync(getBackupByStorageQueryDescription).GetAwaiter().GetResult();

                Console.WriteLine("BackupCount {0}", getBackupFromBackupLocations.Data.Count());

                getAppBackupConfiguationInfo = client.BackupRestore.GetApplicationBackupConfigurationInfoAsync(appId).GetAwaiter().GetResult();
                if (getAppBackupConfiguationInfo.Data.First().PolicyName != backupPolicyName)
                {
                    throw new Exception("Configured policy name is not correct.");
                }
                getServiceBackupConfiguationInfo = client.BackupRestore.GetServiceBackupConfigurationInfoAsync(serviceId).GetAwaiter().GetResult();
                client.BackupRestore.DisableServiceBackupAsync(serviceId).GetAwaiter().GetResult();
                var getApplicationBackupList = client.BackupRestore.GetApplicationBackupListAsync(appId, 120).GetAwaiter().GetResult();
                if (getApplicationBackupList.ContinuationToken.Next)
                {
                    throw new Exception("continutionToken received must be null");
                }
                getApplicationBackupList = client.BackupRestore.GetApplicationBackupListAsync(appId, 120, false, null, null, null, 10).GetAwaiter().GetResult();

                if (getApplicationBackupList.Data.Count() > 10)
                {
                    throw new Exception("the count should be less than 10");
                }
                getApplicationBackupList = client.BackupRestore.GetApplicationBackupListAsync(appId, 120, false, startDateTime, endDateTime, null).GetAwaiter().GetResult();
                foreach (var backupEnumeration in getApplicationBackupList.Data)
                {
                    string path = backupEnumeration.BackupLocation;
                    string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(path);
                    DateTime fileDateTime = DateTime.ParseExact(fileNameWithoutExtension + "Z", "yyyy-MM-dd HH.mm.ssZ", CultureInfo.InvariantCulture);
                    if (fileDateTime < startDateTime || fileDateTime > endDateTime)
                    {
                        throw new Exception("DateTime filter is not working");
                    }
                }


                BackupInfo latestBackupInfo = client.BackupRestore.GetPartitionBackupListAsync(partitionId, 60, true).GetAwaiter().GetResult().Data.ToList()[0];
                DateTime partitionReqStartTime = DateTime.UtcNow;
                RestorePartitionDescription restorePartitionDescription = new RestorePartitionDescription(latestBackupInfo.BackupId, latestBackupInfo.BackupLocation);
                client.BackupRestore.RestorePartitionAsync(partitionId, restorePartitionDescription).GetAwaiter().GetResult();

                while (true)
                {
                    var restoreProgress = client.BackupRestore.GetPartitionRestoreProgressAsync(partitionId).GetAwaiter().GetResult();
                    if (restoreProgress.RestoreState == RestoreState.Success || restoreProgress.RestoreState == RestoreState.Failure || restoreProgress.RestoreState == RestoreState.Timeout)
                    {

                        Console.WriteLine(string.Format("{0}: Restore stress for partitions  {1} is completed with the restoreState is {2}", DateTime.UtcNow, partitionId, restoreProgress.RestoreState.ToString()));
                        //Console.WriteLine("{0}: Restore stress for partitions  {1} is completed with the restoreState is {2}", DateTime.UtcNow, partitionId, restoreProgress.RestoreState.ToString());
                        break;
                    }
                    else
                    {
                        if (restoreProgress.RestoreState == RestoreState.Accepted)
                        {
                            Console.WriteLine(string.Format("{0}: restoreState is Accepted of partitionId {1}, so increasing counter {2} and waiting for restore to complete.", DateTime.UtcNow, partitionId, counter));
                            counter++;
                        }
                    }
                    Task.Delay(20000).GetAwaiter().GetResult();
                }

                BackupPartitionDescription bpd = new BackupPartitionDescription(backupStorageDescription);
                client.BackupRestore.BackupPartitionAsync(partitionId, bpd).GetAwaiter().GetResult();
                var backupPartitionProgress = client.BackupRestore.GetPartitionBackupProgressAsync(partitionId).GetAwaiter().GetResult();
                Console.WriteLine("BackupProgress for the partition {0}, is {1}", partitionId.ToString(), backupPartitionProgress.BackupState);


                Console.WriteLine("APIs are working correctly");
            }
            finally
            {
                try
                {
                    DisableBackupDescription disableBackupDescription = new DisableBackupDescription(false);
                    client.BackupRestore.DisableApplicationBackupAsync(appId, 60, disableBackupDescription).GetAwaiter().GetResult();
                }
                catch
                {
                    
                }


                try
                {
                    client.BackupRestore.DisableServiceBackupAsync(serviceId).GetAwaiter().GetResult();
                }
                catch
                {

                }


                try
                {
                    client.BackupRestore.DisablePartitionBackupAsync(partitionId).GetAwaiter().GetResult();
                }
                catch
                {

                }
            }
        }

    }
}
