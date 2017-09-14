using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;


// https://docs.microsoft.com/en-us/azure/virtual-machines/windows/extensions-diagnostics-template
// https://docs.microsoft.com/en-us/rest/api/storageservices/query-entities
// https://docs.microsoft.com/en-us/rest/api/storageservices/querying-tables-and-entities
// https://docs.microsoft.com/en-us/rest/api/storageservices/query-timeout-and-pagination
// https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services


namespace QueryDiagnosticLogs
{
    class Program
    {
        static string _storageAccountName = "vmdiagnosticstestdiag587";
        static string _azTableName = "wadmetricspt1mp10dv2s20170907";

        static string _storageServiceVersion = "2015-12-11";
        static byte[] _storageAccountKey = Convert.FromBase64String("*****  STORAGE ACCOUNT KEY  ******");

        static string[] perfCounterNames =
        {
            @"\Memory\Pool Nonpaged Bytes",
            @"\Memory\Available Bytes",
            @"\Memory\Cache Bytes",
            @"\Memory\Committed Bytes",
            @"\Memory\Page Faults/sec",
            //@"\Processor Information(_Total)\% Privileged Time",
            //@"\Processor Information(_Total)\% Processor Time",
            //@"\Processor Information(_Total)\% User Time",
            //@"\Processor Information(_Total)\Processor Frequency",
            //@"\LogicalDisk(_Total)\% Disk Read Time",
            //@"\LogicalDisk(_Total)\% Disk Write Time",
            //@"\LogicalDisk(_Total)\% Free Space",
            //@"\LogicalDisk(_Total)\% Idle Time",
            //@"\LogicalDisk(_Total)\Avg. Disk Queue Length",
            //@"\LogicalDisk(_Total)\Avg. Disk Read Queue Length",
            //@"\LogicalDisk(_Total)\Avg. Disk Write Queue Length",
            //@"\LogicalDisk(_Total)\Avg. Disk sec/Read",
            //@"\LogicalDisk(_Total)\Avg. Disk sec/Transfer",
            //@"\LogicalDisk(_Total)\Avg. Disk sec/Write",
            //@"\LogicalDisk(_Total)\Disk Bytes/sec",
            //@"\LogicalDisk(_Total)\Disk Read Bytes/sec",
            //@"\LogicalDisk(_Total)\Disk Reads/sec",
            //@"\LogicalDisk(_Total)\Disk Transfers/sec",
            //@"\LogicalDisk(_Total)\Disk Write Bytes/sec",
            //@"\LogicalDisk(_Total)\Disk Writes/sec",
            //@"\LogicalDisk(_Total)\Free Megabytes"
        };

        static void Main(string[] args)
        {
            Console.WriteLine("Query Azure Diagnostic Logs from Table storage\n");

            // PartitionKey correspond to one specific VM
            var partitionKey = ":002Fsubscriptions:002F9c7a8343:002D5f8f:002D463a:002Db994:002Dd81fc00090e5:002FresourceGroups:002FVMDiagnostics:002DTest:002Fproviders:002FMicrosoft:002ECompute:002FvirtualMachines:002FVMDiagnostic:002D1";

            string request = string.Empty;

            Stopwatch sw = Stopwatch.StartNew();
            sw.Start();

            try
            {
                /*****************************************************************/
                // Test #1, Retrieve one Performance counter for a specific VM
                //          Using a filter in the query
                //GetOnePerfCounter(partitionKey, true);

                // Test #2, Retrieve all Performance counters (one by one) for a specific VM
                //     ==> Note that this will generate a lot of transactions
                GetAllPerfCounter(partitionKey);

                // Test #3, Retrieve all rows for a specific VM
                //     ==> Note that this will generate a lot of transactions
                //GetAllRowsForPartitionKey(partitionKey);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString()); 
            }

            sw.Stop();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(string.Format(CultureInfo.CurrentCulture, "{0}  Total operation completed. Execution time (s) {1}", DateTime.UtcNow.ToString("u"), sw.Elapsed.TotalSeconds));
            Console.ResetColor();

            Console.WriteLine("Hit Enter to exit...");
            Console.ReadLine();
        }

        private static void GetOnePerfCounter(string partitionKey, bool useDatetime = true)
        {
            string perfCounter = @"\Memory\Pool Nonpaged Bytes";
            string request = string.Empty;
            if (useDatetime)
            {
                request = String.Format(@"https://{0}.table.core.windows.net/{1}()?$filter=PartitionKey eq '{2}' and CounterName eq '{3}' and (TIMESTAMP ge datetime'2017-09-12T15:30:00.000Z' and TIMESTAMP lt datetime'2017-09-12T16:00:00.000Z')", _storageAccountName, _azTableName, partitionKey, perfCounter);
            }
            else
            {
                request = String.Format(@"https://{0}.table.core.windows.net/{1}()?$filter=PartitionKey eq '{2}' and CounterName eq '{3}'", _storageAccountName, _azTableName, partitionKey, perfCounter);
            }

            GetMetrics(request);
        }

        private static void GetAllPerfCounter(string partitionKey, bool useDatetime = true)
        {
            string request = string.Empty;
            foreach (string perfCounter in perfCounterNames)
            {
                Console.WriteLine("---------------------------------------------------------------------");
                Console.WriteLine("{0} - Querying metrics for {1}", DateTime.UtcNow.ToString("u"), perfCounter);

                if (useDatetime)
                {
                    request = String.Format(@"https://{0}.table.core.windows.net/{1}()?$filter=PartitionKey eq '{2}' and CounterName eq '{3}' and (TIMESTAMP ge datetime'2017-09-12T15:30:00.000Z' and TIMESTAMP lt datetime'2017-09-12T16:00:00.000Z')", _storageAccountName, _azTableName, partitionKey, perfCounter);
                }
                else
                {
                    request = String.Format(@"https://{0}.table.core.windows.net/{1}()?$filter=PartitionKey eq '{2}' and CounterName eq '{3}'", _storageAccountName, _azTableName, partitionKey, perfCounter);
                }

                GetMetrics(request);
            }
        }

        private static void GetAllRowsForPartitionKey(string partitionKey)
        {
            string request = String.Format(@"https://{0}.table.core.windows.net/{1}()?$filter=PartitionKey eq '{2}'", _storageAccountName, _azTableName, partitionKey);

            GetMetrics(request);
        }

        public static void GetMetrics(string request)
        {
            Console.WriteLine("{0} - Sending Query...", DateTime.UtcNow.ToString("u"));
            Stopwatch sw = Stopwatch.StartNew();
            sw.Start();

            int totalRows = SubmitQuery(request);

            sw.Stop();
            Console.WriteLine(string.Format("{0}  Total Rows Returned: {1}", DateTime.UtcNow.ToString("u"), totalRows));
            Console.WriteLine(string.Format(CultureInfo.CurrentCulture, "{0}  Operation completed. Execution time (s) {1}", DateTime.UtcNow.ToString("u"), sw.Elapsed.TotalSeconds));
        }

        public static int SubmitQuery(string url, string nextPartitionKey = null, string nextRowKey = null)
        {
            int rowsReturned = 0;

            String urlPath = String.Format(@"{0}{1}{2}",
                                            url,
                                            string.IsNullOrEmpty(nextPartitionKey) ? string.Empty : string.Format("&NextPartitionKey={0}", nextPartitionKey),
                                            string.IsNullOrEmpty(nextRowKey) ? string.Empty : string.Format("&NextRowKey={0}", nextRowKey));

            //Console.WriteLine("{0}{1}", "\n\n", urlPath, "\n\n");
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(urlPath);

            request.ContentLength = 0;
            request.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("R", CultureInfo.InvariantCulture));

            var resource = request.RequestUri.PathAndQuery;
            if (resource.Contains("?"))
            {
                resource = resource.Substring(0, resource.IndexOf("?"));
            }

            string stringToSign = string.Format("{0}\n/{1}{2}",
                    request.Headers["x-ms-date"],
                    _storageAccountName,
                    resource
                );

            var hasher = new HMACSHA256(_storageAccountKey);

            string signedSignature = Convert.ToBase64String(hasher.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
            string authorizationHeader = string.Format("{0} {1}:{2}", "SharedKeyLite", _storageAccountName, signedSignature);
            request.Headers.Add("Authorization", authorizationHeader);

            request.Headers.Add("x-ms-version", _storageServiceVersion);
            request.Accept = "application/json;odata=minimalmetadata";
            //request.Headers.Add("DataServiceVersion", "3.0;NetFx");
            //request.Headers.Add("MaxDataServiceVersion", "3.0;NetFx");

            HttpWebResponse response = (HttpWebResponse)request.GetResponse();

            if (response.StatusCode == HttpStatusCode.OK)
            {
                using (var reader = new StreamReader(response.GetResponseStream()))
                {
                    String responseFromServer = reader.ReadToEnd();
                    //string jSonString = JToken.Parse(responseFromServer).ToString();
                    //Console.WriteLine(jSonString);

                    var test = JObject.Parse(responseFromServer);

                    // How many rows were returned ?
                    JArray items = (JArray)test["value"];
                    if (items != null)
                        rowsReturned += items.Count;
                    else
                        rowsReturned += 1;
                }

                // Find out if the request returned more than 1000 rows
                if (response.Headers["x-ms-continuation-NextPartitionKey"] != null ||
                    response.Headers["x-ms-continuation-NextRowKey"] != null)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("{0} --> Query returned more than the maximum number of rows (1000)", DateTime.UtcNow.ToString("u"));
                    Console.ResetColor();
                    Console.WriteLine("{0}     Querying next page...", DateTime.UtcNow.ToString("u"));
                    //Console.WriteLine("Need to Query again adding NextRowKey={0}", response.Headers["x-ms-continuation-NextRowKey"]);
                    //Console.WriteLine("Need to Query again adding NextPartitionKey={0}&NextRowKey={1}", response.Headers["x-ms-continuation-NextPartitionKey"], response.Headers["x-ms-continuation-NextRowKey"]);

                    // Get the next result page
                    rowsReturned += SubmitQuery(url, response.Headers["x-ms-continuation-NextPartitionKey"], response.Headers["x-ms-continuation-NextRowKey"]);
                }
            }

            return rowsReturned;
        }

        private static string BuildRowKey(string perfCounterName, DateTime date)
        {
            // RowKey is made of the CounterName and Max time ticks minus the time of the beginning of the aggregation period
            //   Ex.: If the sample period started on 2017-09-13T13:14:00.000Z then the calculation would be: 
            //        DateTime.MaxValue.Ticks - (new DateTime(2017, 09, 13, 13, 14, 0, 0, DateTimeKind.Utc).Ticks) = 2518969923599999999
            // So for the following :
            //   CounterName: "\LogicalDisk(_Total)\% Disk Write Time"   ==> Becomes ":005CLogicalDisk:0028:005FTotal:0029:005C:0025:0020Disk:0020Write:0020Time"
            //   Sample TimeStamp: 2017-09-13T13:14:00.000Z
            //var rowKey = "2518969923599999999__:005CLogicalDisk:0028:005FTotal:0029:005C:0025:0020Disk:0020Write:0020Time";
            //              

            var timestamp = DateTime.MaxValue.Ticks - date.Ticks;
            var counter = @"\LogicalDisk(_Total)\% Disk Write Time";
            var counterName = counter.Replace("\\", ":005C").Replace("(", ":0028").Replace("_", ":005F").Replace(")", ":0029").Replace("%", ":0025").Replace(" ", ":0020");
            var rowKey = string.Format("{0}__{1}", timestamp, counterName);

            return rowKey;
        }
    }
}
