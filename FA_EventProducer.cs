using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json;
using System.Collections.Generic;


namespace Company.Function
{
    public class FA_EventProducer
    {
        private readonly string EventHubConnectionString = ""; // Add your Event Hub connection string here
        private readonly string CsvFilePath = "C:\\Research\\Dataset\\robotsensors.csv";

        [FunctionName("FA_EventProducer")]
public async Task RunAsync([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer, ILogger log)
{

    var eventHubClient = new EventHubProducerClient(EventHubConnectionString);

    while (true)
    {
        using (var csvFile = new StreamReader(CsvFilePath))
        {
            var headers = csvFile.ReadLine().Split(',');

            while (!csvFile.EndOfStream)
            {
                var line = csvFile.ReadLine().Split(',');
                var data = new Dictionary<string, object>();

                for (int i = 0; i < headers.Length; i++)
                {
                    var columnName = headers[i].Trim();
                    var columnValue = line[i].Trim();

                    data.Add(columnName, columnValue);
                }

                var eventData = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data)));

                var eventDataList = new List<EventData> { eventData };
                await eventHubClient.SendAsync(eventDataList);

                log.LogInformation($"Pushed data: {JsonConvert.SerializeObject(data)}");
                
                await Task.Delay(1000); // Delay for about 1 second before sending the next message
            }
        }

        // Wait for 5 minutes before starting over
        await Task.Delay(TimeSpan.FromMinutes(5));
    }
}


        
    }
}
