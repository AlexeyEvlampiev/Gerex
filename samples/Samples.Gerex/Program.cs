using System;

namespace Samples.Gerex
{
    using System.Linq;
    using System.Reactive.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using global::Gerex;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Management;

    class Program
    {
        private const string QueueName = "gerex-sample-queue";
        private const string TopicName = "gerex-sample-topic";
        private const string SubscriptionName = "sample-subscription";

        static async Task<int> Main(string[] args)
        {
            var connectionString = args?.FirstOrDefault() ?? Environment.GetEnvironmentVariable("ConnectionString");
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine("Connection string is required");
                return 1;
            }

            await ProvisionSamplesAsync(connectionString);

            var connection = new ServiceBusConnection(connectionString);
            
            var topicClient = new TopicClient(connection, TopicName, RetryPolicy.Default);
            var queueClient = new QueueClient(connection, QueueName, ReceiveMode.PeekLock, RetryPolicy.Default);


            
            Console.WriteLine("---  Topic/Subscription sample -------------");

            await topicClient
                .SendAsync(new Message(Encoding.UTF8.GetBytes($"This message has been sent to {TopicName}")));
            await connection
                .ProcessMessages(async (message, token) =>
                {
                    var text = Encoding.UTF8.GetString(message.Body);
                    Console.WriteLine(text);
                })
                .FromSubscription(TopicName, SubscriptionName, ReceiveMode.PeekLock)
                .FirstOrDefaultAsync();

            await Observable.Range(0, 3).Do(_ => Console.WriteLine());

            
            Console.WriteLine("---  Queue sample  --------------------------");
            await queueClient
                .SendAsync(new Message(Encoding.UTF8.GetBytes($"This message has been sent to {QueueName}")));
            await connection
                .ProcessMessages(async (message, token) =>
                {
                    var text = Encoding.UTF8.GetString(message.Body);
                    Console.WriteLine(text);
                })
                .FromQueue(QueueName, ReceiveMode.PeekLock, RetryPolicy.Default)
                .FirstOrDefaultAsync();
            return 0;
        }

        private static async Task ProvisionSamplesAsync(string connectionString)
        {
            var management = new ManagementClient(connectionString);

            if (false == await management.TopicExistsAsync(TopicName))
            {
                await management.CreateTopicAsync(new TopicDescription(TopicName)
                {
                    AutoDeleteOnIdle = TimeSpan.FromMinutes(10)
                });
            }

            if (false == await management.SubscriptionExistsAsync(TopicName, SubscriptionName))
            {
                await management.CreateSubscriptionAsync(new SubscriptionDescription(TopicName, SubscriptionName)
                {
                    AutoDeleteOnIdle = TimeSpan.FromMinutes(20),
                    DefaultMessageTimeToLive = TimeSpan.FromMinutes(1),
                    MaxDeliveryCount = 2
                });
            }

            if (false == await management.QueueExistsAsync(QueueName))
            {
                await management.CreateQueueAsync(new QueueDescription(QueueName)
                {
                    AutoDeleteOnIdle = TimeSpan.FromMinutes(20),
                    DefaultMessageTimeToLive = TimeSpan.FromMinutes(1),
                    MaxDeliveryCount = 2
                });
            }
        }
    }
}
