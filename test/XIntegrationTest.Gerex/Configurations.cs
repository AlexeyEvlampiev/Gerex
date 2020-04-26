namespace Gerex
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus.Management;
    using Microsoft.Extensions.Configuration;

    class Configurations
    {
        public const string TopicName = "b3e929ad-a52b-4074-833d-72056d02c61f";
        public const string SubscriptionName = "shared";
        public const string QueueName = "e16f65f6-9207-40fd-8779-ec61f422857a";

        static readonly Lazy<Configurations> _instance = new Lazy<Configurations>(()=> new Configurations());

        public static Configurations Instance => _instance.Value;

        private Configurations()
        {
            var config =
                new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();
            ConnectionString = config["ConnectionString"];
            if (String.IsNullOrWhiteSpace(ConnectionString))
                throw new InvalidOperationException($"Connection string value is missing");

            var management = new ManagementClient(ConnectionString);
            CreateTopicAsync(management).GetAwaiter().GetResult();
            CreateQueueAsync(management).GetAwaiter().GetResult();

        }

        public string ConnectionString { get; }

        private async Task CreateQueueAsync(ManagementClient management)
        {
            if (false == await management.QueueExistsAsync(QueueName))
            {
                await management.CreateQueueAsync(QueueName);
            }
        }

       

        private async Task CreateTopicAsync(ManagementClient management)
        {
            if (false == await management.TopicExistsAsync(TopicName))
            {
                await management.CreateTopicAsync(TopicName);
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
        }


    }
}
