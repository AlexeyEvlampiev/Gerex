
// ReSharper disable InconsistentNaming
namespace Gerex
{
    using System;
    using System.Reactive.Linq;
    using System.Reactive.Threading.Tasks;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Xunit;
    using static Configurations;

    public class ServiceBusConnection_ProcessMessages_Should
    {
        [Fact]
        public async Task Work()
        {
            var config = Configurations.Instance;
            var connection = new ServiceBusConnection(config.ConnectionString);
            var topic = new TopicClient(connection, TopicName, RetryPolicy.Default);

            await Observable
                .Range(0, 3)
                .Select(i => Encoding.UTF8.GetBytes($"Message {i}"))
                .SelectMany(body => topic.SendAsync(new Message(body)).ToObservable());

            var received = await connection
                .ProcessMessages((message, ct) =>
                {
                    var text = Encoding.UTF8.GetString(message.Body);
                    return Task.FromResult(text);
                })
                .FromSubscription(TopicName, SubscriptionName, ReceiveMode.PeekLock, RetryPolicy.Default)
                .Take(TimeSpan.FromSeconds(1))
                .ToList();


            Assert.Equal(3, received.Count);
            await Observable
                .Range(0, 3)
                .Do(i => Assert.Contains($"Message {i}", received));

        }
    }
}
