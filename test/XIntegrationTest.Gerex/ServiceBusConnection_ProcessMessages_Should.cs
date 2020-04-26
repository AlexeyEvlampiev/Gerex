
// ReSharper disable InconsistentNaming
namespace Gerex
{
    using System;
    using System.Diagnostics;
    using System.Reactive.Linq;
    using System.Reactive.Threading.Tasks;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Xunit;
    using static Configurations;

    public class ServiceBusConnection_ProcessMessages_Should
    {
        private readonly IObservable<int> _cleanUp;

        public ServiceBusConnection_ProcessMessages_Should()
        {
            ConnectionString = Configurations.Instance.ConnectionString;
            var connection = new ServiceBusConnection(ConnectionString);
       

            _cleanUp = connection
                .ProcessMessages((m, ct) => Task.CompletedTask)
                .FromSubscription(TopicName, SubscriptionName, ReceiveMode.PeekLock, RetryPolicy.Default)
                .Take(TimeSpan.FromMilliseconds(300))
                .Count()
                .Do(count=> Debug.WriteLine($"Deleted {count} messages"))
                .LastOrDefaultAsync();
        }

        public string ConnectionString { get; }


        [Fact]
        public async Task ProcessAll()
        {
            Debug.WriteLine("Cleaning the subscription...");
            await _cleanUp;

            var connection = new ServiceBusConnection(ConnectionString);
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

        [Fact]
        public async Task ObserveUnhandledExceptions()
        {
            Debug.WriteLine("Cleaning the subscription...");
            await _cleanUp;

            var connection = new ServiceBusConnection(ConnectionString);
            var topic = new TopicClient(connection, TopicName, RetryPolicy.Default);

            await topic.SendAsync(new Message(Encoding.UTF8.GetBytes("Hello world")));

            var processing = connection
                .ProcessMessages((message, ct) => throw new Exception("Test error")
                {
                    Data = {["Comment"] = "Test"}
                })
                .FromSubscription(TopicName, SubscriptionName, ReceiveMode.PeekLock, RetryPolicy.Default);


            var exception = await Assert.ThrowsAsync<Exception>(async () => await processing);
            Assert.True(exception.Data.Contains("Comment"));
            Assert.Equal("Test", exception.Data["Comment"]);
        }


        [Fact]
        public async Task DelegateErrorHandlingIfConfigured()
        {
            Debug.WriteLine("Cleaning the subscription...");
            await _cleanUp;

            var connection = new ServiceBusConnection(ConnectionString);
            var topic = new TopicClient(connection, TopicName, RetryPolicy.Default);

            await topic.SendAsync(new Message(Encoding.UTF8.GetBytes("Hello world")));

            await connection
                .ProcessMessages((message, ct) => throw new Exception("Test error")
                {
                    Data = { ["Comment"] = "Test" }
                })
                .FromSubscription(TopicName, SubscriptionName, ReceiveMode.PeekLock, RetryPolicy.Default)
                .WithErrorHandler(args =>
                {
                    Assert.True(args.Exception.Data.Contains("Comment"));
                    Assert.Equal("Test", args.Exception.Data["Comment"]);
                    return Task.CompletedTask;
                })
                .Take(TimeSpan.FromMilliseconds(300))
                .LastOrDefaultAsync();
        }
    }
}
