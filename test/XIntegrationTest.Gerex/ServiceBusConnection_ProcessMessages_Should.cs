
// ReSharper disable InconsistentNaming
namespace Gerex
{
    using System.Reactive.Linq;
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
            await connection
                .ProcessMessages((message, ct) => Task.CompletedTask)
                .FromSubscription(TopicName, SubscriptionName, ReceiveMode.PeekLock, RetryPolicy.Default)
                .WithErrorHandler(args=> Task.CompletedTask);
        }
    }
}
