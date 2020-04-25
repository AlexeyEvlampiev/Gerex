
// ReSharper disable InconsistentNaming
namespace Gerex
{
    using System.Reactive.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Xunit;

    public class ServiceBusConnection_ProcessMessages_Should
    {
        [Fact]
        public async Task Test1()
        {
            ServiceBusConnection connection = new ServiceBusConnection("");
            await connection
                .ProcessMessages((message, ct) => Task.CompletedTask)
                .FromSubscription("topic", "subscription", ReceiveMode.PeekLock, RetryPolicy.Default)
                .WithErrorHandler(args=> Task.CompletedTask);
        }
    }
}
