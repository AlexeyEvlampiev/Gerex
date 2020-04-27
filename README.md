# Gerex
Reactive extensions for Azure Service Bus .NET client
## Examples
### Processing queue messages
 ```csharp
var connection = new ServiceBusConnection(connectionString);
await connection
                .ProcessMessages(async (message, token) => {/* Processing logics goes here... */})
                .FromQueue(QueueName, ReceiveMode.PeekLock, RetryPolicy.Default)
                .FirstOrDefaultAsync();
 ```               
### Processing subscription messages
 ```csharp
var connection = new ServiceBusConnection(connectionString);
await connection
                .ProcessMessages(async (message, token) => {/* Processing logics goes here... */})
                .FromSubscription(TopicName, SubscriptionName, ReceiveMode.PeekLock)
                .FirstOrDefaultAsync();
