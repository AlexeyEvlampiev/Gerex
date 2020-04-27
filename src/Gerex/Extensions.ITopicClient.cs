namespace Gerex
{
    using System;
    using System.Diagnostics;
    using System.Reactive;
    using System.Reactive.Threading.Tasks;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;

    public static partial class Extensions
    {


        [DebuggerStepThrough]
        public static ITopicHandlerRegistration<Unit> ProcessMessages(this ITopicClient self,
            Func<Message, CancellationToken, Task> handler)
        {
            if (self == null) throw new ArgumentNullException(nameof(self));
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            IObservable<Unit> ProcessAll(Message message, CancellationToken token)
            {
                return handler.Invoke(message, token).ToObservable();
            }

            return new Builder<Unit>(self, ProcessAll);
        }

        [DebuggerStepThrough]
        public static IHandlerRegistration<T> ProcessMessages<T>(this ITopicClient self,
            Func<Message, CancellationToken, Task<T>> handler)
        {
            if (self == null) throw new ArgumentNullException(nameof(self));
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            IObservable<T> ProcessAll(Message message, CancellationToken token)
            {
                return handler.Invoke(message, token).ToObservable();
            }

            return new Builder<T>(self, ProcessAll);
        }

        public static ITopicHandlerRegistration<T> ProcessMessages<T>(this ITopicClient self, 
            Func<Message, CancellationToken, IObservable<T>> handler)
        {
            if (self == null) throw new ArgumentNullException(nameof(self));
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            return new Builder<T>(self, handler);
        }


        public interface ITopicHandlerRegistration<T>
        {
            IReceiverRegistration<T> FromSubscription(string subscription,
                ReceiveMode receiveMode,
                RetryPolicy retryPolicy = null);

        }

        sealed partial class Builder<T> : ITopicHandlerRegistration<T>
        {
            private readonly string _topicName;
            public Builder(ITopicClient client, Func<Message, CancellationToken, IObservable<T>> handler)
                : this(client.ServiceBusConnection, handler)
            {
                _topicName = client.TopicName;
            }

            public IReceiverRegistration<T> FromSubscription(string subscription, ReceiveMode receiveMode, RetryPolicy retryPolicy)
            {
                _receiverClientFactory = () => new SubscriptionClient(_connection, _topicName, subscription, receiveMode, retryPolicy);
                return this;
            }

        }
    }
}
