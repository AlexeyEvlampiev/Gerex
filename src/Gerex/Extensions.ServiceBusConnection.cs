namespace Gerex
{
    using System;
    using System.Reactive;
    using System.Reactive.Disposables;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;

    public static partial class Extensions
    {
        public static IHandlerRegistration<Unit> ProcessMessages(this ServiceBusConnection self,
            Func<Message, CancellationToken, Task> handler)
        {
            if (self == null) throw new ArgumentNullException(nameof(self));
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            return new Builder<Unit>(self, async (msg, ct) =>
            {
                await handler.Invoke(msg, ct);
                return Unit.Default;
            });
        }


        public interface IHandlerRegistration<T>
        {

            ISubscriptionRegistration<T> FromSubscription(string topic, string subscription, ReceiveMode receiveMode, RetryPolicy retryPolicy = null);

        }

        public interface ISubscriptionRegistration<T> : IObservable<T>
        {
            IObservable<T> WithErrorHandler(Func<ExceptionReceivedEventArgs, Task> handler);
        }


        sealed class Builder<T> : ObservableBase<T>, IHandlerRegistration<T>, ISubscriptionRegistration<T>
        {
            private readonly ServiceBusConnection _connection;
            private readonly Func<Message, CancellationToken, Task<T>> _handler;
            private Func<SubscriptionClient> _subscriptionClientFactory;
            private Func<ExceptionReceivedEventArgs, Task> _errorHandler;


            internal Builder(ServiceBusConnection connection, Func<Message, CancellationToken, Task<T>> handler)
            {
                _connection = connection;
                _handler = handler;
            }

            ISubscriptionRegistration<T> IHandlerRegistration<T>.FromSubscription(
                string topic, 
                string subscription, 
                ReceiveMode receiveMode,
                RetryPolicy retryPolicy)
            {
                _subscriptionClientFactory = ()=> new SubscriptionClient(_connection, topic, subscription, receiveMode, retryPolicy);
                return this;
            }



            public IObservable<T> WithErrorHandler(Func<ExceptionReceivedEventArgs, Task> handler)
            {
                _errorHandler = handler;
                return this;
            }

            protected override IDisposable SubscribeCore(IObserver<T> observer)
            {
                var client = _subscriptionClientFactory.Invoke();
                client.RegisterMessageHandler(
                    async (message, token) =>
                    {
                        var result = await _handler.Invoke(message, token);
                        observer.OnNext(result);
                    }, 
                    new MessageHandlerOptions(async args =>
                    {
                        if (_errorHandler == null)
                        {
                            observer.OnError(args.Exception);
                            return;
                        }

                        await _errorHandler.Invoke(args);

                    }));
                return Disposable.Create(() => client.CloseAsync().GetAwaiter().GetResult());
            }
        }
    }
}
