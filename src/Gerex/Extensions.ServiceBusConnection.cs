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


        public static IHandlerRegistration<T> ProcessMessages<T>(this ServiceBusConnection self,
            Func<Message, CancellationToken, Task<T>> handler)
        {
            if (self == null) throw new ArgumentNullException(nameof(self));
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            return new Builder<T>(self, async (msg, ct) =>
            {
                var result = await handler.Invoke(msg, ct);
                return result;
            });
        }

        public interface IHandlerRegistration<T>
        {

            ISubscriptionRegistration<T> FromSubscription(string topic, string subscription, ReceiveMode receiveMode, RetryPolicy retryPolicy = null);

        }

        public interface ISubscriptionRegistration<T> : IObservable<T>
        {
            IObservable<T> WithErrorHandler(Func<ExceptionReceivedEventArgs, Task> handler);
            IObservable<T> WithOptions(Action<MessageHandlerReducedOptions> config);
        }


        sealed class Builder<T> : ObservableBase<T>, IHandlerRegistration<T>, ISubscriptionRegistration<T>
        {
            private readonly ServiceBusConnection _connection;
            private readonly Func<Message, CancellationToken, Task<T>> _handler;
            private Func<SubscriptionClient> _subscriptionClientFactory;
            private Func<ExceptionReceivedEventArgs, Task> _errorHandler;
            private readonly MessageHandlerReducedOptions _reducedOptions = new MessageHandlerReducedOptions();


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

            IObservable<T> ISubscriptionRegistration<T>.WithOptions(Action<MessageHandlerReducedOptions> config)
            {
                config?.Invoke(_reducedOptions);
                return this;
            }

            protected override IDisposable SubscribeCore(IObserver<T> observer)
            {
                var client = _subscriptionClientFactory.Invoke();
                var options = new MessageHandlerOptions(async args =>
                {
                    if (_errorHandler == null)
                    {
                        observer.OnError(args.Exception);
                        return;
                    }
                    await _errorHandler.Invoke(args);
                });

                options.MaxConcurrentCalls = _reducedOptions.MaxConcurrentCalls.GetValueOrDefault(options.MaxConcurrentCalls);
                options.MaxAutoRenewDuration = _reducedOptions.MaxAutoRenewDuration.GetValueOrDefault(options.MaxAutoRenewDuration);

                client.RegisterMessageHandler(
                    async (message, token) =>
                    {
                        var result = await _handler.Invoke(message, token);
                        observer.OnNext(result);
                    }, options);

   
                return Disposable.Create(() => client.CloseAsync().GetAwaiter().GetResult());
            }
        }
    }
}
