namespace Gerex
{
    using System;
    using System.Diagnostics;
    using System.Reactive;
    using System.Reactive.Disposables;
    using System.Reactive.Linq;
    using System.Reactive.Threading.Tasks;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;

    public static partial class Extensions
    {
        [DebuggerStepThrough]
        public static IHandlerRegistration<T> ProcessMessages<T>(this ServiceBusConnection self,
            Func<Message, CancellationToken, IObservable<T>> handler)
        {
            if (self == null) throw new ArgumentNullException(nameof(self));
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            return new Builder<T>(self, handler);
        }

        [DebuggerStepThrough]
        public static IHandlerRegistration<Unit> ProcessMessages(this ServiceBusConnection self,
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
        public static IHandlerRegistration<T> ProcessMessages<T>(this ServiceBusConnection self,
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
            private Func<SubscriptionClient> _subscriptionClientFactory;
            private Func<Message, CancellationToken, IObservable<T>> _messageHandler;
            private Func<ExceptionReceivedEventArgs, Task> _errorHandler;
            private readonly MessageHandlerReducedOptions _reducedOptions = new MessageHandlerReducedOptions();


            [DebuggerStepThrough]
            public Builder(
                ServiceBusConnection connection, 
                Func<Message, CancellationToken, IObservable<T>> handler)
            {
                _connection = connection ?? throw new ArgumentNullException(nameof(connection));
                _messageHandler = handler ?? throw new ArgumentNullException(nameof(handler));
            }


            [DebuggerStepThrough]
            ISubscriptionRegistration<T> IHandlerRegistration<T>.FromSubscription(
                string topic, 
                string subscription, 
                ReceiveMode receiveMode,
                RetryPolicy retryPolicy)
            {
                _subscriptionClientFactory = ()=> new SubscriptionClient(_connection, topic, subscription, receiveMode, retryPolicy);
                return this;
            }


            [DebuggerStepThrough]
            public IObservable<T> WithErrorHandler(Func<ExceptionReceivedEventArgs, Task> handler)
            {
                _errorHandler = handler;
                return this;
            }

            [DebuggerStepThrough]
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
                        await _messageHandler
                            .Invoke(message, token)
                            .Do(observer.OnNext)
                            .Do(i=> token.ThrowIfCancellationRequested())
                            .LastOrDefaultAsync();
                    }, options);

   
                return Disposable.Create(() => client.CloseAsync().GetAwaiter().GetResult());
            }
        }
    }
}
