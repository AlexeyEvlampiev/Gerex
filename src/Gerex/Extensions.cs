namespace Gerex
{
    using System;

    public static partial class Extensions
    {
        public sealed class MessageHandlerReducedOptions
        {
            public TimeSpan? MaxAutoRenewDuration { get; set; }
            public int? MaxConcurrentCalls { get; set; }
        }
    }
}
