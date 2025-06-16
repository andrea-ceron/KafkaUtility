namespace Kafka.Utility.Abstractions.MessageHandlers;

public interface IMessageHandler<TKey, TValue>
    where TKey : class
    where TValue : class
{
    Task OnMessageReceivedAsync(TKey key, TValue message, CancellationToken cancellationToken = default);
}
