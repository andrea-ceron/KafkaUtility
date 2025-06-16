namespace Kafka.Utility.Abstractions.Clients;

public interface IProducerClient<TKey, TValue> : IDisposable
    where TKey : class
    where TValue : class
{

    /// <summary>
    /// Invio di un <paramref name="message"/> ad un <paramref name="topic"/>; la partizione è determinata dalla configurazione
    /// </summary>
    /// <param name="topic">Topic a cui inviare il messaggio</param>
    /// <param name="message">Messaggio da inviare</param>
    /// <param name="cancellationToken">CancellationToken per terminare l'operazione</param>
    /// <returns></returns>
    Task ProduceAsync(string topic, TKey key, TValue message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Invio di un <paramref name="message"/> ad un <paramref name="topic"/> su una specifica <paramref name="partition"/>
    /// </summary>
    /// <param name="topic">Topic a cui inviare il messaggio</param>
    /// <param name="partition">Partizione del topic</param>
    /// <param name="key">Chiave da inviare</param>
    /// <param name="message">Messaggio da inviare</param>
    /// <param name="cancellationToken">CancellationToken per terminare l'operazione</param>
    /// <returns></returns>
    Task ProduceAsync(string topic, int partition, TKey key, TValue message, CancellationToken cancellationToken = default);

}
