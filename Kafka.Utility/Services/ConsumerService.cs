using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Kafka.Utility.Abstractions.Clients;
using Kafka.Utility.Abstractions.MessageHandlers;

namespace Kafka.Utility.Services;

public class ConsumerService<TKafkaTopicsInput>(ILogger<ConsumerService<TKafkaTopicsInput>> logger, IConsumerClient<string, string> consumerClient, IAdministatorClient adminClient, IOptions<TKafkaTopicsInput> optionsTopics, IServiceScopeFactory serviceScopeFactory, IMessageHandlerFactory<string, string> messageHandlerFactory) : BackgroundService
    where TKafkaTopicsInput : class, IKafkaTopics {
    
    private readonly IEnumerable<string> _topics = optionsTopics.Value.GetTopics();

    bool _disposedValue;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        logger.LogInformation("START ConsumerService.ExecuteAsync...");

        foreach (string topic in _topics) {
            if (!adminClient.TopicExists(topic)) {
                await adminClient.CreateTopicAsync(topic);
            }
        }

        await consumerClient.ConsumeInLoopAsync(_topics, async msg => {
            await using var scope = serviceScopeFactory.CreateAsyncScope();
            IMessageHandler<string, string> handler = messageHandlerFactory.Create(msg.Topic, scope.ServiceProvider);
            await handler.OnMessageReceivedAsync(msg.Message.Key, msg.Message.Value);
        }, stoppingToken);

        logger.LogInformation("STOP ConsumerService");
    }

    protected virtual void Dispose(bool disposing) {
        if (!_disposedValue) {
            if (disposing) {
                // Eliminare lo stato gestito (oggetti gestiti)
                consumerClient?.Dispose();
                adminClient?.Dispose();
                base.Dispose();
            }

            // Liberare risorse non gestite (oggetti non gestiti) ed eseguire l'override del finalizzatore
            // Impostare campi di grandi dimensioni su Null
            _disposedValue = true;
        }
    }

    // // Eseguire l'override del finalizzatore solo se 'Dispose(bool disposing)' contiene codice per liberare risorse non gestite
    // ~ConsumerClient()
    // {
    //     // Non modificare questo codice. Inserire il codice di pulizia nel metodo 'Dispose(bool disposing)'
    //     Dispose(disposing: false);
    // }

    /// <inheritdoc/>
    public override void Dispose() {
        // Non modificare questo codice. Inserire il codice di pulizia nel metodo 'Dispose(bool disposing)'
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

}

