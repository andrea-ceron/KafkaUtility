using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Kafka.Utility.Abstractions.Clients;

namespace Kafka.Utility.Services;

public abstract class ProducerService<TKafkaTopicsOutput>(ILogger<ProducerService<TKafkaTopicsOutput>> logger, IProducerClient<string, string> producerClient, IAdministatorClient adminClient, IOptions<TKafkaTopicsOutput> optionsTopics, IOptions<KafkaProducerServiceOptions> optionsProducerService, IServiceScopeFactory serviceScopeFactory) : IHostedService, IDisposable where TKafkaTopicsOutput : class, IKafkaTopics
{
    protected ILogger<ProducerService<TKafkaTopicsOutput>> Logger { get; } = logger;
    protected IProducerClient<string, string> ProducerClient { get; } = producerClient;
    protected IAdministatorClient AdminClient { get; } = adminClient;
    protected IServiceScopeFactory ServiceScopeFactory { get; } = serviceScopeFactory;

    /// <summary>
    /// KafkaTopics di riferimento
    /// </summary>
    protected TKafkaTopicsOutput KafkaTopics { get; } = optionsTopics.Value;

    /// <summary>
    /// Elenco topic da elaborare
    /// </summary>
    protected IEnumerable<string> Topics { get; } = optionsTopics.Value.GetTopics();

    /// <summary>
    /// Numero di secondi attesi per la prima esecuzione del metodo ExecuteTask
    /// </summary>
    protected int DueTime { get; } = optionsProducerService.Value.DelaySeconds;

    /// <summary>
    /// Intervallo di tempo in secondi che indica ogni quanto viene eseguito il metodo ExecuteTask
    /// </summary>
    protected int Period { get; } = optionsProducerService.Value.IntervalSeconds;

    /// <summary>
    /// CancellationTokenSource
    /// </summary>
    protected CancellationTokenSource StoppingCts { get; } = new CancellationTokenSource();

    /// <summary>
    /// <see cref="Task"/> utilizzato per verificare lo stato di esecuzione del metodo <see cref="ProducerService.ExecuteTaskAsync"/>
    /// </summary>
    protected Task ExecutingTask { get; private set; } = Task.CompletedTask;

    protected Timer? TimerTask { get; private set; }

    bool _disposedValue;

    public async Task StartAsync(CancellationToken cancellationToken) {
        Logger.LogInformation("START ProducerService.StartAsync...");

        foreach (string topic in Topics) {
            if (!AdminClient.TopicExists(topic)) {
                await AdminClient.CreateTopicAsync(topic);
            }
        }

        // Attivo il TimerTask per invocare una sola volta il metodo ExecuteTask dopo DueTime secondi di ritardo
        TimerTask = new Timer(ExecuteTask, null, TimeSpan.FromSeconds(DueTime), TimeSpan.FromMilliseconds(Timeout.Infinite));
    }

    private void ExecuteTask(object? state) {
        // Blocco il TimerTask per impedire che il metodo ExecuteTask venga invocato nuovamente prima che sia terminata
        // l'esecuzione del metodo ExecuteTaskAsync
        StopTimer();
        ExecutingTask = ExecuteTaskAsync(StoppingCts.Token);
    }

    private async Task ExecuteTaskAsync(CancellationToken cancellationToken) {
        Logger.LogInformation("START ProducerService.ExecuteTaskAsync...");

        try {

            await OperationsAsync(cancellationToken);

        } catch (Exception ex) {
            Logger.LogError(ex, "Exception sollevata all'interno del metodo {methodName}. Exception Message: {message}",
                nameof(ExecuteTaskAsync), ex.Message);
        }

        Logger.LogInformation("STOP ProducerService.ExecuteTaskAsync");

        ActivateTimer();
    }

    protected abstract Task OperationsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Attivazione del TimerTask
    /// </summary>
    private void ActivateTimer() {
        // Riattivo nuovamente il TimerTask per invocare una sola volta il metodo ExecuteTask dopo che sono trascorsi Period secondi
        TimerTask?.Change(TimeSpan.FromSeconds(Period), TimeSpan.FromMilliseconds(Timeout.Infinite));
    }

    /// <summary>
    /// Spegnimento del TimerTask
    /// </summary>
    private void StopTimer() {
        // Blocco il TimerTask per impedire che il metodo ExecuteTask venga invocato nuovamente
        TimerTask?.Change(Timeout.Infinite, 0);
    }

    public async Task StopAsync(CancellationToken cancellationToken) {
        Logger.LogInformation("ProducerService.StopAsync...");

        StopTimer();

        if (ExecutingTask != null && !ExecutingTask.IsCompleted) {
            try {
                // Signal cancellation to the executing method
                StoppingCts.Cancel();
            } finally {
                // Wait until the task completes or the stop token triggers
                await Task.WhenAny(ExecutingTask, Task.Delay(Timeout.Infinite, cancellationToken));
            }
        }

        Logger.LogInformation("STOP ProducerService");
    }

    protected virtual void Dispose(bool disposing) {
        if (!_disposedValue) {
            if (disposing) {
                // Eliminare lo stato gestito (oggetti gestiti)
                StoppingCts.Cancel();
                StoppingCts.Dispose();
                TimerTask?.Dispose();
                ProducerClient?.Dispose();
                AdminClient?.Dispose();
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
    public void Dispose() {
        // Non modificare questo codice. Inserire il codice di pulizia nel metodo 'Dispose(bool disposing)'
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
