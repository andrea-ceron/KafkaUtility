using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Kafka.Utility.Abstractions.Clients;

namespace Kafka.Utility.Services;

/// <summary>
/// Classe astratta che implementa un background service per l'invio di messaggi verso kafka
/// </summary>
/// <param name="logger"></param>
/// <param name="serviceProvider"></param>
public abstract class ProducerServiceWithSubscription(ILogger<ProducerServiceWithSubscription> logger, IServiceProvider serviceProvider)
    : BackgroundService
{
    /// <summary>
    /// L'implementatore definisce quando completare il tcs
    /// </summary>
    /// <param name="tcs"></param>
    /// <returns></returns>
    protected abstract IDisposable Subscribe(TaskCompletionSource tcs);

    /// <summary>
    /// Resituisce l'elenco dei topic configurati
    /// </summary>
    /// <returns></returns>
    protected abstract IEnumerable<string> GetTopics();

    /// <summary>
    /// Metodo che effettua la produzione di messaggi verso kafka
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    protected abstract Task OperationsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Invocato quando l'host è pronta a far partire il servizio
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("START ProducerServiceWithSubscription.StartAsync...");

        IAdministatorClient adminClient = serviceProvider.GetRequiredService<IAdministatorClient>();

        foreach (string topic in GetTopics())
        {
            if (!adminClient.TopicExists(topic))
            {
                await adminClient.CreateTopicAsync(topic);
            }
        }

        await base.StartAsync(cancellationToken);
    }

    /// <summary>
    /// Invocato alla chiusura controllata dell'applicazione
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("STOP ProducerServiceWithSubscription");

        await base.StopAsync(cancellationToken);
    }

    /// <summary>
    /// Chiamato all'avvio del background service
    /// </summary>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        await ProducerTask(cts.Token).ContinueWith((task, handler) =>
        {
            if (task.IsFaulted)
            {
                logger.LogCritical(task.Exception, "Errore loop ProducerServiceWithSubscription");
                return Task.CompletedTask;
            }

            return task;
        }, stoppingToken);
    }

    /// <summary>
    /// Loop del producer service: ogni volta che tcs viene marcato come completato
    /// viene effettuata la chiamata a <see cref="ProcessImplAsync"/> che si occupa di
    /// invocare <see cref="OperationsAsync"/>.
    /// Sfruttiamo il <see cref="TaskCompletionSource"/>: al suo interno incapsula un task
    /// la cui unica funzione è attendere il completamento di se stesso tramite le API di
    /// <see cref="TaskCompletionSource"/> da parte di agenti asincroni esterni.
    /// Tramite il metodo astratto Subscribe definiamo quando il tcs dovrà essere marcato come completato.
    /// Questa metodologia di consente di attendere un evento "esterno" prima di effettuare
    /// la chiamata a <see cref="ProcessImplAsync"/>: ovvero, chiameremo tale metodo solo quando
    /// effettivamente necessario, contrariamente all'implementazione del <see cref="ProducerService{TKafkaTopicsOutput}"/>
    /// che effettua la chiamata ogni x minuti
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task ProducerTask(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            //istanziamo un nuovo TaskCompletionSource ad ogni loop del while
            TaskCompletionSource tcs = new();

            // definiamo quando tcs dovrà essere marcato come completao
            using var x = Subscribe(tcs);

            // invochiamo ProcessImplAsync che si occuperà di produrre
            // NB: la invochiamo prima dell'attesa del completamento di tcs
            // in modo tale che al primo avvio venga effettuata una prima chiamata
            // senza attesa (esempio: il servizio è andato giù durante la ProcessImplAsync
            // precedente, per cui sono ancora presenti record da produrre).
            await ProcessImplAsync(cancellationToken);

            // attesa del completamento di tcs
            await tcs.Task;
        }
    }

    /// <summary>
    /// Primitiva che si occupa di invocare l'implementazione di <see cref="OperationsAsync"/>
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task ProcessImplAsync(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("START ProducerServiceWithSubscription.ExecuteTaskAsync...");
        try
        {
            await OperationsAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Exception sollevata all'interno del metodo {methodName}. Exception Message: {message}",
                nameof(ProcessImplAsync), ex.Message);
        }

        logger.LogInformation("STOP ProducerServiceWithSubscription.ExecuteTaskAsync");
    }
}
