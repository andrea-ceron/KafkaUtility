using AutoMapper;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Kafka.Utility.Abstractions.MessageHandlers;
using Kafka.Utility.Constants;
using Kafka.Utility.Messages;

namespace Kafka.Utility.MessageHandlers;

/// <summary>
/// Classe per gestire messaggi di tipo <see cref="OperationMessage{TMessageDto}"/>. <br/>
/// Il DTO <typeparamref name="TMessageDto"/> contenuto nel messaggio viene mappato nel DTO di destinazione <typeparamref name="TDomainDto"/>
/// </summary>
/// <typeparam name="TMessageDto">DTO contenuto nel messaggio <see cref="OperationMessage{TMessageDto}"/></typeparam>
/// <typeparam name="TDomainDto">DTO di destinazione</typeparam>
/// partendo dal <typeparamref name="TDomainDto"/></typeparam>
public abstract class AbstractOperationMessageHandler<TMessageDto, TDomainDto>
    : AbstractOperationMessageHandlerBase<TMessageDto>
    where TMessageDto : class
    where TDomainDto : class
{

    protected IMapper Mapper { get; }
    protected string DomainDtoType { get; }

    protected AbstractOperationMessageHandler(ILogger<AbstractOperationMessageHandler<TMessageDto, TDomainDto>> logger, IMapper mapper) : base(logger)
    {
        Mapper = mapper;
        DomainDtoType = typeof(TDomainDto).Name;
    }

    /// <inheritdoc/>
    protected override async Task InsertAsync(TMessageDto messageDto, CancellationToken cancellationToken = default)
    {
        Logger.LogInformation("Mapping da {messageDtoType} a {domainDtoType} per eseguire l'Insert...", MessageDtoType, DomainDtoType);
        await InsertAsync(Mapper.Map<TDomainDto>(messageDto), cancellationToken);
    }

    /// <inheritdoc/>
    protected override async Task UpdateAsync(TMessageDto messageDto, CancellationToken cancellationToken = default)
    {
        Logger.LogInformation("Mapping da {messageDtoType} a {domainDtoType} per eseguire l'Update...", MessageDtoType, DomainDtoType);
        await UpdateAsync(Mapper.Map<TDomainDto>(messageDto), cancellationToken);
    }

    /// <inheritdoc/>
    protected override async Task DeleteAsync(TMessageDto messageDto, CancellationToken cancellationToken = default)
    {
        Logger.LogInformation("Mapping da {messageDtoType} a {domainDtoType} per eseguire la Delete...", MessageDtoType, DomainDtoType);
        await DeleteAsync(Mapper.Map<TDomainDto>(messageDto), cancellationToken);
    }

    /// <summary>
    /// Insert del model partendo dal <paramref name="domainDto"/>
    /// </summary>
    /// <param name="domainDto"><typeparamref name="TDomainDto"/> utilizzato per inserire il relativo model</param>
    /// <param name="cancellationToken"></param>
    protected abstract Task InsertAsync(TDomainDto domainDto, CancellationToken cancellationToken = default);

    /// <summary>
    /// Update del model partendo dal <paramref name="domainDto"/>
    /// </summary>
    /// <param name="domainDto"><typeparamref name="TDomainDto"/> utilizzato per aggiornare il relativo model</param>
    /// <param name="cancellationToken"></param>
    protected abstract Task UpdateAsync(TDomainDto domainDto, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete del model partendo dal <paramref name="domainDto"/>
    /// </summary>
    /// <param name="domainDto"><typeparamref name="TDomainDto"/> utilizzato per eliminare il relativo model</param>
    /// <param name="cancellationToken"></param>
    protected abstract Task DeleteAsync(TDomainDto domainDto, CancellationToken cancellationToken = default);
}

/// <summary>
/// Classe per gestire messaggi di tipo <see cref="OperationMessage{TValue}"/>.
/// </summary>
/// <typeparam name="TValue">DTO contenuto nel messaggio <see cref="OperationMessage{TValue}"/></typeparam>
/// partendo dal <typeparamref name="TValue"/> contenuto nel messaggio</typeparam>
public abstract class AbstractOperationMessageHandlerBase<TMessageDto> : IMessageHandler<string, string>
{
    protected ILogger<AbstractOperationMessageHandlerBase<TMessageDto>> Logger { get; }
    protected string MessageDtoType { get; }

    protected AbstractOperationMessageHandlerBase(ILogger<AbstractOperationMessageHandlerBase<TMessageDto>> logger)
    {
        Logger = logger;
        MessageDtoType = typeof(string).Name;
    }

    public async Task OnMessageReceivedAsync(string key, string message, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            Logger.LogError("Il messaggio OperationMessage è null o white space e quindi sarà scartato");
            return;
        }

        Logger.LogInformation("Messaggio OperationMessage da elaborare: '{msg}'...", message);

        await Task.Run(async () =>
        {
            #region Deserializzazione e verifica del messaggio
            Logger.LogInformation("Deserializzazione del messaggio OperationMessage con DTO di tipo {MessageDtoType}", MessageDtoType);

            OperationMessage<TMessageDto>? opMsg;
            try
            {
                opMsg = JsonSerializer.Deserialize<OperationMessage<TMessageDto>?>(message);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Si è verificato un errore durante la deserializzazione del messaggio OperationMessage con DTO di tipo {MessageDtoType}: {ex}. Il messaggio sarà quindi scartato", MessageDtoType, ex.ToString());
                return;
            }

            if (opMsg is null)
            {
                Logger.LogError("Il messaggio non è del tipo OperationMessage, sarà quindi scartato");
                return;
            }

            opMsg.CheckMessage();
            #endregion

            #region Operazione da eseguire
            Logger.LogInformation("Esecuzione operazione '{operation}'...", opMsg.Operation);
            switch (opMsg.Operation)
            {
                case Operations.Insert:
                    await InsertAsync(opMsg.Dto, cancellationToken);
                    break;
                case Operations.Update:
                    await UpdateAsync(opMsg.Dto, cancellationToken);
                    break;
                case Operations.Delete:
                    await DeleteAsync(opMsg.Dto, cancellationToken);
                    break;
                default:
                    Logger.LogError("Il campo Operation del messaggio OperationMessage contiene un valore non valido: '{Operation}'", opMsg.Operation);
                    return;
            }
            Logger.LogInformation("Operazione '{operation}' completata con successo", opMsg.Operation);
            #endregion

        }, cancellationToken);
    }

    /// <summary>
    /// Insert del model partendo dal <paramref name="messageDto"/>
    /// </summary>
    /// <param name="messageDto"><typeparamref name="TMessageDto"/> utilizzato per inserire il relativo model</param>
    /// <param name="cancellationToken"></param>
    protected abstract Task InsertAsync(TMessageDto messageDto, CancellationToken cancellationToken = default);

    /// <summary>
    /// Update del model partendo dal <paramref name="messageDto"/>
    /// </summary>
    /// <param name="messageDto"><typeparamref name="TMessageDto"/> utilizzato per aggiornare il relativo model</param>
    /// <param name="cancellationToken"></param>
    protected abstract Task UpdateAsync(TMessageDto messageDto, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete del model partendo dal <paramref name="messageDto"/>
    /// </summary>
    /// <param name="messageDto"><typeparamref name="TMessageDto"/> utilizzato per eliminare il relativo model</param>
    /// <param name="cancellationToken"></param>
    protected abstract Task DeleteAsync(TMessageDto messageDto, CancellationToken cancellationToken = default);

}
