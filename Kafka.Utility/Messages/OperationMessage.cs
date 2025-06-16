using Kafka.Utility.Constants;
using Kafka.Utility.Exceptions;

namespace Kafka.Utility.Messages;

public class OperationMessage<TDto>
{
    /// <summary>
    /// Operazione da eseguire. <br/>
    /// Dominio valori: <see cref="Operations"/>
    /// </summary>
    public required string Operation { get; set; }

    /// <summary>
    /// Dto da elaborare
    /// </summary>
    public required TDto Dto { get; set; }

    /// <summary>
    /// Verifica se il messaggio è valorizzato correttamente
    /// </summary>
    public void CheckMessage() {
        if (string.IsNullOrWhiteSpace(Operation)) {
            throw new MessageException($"La property {nameof(Operation)} non può essere null", nameof(Operation));
        }
        if (!Operations.IsValid(Operation)) {
            throw new MessageException($"La property {nameof(Operation)} contiene un valore non valido '{Operation}'", nameof(Operation));
        }
        if (Dto == null)
        {
            throw new MessageException($"La property {nameof(Dto)} ({typeof(TDto).Name}) non può essere null", nameof(Dto));
        }
    }
}
