// --------------------------------------------------------------------------------
// Step A: Write PlainText Events
// --------------------------------------------------------------------------------

using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using EventStore.Client;

// Old stream with plaintext events
const string oldStreamName = "BankAccount-1";
// New encrypted stream
const string newStreamName = "BankAccountV2-1";

// 1. Connect to EventStoreDB via gRPC
//    The connection string can be found in the docs. We assume "insecure" local ES DB here.
var settings = EventStoreClientSettings.Create("esdb://localhost:2113?tls=false");
var client = new EventStoreClient(settings);

// 2. STEP A: Write some plain text events to the old stream
await WritePlainTextEvents(client);

// 3. STEP B: Read them back to demonstrate they're in plain text
Console.WriteLine("\nReading old (plaintext) events from old stream:");
await ReadAndPrintPlainTextEvents(client);

// 4. STEP C: Introduce new encryption approach
//            We'll show how we'd write a new, "Encrypted" event going forward
//            But first, let's do a quick demonstration of "live" new event with encryption
await WriteEncryptedEvent(client);

// 5. STEP D: Migrate old plain text events to the new encrypted stream
//            We'll read from old stream, encrypt, and rewrite to new stream
Console.WriteLine("\nMigrating old events to new encrypted stream...");
await MigrateOldEventsToNewStream(client);

// 6. STEP E: Tombstone (delete) the old stream
//            This effectively stops anyone from reading it (or writing to it).
//            It's an "irreversible" operation in EventStore.
//await TombstoneOldStream(client);
//Console.WriteLine($"Old stream '{OldStreamName}' tombstoned.");

// 7. STEP F: Read from the new encrypted stream to confirm
Console.WriteLine("\nReading events from new (encrypted) stream:");
await ReadAndPrintEncryptedStream(client);

Console.WriteLine("\nPlaypen demo completed.");

static async Task WritePlainTextEvents(EventStoreClient client)
{
    var createdEvent = new BankAccountCreated
    {
        TransactionId = Guid.NewGuid().ToString("N"),
        CreatedAt = DateTime.UtcNow
    };

    var stakeholderUpdateEvent = new BankAccountStakeholderDetailsUpdated
    {
        StakeholderName = "Alice",
        Address = "123 Main Street",
        BankAccountNumber = "987654321"
    };

    // Convert to JSON, create EventData objects
    var eventData1 = new EventData(
        Uuid.NewUuid(),
        nameof(BankAccountCreated),
        System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(createdEvent)
    );

    var eventData2 = new EventData(
        Uuid.NewUuid(),
        nameof(BankAccountStakeholderDetailsUpdated),
        System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(stakeholderUpdateEvent)
    );

    // Append to old (plaintext) stream
    await client.AppendToStreamAsync(oldStreamName, StreamState.Any, [eventData1, eventData2]);

    Console.WriteLine($"Wrote plain text events to '{oldStreamName}'.");
}

// --------------------------------------------------------------------------------
// Step B: Read and Print PlainText Events
// --------------------------------------------------------------------------------
static async Task ReadAndPrintPlainTextEvents(EventStoreClient client)
{
    var result = client.ReadStreamAsync(Direction.Forwards, oldStreamName, StreamPosition.Start);
    await foreach (var resolvedEvent in result)
    {
        var eventType = resolvedEvent.Event.EventType;
        var dataJson = Encoding.UTF8.GetString(resolvedEvent.Event.Data.ToArray());
        Console.WriteLine($"EventType: {eventType}, Data: {dataJson}");
    }
}

// --------------------------------------------------------------------------------
// Step C: Introduce a new "Encrypted" event approach
//         This is how we'd write events going forward (instead of plain text).
// --------------------------------------------------------------------------------
static async Task WriteEncryptedEvent(EventStoreClient client)
{
    // Example new stakeholder event but we encrypt the PII fields
    var piidata = new BankAccountStakeholderDetailsUpdatedV2
    {
        EncryptedStakeholderName = Encrypt("Bob"),
        EncryptedAddress = Encrypt("456 Secure Ave"),
        EncryptedBankAccountNumber = Encrypt("123456789")
    };

    var encryptedEventData = new EventData(
        Uuid.NewUuid(),
        nameof(BankAccountStakeholderDetailsUpdatedV2),
        System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(piidata)
    );

    // Write it to the new encrypted stream (in real usage, you'd unify or use the same stream with a versioned event).
    await client.AppendToStreamAsync(newStreamName, StreamState.Any, [encryptedEventData]);

    Console.WriteLine($"Wrote an encrypted event to '{newStreamName}'.");
}

// --------------------------------------------------------------------------------
// Step D: Migrate Old PlainText Events -> New Encrypted Stream
// --------------------------------------------------------------------------------
static async Task MigrateOldEventsToNewStream(EventStoreClient client)
{
    // Read from the old stream
    var readResult = client.ReadStreamAsync(Direction.Forwards, oldStreamName, StreamPosition.Start);

    await foreach (var resolvedEvent in readResult)
    {
        var eventType = resolvedEvent.Event.EventType;
        if (eventType == nameof(BankAccountStakeholderDetailsUpdated))
        {
            // De-serialize old event
            var oldData = System.Text.Json.JsonSerializer
                .Deserialize<BankAccountStakeholderDetailsUpdated>(resolvedEvent.Event.Data.Span);

            // Encrypt relevant fields
            var encryptedEvent = new BankAccountStakeholderDetailsUpdatedV2
            {
                EncryptedStakeholderName = Encrypt(oldData!.StakeholderName),
                EncryptedAddress = Encrypt(oldData.Address),
                EncryptedBankAccountNumber = Encrypt(oldData.BankAccountNumber)
            };

            var newEventData = new EventData(
                Uuid.NewUuid(),
                nameof(BankAccountStakeholderDetailsUpdatedV2),
                System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(encryptedEvent)
            );

            // Append to the new encrypted stream
            await client.AppendToStreamAsync(newStreamName, StreamState.Any, [newEventData]);
        }
        else
        {
            // For demonstration, let's simply copy as-is if it's not containing PII
            // e.g., BankAccountCreated might not have PII

            // We need to convert from EventRecord -> EventData before appending
            var newEventData = new EventData(
                resolvedEvent.Event.EventId,
                resolvedEvent.Event.EventType,
                resolvedEvent.Event.Data,
                resolvedEvent.Event.Metadata
            );

            await client.AppendToStreamAsync(newStreamName, StreamState.Any, [newEventData]);
        }
    }
}

// --------------------------------------------------------------------------------
// Step E: Tombstone Old Stream
// --------------------------------------------------------------------------------
static async Task TombstoneOldStream(EventStoreClient client)
{
    // Tombstone is effectively a "delete" that cannot be reversed or reused.
    await client.TombstoneAsync(oldStreamName, StreamState.Any);
}

// --------------------------------------------------------------------------------
// Step F: Read and Print Encrypted Stream
//         We'll show how to decrypt the relevant fields
// --------------------------------------------------------------------------------
static async Task ReadAndPrintEncryptedStream(EventStoreClient client)
{
    var result = client.ReadStreamAsync(Direction.Forwards, newStreamName, StreamPosition.Start);
    await foreach (var resolvedEvent in result)
    {
        var eventType = resolvedEvent.Event.EventType;
        var dataJson = Encoding.UTF8.GetString(resolvedEvent.Event.Data.ToArray());

        if (eventType == nameof(BankAccountStakeholderDetailsUpdatedV2))
        {
            var encryptedObj = System.Text.Json.JsonSerializer
                .Deserialize<BankAccountStakeholderDetailsUpdatedV2>(resolvedEvent.Event.Data.Span);

            // Decrypt
            var name = Decrypt(encryptedObj!.EncryptedStakeholderName);
            var address = Decrypt(encryptedObj.EncryptedAddress);
            var account = Decrypt(encryptedObj.EncryptedBankAccountNumber);

            Console.WriteLine(
                $"EventType: {eventType}, DecryptedData => Name: {name}, Address: {address}, Account: {account}");
        }
        else
        {
            // For non-PII events, just print raw JSON
            Console.WriteLine($"EventType: {eventType}, Data: {dataJson}");
        }
    }
}

// --------------------------------------------------------------------------------
// Helper: AES Encryption
// --------------------------------------------------------------------------------
static string Encrypt(string plainText)
{
    using var aes = Aes.Create();
    aes.Key = EncryptionKey;
    aes.IV = new byte[16]; // zero IV for demonstration only -- DO NOT USE IN PROD!

    var encryptor = aes.CreateEncryptor(aes.Key, aes.IV);
    var plainBytes = Encoding.UTF8.GetBytes(plainText);
    var cipherBytes = encryptor.TransformFinalBlock(plainBytes, 0, plainBytes.Length);
    return Convert.ToBase64String(cipherBytes);
}

// --------------------------------------------------------------------------------
// Helper: AES Decryption
// --------------------------------------------------------------------------------
static string Decrypt(string cipherText)
{
    using var aes = Aes.Create();
    aes.Key = EncryptionKey;
    aes.IV = new byte[16]; // same zero IV used in Encrypt

    var decryptor = aes.CreateDecryptor(aes.Key, aes.IV);
    var cipherBytes = Convert.FromBase64String(cipherText);
    var plainBytes = decryptor.TransformFinalBlock(cipherBytes, 0, cipherBytes.Length);
    return Encoding.UTF8.GetString(plainBytes);
}

// --------------------------------------------------------------------------------
// Domain/Event Classes
// --------------------------------------------------------------------------------

// Plain text versions
public class BankAccountCreated
{
    public string TransactionId { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class BankAccountStakeholderDetailsUpdated
{
    public string StakeholderName { get; set; }
    public string Address { get; set; }
    public string BankAccountNumber { get; set; }
}

// Encrypted version
public class BankAccountStakeholderDetailsUpdatedV2
{
    public string EncryptedStakeholderName { get; set; }
    public string EncryptedAddress { get; set; }
    public string EncryptedBankAccountNumber { get; set; }
}