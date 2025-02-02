public partial class Program
{
    // For demonstration only. In production, you'd use real key management (KMS, Azure KeyVault, etc.).
    private static readonly byte[] EncryptionKey = "1234567890123456"u8.ToArray(); // 16 bytes for AES-128
}