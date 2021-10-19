using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;

namespace BrokeredMessaging.SenderConsole
{
    class SenderConsole
    {
        //ToDo: Enter a valid Service Bus connection string
        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://sb-joechat-dev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Lcz2knx39zxZWqA2/evzIM4CmiThIh/N7WFh1S5XXs0=";

        // name of your Service Bus queue
        static string queueName = "sbq-brokeredmessaging";

        // the client that owns the connection and can be used to create senders and receivers
        static ServiceBusClient client;

        // the sender used to publish messages to the queue
        static ServiceBusSender sender;

        // number of messages to be sent to the queue
        private const int numOfMessages = 3;

        static async Task Main()
        {
            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read regularly.

            // Create the clients that we'll use for sending and processing messages.
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(queueName);

            // create a batch
            using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            for (int i = 1; i <= numOfMessages; i++)
            {
                // try adding a message to the batch
                if (!messageBatch.TryAddMessage(new ServiceBusMessage($"Joe-Chat Message #: {i}")))
                {
                    // if it is too large for the batch
                    throw new Exception($"The Joe-Chat Message #: {i} is too large to fit in the batch.");
                }
            }

            try
            {
                // Use the producer client to send the batch of messages to the Service Bus queue
                await sender.SendMessagesAsync(messageBatch);
                Console.WriteLine($"A batch of {numOfMessages} messages has been published to the queue.");
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }

            Console.WriteLine("Press any key to end the application");
            Console.ReadKey();
        }
    }
}
