using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System;
using System.Text;
using System.Threading.Tasks;

namespace BrokeredMessaging.ChatConsole
{
    class ChatApplication
    {
        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://sb-joechat-dev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Lcz2knx39zxZWqA2/evzIM4CmiThIh/N7WFh1S5XXs0=";

        // Name of your Service Bus topic
        static string topicName = "joe-chat-topic";

        // Name of topic subscription
        //static string subscriptionName = "joe-chat-subscription";

        // the client that owns the connection and can be used to create senders and receivers
        static ServiceBusClient client;

        // the sender used to publish messages to the topic
        static ServiceBusSender sender;

        // the processor that reads and processes messages from the queue
        static ServiceBusProcessor processor;

        private static Task Main(string[] args)
        {
            Console.WriteLine("Enter name:");
            var userName = Console.ReadLine();

            // Create ServiceBus Admin Client to manage artifacts
            ServiceBusAdministrationClient adminClient = new ServiceBusAdministrationClient(connectionString);

            // Create a topic if it does not exist
            if (!adminClient.TopicExistsAsync(topicName).Result)
            {
                var topicOptions = new CreateTopicOptions(topicName)
                {
                    AutoDeleteOnIdle = TimeSpan.FromDays(7),
                    DefaultMessageTimeToLive = TimeSpan.FromDays(2),
                    DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(1),
                    EnableBatchedOperations = true,
                    EnablePartitioning = false,
                    MaxSizeInMegabytes = 2048,
                    RequiresDuplicateDetection = true,
                    UserMetadata = "some metadata"
                };

                topicOptions.AuthorizationRules.Add(new SharedAccessAuthorizationRule("allClaims", new[] { AccessRights.Manage, AccessRights.Send, AccessRights.Listen }));

                adminClient.CreateTopicAsync(topicOptions);
            }

            // Create a subscription to the chat topic for the signed in user
            var subscriptionOptions = new CreateSubscriptionOptions(topicName, userName)
            {
                AutoDeleteOnIdle = TimeSpan.FromDays(7),
                DefaultMessageTimeToLive = TimeSpan.FromDays(2),
                EnableBatchedOperations = true,
                UserMetadata = "some metadata"
            };
            adminClient.CreateSubscriptionAsync(subscriptionOptions);

            // Create clients
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(topicName);

            // Create a processor that we can use to process the messages
            processor = client.CreateProcessor(topicName, userName, new ServiceBusProcessorOptions());
            
            // add handler to process messages
            processor.ProcessMessageAsync += MessageHandler;

            // add handler to process any errors
            processor.ProcessErrorAsync += ErrorHandler;

            // start processing 
            processor.StartProcessingAsync();
            
            // Send a hello message to the chat room
            ServiceBusMessage helloMessage = new ServiceBusMessage(Encoding.UTF8.GetBytes($"{userName}: Has entered the chat room..."));
            helloMessage.Subject = userName;
            sender.SendMessageAsync(helloMessage);

            while (true)
            {
                string textMsg = Console.ReadLine();
                if (textMsg.Equals("exit")) break;

                // Send chat message
                var chatMessage = new ServiceBusMessage(Encoding.UTF8.GetBytes($"{userName}: {textMsg}"));
                chatMessage.Subject = userName;
                sender.SendMessageAsync(chatMessage);
            }

            // Send a message to say you're leaving the chat room
            var exitMessage = new ServiceBusMessage(Encoding.UTF8.GetBytes($"{userName}: Has left the chat room..."));
            sender.SendMessageAsync(exitMessage);

            // Calling DisposeAsync on client types is required to ensure that network
            // resources and other unmanaged objects are properly cleaned up.
            _ = processor.DisposeAsync();
            _ = client.DisposeAsync();

            return null;
        }

        // handle received messages
        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"{body}");

            // complete the message. messages is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}