using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.SignalR;
using NotificationApi.Hubs;

namespace NotificationApi.Services;

public class CatEventServiceBusSubscriber : BackgroundService
{
    private readonly ServiceBusClient _serviceBusClient;
    private readonly IHubContext<CatNotificationHub> _hubContext;
    private readonly IConfiguration _configuration;
    private readonly ILogger<CatEventServiceBusSubscriber> _logger;
    private ServiceBusProcessor? _processor;

    public CatEventServiceBusSubscriber(
        ServiceBusClient serviceBusClient,
        IHubContext<CatNotificationHub> hubContext,
        IConfiguration configuration,
        ILogger<CatEventServiceBusSubscriber> logger)
    {
        _serviceBusClient = serviceBusClient;
        _hubContext = hubContext;
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topicName = _configuration["AzureServiceBus:TopicName"] ?? "cat-event-updates-topic";
        var subscriptionName = _configuration["AzureServiceBus:SubscriptionName"] ?? "notification-api-sub";

        _processor = _serviceBusClient.CreateProcessor(topicName, subscriptionName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1
        });

        _processor.ProcessMessageAsync += MessageHandler;
        _processor.ProcessErrorAsync += ErrorHandler;

        _logger.LogInformation("NotificationApi Subscribing to Topic: {Topic}, Subscription: {Sub}", topicName, subscriptionName);
        await _processor.StartProcessingAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }
    }

    private async Task MessageHandler(ProcessMessageEventArgs args)
    {
        try
        {
            var messageBody = args.Message.Body.ToString();
            _logger.LogInformation("NotificationApi received Service Bus message: {Body}", messageBody);

            var summary = JsonSerializer.Deserialize<CatEventSummaryMessage>(messageBody);

            if (summary != null)
            {
                // Broadcast to all clients connected to this Notification API's SignalR Hub
                await _hubContext.Clients.All.SendAsync("ReceiveCatNotification", summary, cancellationToken: args.CancellationToken);
            }

            await args.CompleteMessageAsync(args.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process Service Bus message in NotificationApi.");
            await args.AbandonMessageAsync(args.Message);
        }
    }

    private Task ErrorHandler(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Service Bus error in NotificationApi on {EntityPath}", args.EntityPath);
        return Task.CompletedTask;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_processor != null)
        {
            await _processor.StopProcessingAsync(cancellationToken);
        }
        await base.StopAsync(cancellationToken);
    }
}

public record CatEventSummaryMessage(
    string EventId,
    string Title,
    DateTime CreatedAt
);