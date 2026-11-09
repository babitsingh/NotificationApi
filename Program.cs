using NotificationApi.Hubs;
using NotificationApi.Services;
using Azure.Messaging.ServiceBus;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();



// Register Service Bus Client singleton
builder.Services.AddSingleton<ServiceBusClient>(sp =>
{
    var connString = builder.Configuration.GetConnectionString("ServiceBus")!;
    return new ServiceBusClient(connString);
});

// Add Swagger generation
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Register SignalR
builder.Services.AddSignalR();

// Register the Service Bus Subscriber Worker inside NotificationApi
builder.Services.AddHostedService<CatEventServiceBusSubscriber>();

// Enable CORS for frontend clients / Postman connections
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll", policy =>
    {
        policy.AllowAnyHeader()
              .AllowAnyMethod()
              .SetIsOriginAllowed(_ => true)
              .AllowCredentials();
    });
});

var app = builder.Build();
// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    // Enable Swagger middleware
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "NotificationApi v1");
        c.RoutePrefix = string.Empty; // Optional: Makes Swagger UI load directly at root http://localhost:xxxx/
    });
}

app.UseCors("AllowAll");

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

// Expose SignalR hub endpoint in this separate Notification API
app.MapHub<CatNotificationHub>("/hubs/cat-notifications");
app.Run();