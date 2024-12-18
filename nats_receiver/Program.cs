﻿using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;
using System.Threading;

await using var nc = new NatsClient();
var js = nc.CreateJetStreamContext();

var durableConfig = new ConsumerConfig
{
    Name = "durable_processor",
    DurableName = "durable_processor",
};

using var cts = new CancellationTokenSource();

Thread.Sleep(2000);

var consumer = await js.CreateOrUpdateConsumerAsync(stream: "STREAM_DEMO", durableConfig);

Console.WriteLine("Consumer Instance");

//option 1: normal pubsub
var s1 = Task.Run(async () =>
{
    await foreach (var msg in nc.SubscribeAsync<string>(subject: "nats.demo.pubsub", cancellationToken: cts.Token))
    {
        Console.WriteLine($"Received: {msg.Data}");
    }
});

//option 1: normal pubsub
var s2 = Task.Run(async () =>
{
    await foreach (var msg in nc.SubscribeAsync<string>(subject: "nats.*.wc", cancellationToken: cts.Token))
    {
        Console.WriteLine($"Wildcard Received: {msg.Data}");
    }
});

var s3 = Task.Run(async () =>
{
    await foreach (var msg in nc.SubscribeAsync<string>(subject: "nats.demo.queuegroups", queueGroup: "load-balancing-queue"))
    {
        Console.WriteLine($"QG Received: {msg.Data}");
    }
});

var s4 = Task.Run(async () =>
{
    await foreach (var msg in nc.SubscribeAsync<string>(subject: "nats.demo.requestresponse", queueGroup: "request-response-queue"))
    {
        Console.WriteLine($"RR Received: {msg.Data}");
        var replyMessage = $"Reply {msg.Data}";
        await msg.ReplyAsync(replyMessage);
    }
});

//option 5
var s5 = Task.Run(async () =>
{
    // Continuously consume a batch of messages (1000 by default)
    await foreach (var msg in consumer.ConsumeAsync<string>().WithCancellation(cts.Token))
    {
        var data = msg.Data;
        Console.WriteLine($"Stream: {data}");
        await msg.AckAsync();
    }
});


var subscriptionTasks = new List<Task>
{
    s1,
    s2, 
    s3,
    s4,
    s5
};

await Task.WhenAll(subscriptionTasks);
