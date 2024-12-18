using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;
using System.IO;
using System.Text;
using System.Text.Json;

await using var nc = new NatsClient();

var js = nc.CreateJetStreamContext();
var stream_subject = "streams.>";
await js.CreateStreamAsync(new StreamConfig(name: "STREAM_DEMO", subjects: [stream_subject]));


string ALLOWED_OPTIONS = "123456Q";
Console.WriteLine("Producer Instance");

while(true)
{
    Console.Clear();

    Console.WriteLine("NATS demo producer");
    Console.WriteLine("==================");
    Console.WriteLine("Select mode:");
    Console.WriteLine("1) Pub / Sub");
    Console.WriteLine("2) Wildcards");
    Console.WriteLine("3) Load-balancing (queue groups)");
    Console.WriteLine("4) Request / Response");
    Console.WriteLine("6) Streaming");
    Console.WriteLine("Q) Quit");

    ConsoleKeyInfo input;
    do
    {
        input = Console.ReadKey(true);
    } while (!ALLOWED_OPTIONS.Contains(input.KeyChar));

    switch (input.KeyChar)
    {
        case '1':
            Console.Clear();
            Console.WriteLine("Pub/Sub demo");
            Console.WriteLine("============");

            byte[] d1 = Encoding.UTF8.GetBytes("Message From Producer");
            await nc.PublishAsync("nats.demo.pubsub", d1);
            break;
        case '2':
            Console.Clear();
            Console.WriteLine("Wildcards demo");
            Console.WriteLine("===============");

            Console.WriteLine("Sending to nats.*.wc");

            await nc.PublishAsync("nats.*.wc", "Message From Producer(Wildcard)");
            break;
        case '3':
            Console.Clear();
            Console.WriteLine("Load-balancing demo");
            Console.WriteLine("===================");
            for (int i = 1; i <= 10; i++)
            {
                string message = $"Message {i}";

                Console.WriteLine($"Sending: {message}");

                byte[] d2 = Encoding.UTF8.GetBytes(message);

                await nc.PublishAsync("nats.demo.queuegroups", d2);

                Thread.Sleep(100);
            }
            break;
        case '4':
            Console.Clear();
            Console.WriteLine("Request/Response demo");
            Console.WriteLine("================================");

            for (int i = 1; i <= 10; i++)
            {
                string message = $"Message {i}";

                NatsMsg<string> reply = await nc.RequestAsync<string,string>("nats.demo.requestresponse", message);

                var responseMsg = reply.Data;

                Console.WriteLine($"Response: {responseMsg}");

                Thread.Sleep(100);
            }
            break;
        case '5':
            for (int i = 1; i <= 25; i++)
            {
                string message = $"[{DateTime.Now.ToString("hh:mm:ss:fffffff")}] Message {i}";
                Console.WriteLine($"Sending {message}");
                string subject = stream_subject + $".DATA.{Guid.NewGuid().ToString()}";
                var ack  =await js.PublishAsync(subject, message);
                ack.EnsureSuccess();
            }
            break;

    }

    Console.WriteLine();
    Console.WriteLine("Done. Press any key to continue...");
    Console.ReadKey(true);
    Console.Clear();
}