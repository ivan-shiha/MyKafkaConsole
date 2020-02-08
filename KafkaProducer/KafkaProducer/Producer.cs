namespace KafkaProducer
{
    using Confluent.Kafka;
    using Confluent.Kafka.Admin;
    using Microsoft.Extensions.Configuration;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class Producer
    {
        private readonly IEnumerable<KeyValuePair<string, string>> clientConfig;
        private IAdminClient adminClient;
        private IProducer<Null, string> producer;
        private readonly TimeSpan timeout;

        public Producer(IConfigurationRoot jsonConfig)
        {
            clientConfig = new ClientConfig
            {
                BootstrapServers = jsonConfig["bootstrapServer"]
            };

            timeout = TimeSpan.FromSeconds(double.Parse(jsonConfig["timeout"]));
        }

        public async Task StartAsync()
        {
            try
            {
                PrintStartMenu();

                string input;
                while ((input = Console.ReadLine()) != "9")
                {
                    switch (input)
                    {
                        case "1":
                            await AddMessagesAsync(GetPropFromConsole("topic"));
                            break;
                        case "2":
                            await ShowMoreAdminCommandsAsync();
                            break;
                    }

                    PrintStartMenu();
                }
            }
            catch (ProduceException<Null, string> ex)
            {
                Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
            }
            catch (CreateTopicsException ex)
            {
                ex.Results.ForEach(r => Console.WriteLine($"Topic: {r.Topic} was not created - reason: {r.Error.Reason}"));
            }
            catch (DeleteTopicsException ex)
            {
                ex.Results.ForEach(r => Console.WriteLine($"Topic: {r.Topic} was not deleted - reason: {r.Error.Reason}"));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("press any key to exit...");
                Console.ReadKey();
            }
        }

        private async Task AddMessagesAsync(string inputTopic)
        {
            Console.WriteLine("message ([9] for back):");
            using (producer = new ProducerBuilder<Null, string>(clientConfig).Build())
            {
                string input;
                while ((input = Console.ReadLine()) != "9")
                {
                    var dr = await producer.ProduceAsync(inputTopic, new Message<Null, string> { Value = input });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");

                    Console.WriteLine("message ([9] for back):");
                }
            }
        }

        private async Task ShowMoreAdminCommandsAsync()
        {
            PrintAdminCommands();
            using (adminClient = new AdminClientBuilder(clientConfig).Build())
            {
                string input;
                while ((input = Console.ReadLine()) != "9")
                {
                    switch (input)
                    {
                        case "1":
                            PrintAllBrokers();
                            break;
                        case "2":
                            PrintAllTopics();
                            break;
                        case "3":
                            PrintAllGroups();
                            break;
                        case "4":
                            PrintTopicMetadata(GetPropFromConsole("topic"));
                            break;
                        case "5":
                            await CreateTopicsAsync(GetTopicsFromConsole(), int.Parse(GetPropFromConsole("numPartitions")));
                            break;
                        case "6":
                            await DeleteTopicsAsync(GetTopicsFromConsole());
                            break;
                        case "7":
                            var (name, type) = GetConfigDetailsFromConsole();
                            await PrintConfig(name, type);
                            break;
                        case "8":
                            await IncreasePartitionsInTopic(GetTopicsFromConsole(), int.Parse(GetPropFromConsole("increaseTo")));
                            break;
                    }

                    PrintAdminCommands();
                }
            }
        }

        private string GetPropFromConsole(string prop)
        {
            Console.WriteLine($"{prop}:");
            return Console.ReadLine();
        }

        private IEnumerable<string> GetTopicsFromConsole()
        {
            Console.WriteLine("topics (comma separated):");
            return Console.ReadLine()
                .Split(",", StringSplitOptions.RemoveEmptyEntries)
                .Select(t => t.Trim());
        }

        private void PrintTopicMetadata(string topic)
        {
            Console.WriteLine($"Metadata for: {topic}");
            Console.WriteLine(adminClient.GetMetadata(topic, timeout));
        }

        private void PrintAllBrokers()
        {
            Console.WriteLine("Brokers:");
            adminClient.GetMetadata(timeout)
                .Brokers
                .ForEach(Console.WriteLine);
        }

        private void PrintAllTopics()
        {
            Console.WriteLine("Topics:");
            adminClient.GetMetadata(timeout)
                .Topics
                .ForEach(Console.WriteLine);
        }

        private async Task IncreasePartitionsInTopic(IEnumerable<string> topics, int increaseTo)
        {
            var partitions = topics.Select(t => new PartitionsSpecification { Topic = t, IncreaseTo = increaseTo }).ToList();
            await adminClient.CreatePartitionsAsync(partitions);
            partitions.ForEach(p => Console.WriteLine($"Partitions in topic: {p.Topic} are successfully increased to: {p.IncreaseTo}"));
        }

        private async Task CreateTopicsAsync(IEnumerable<string> topicsToCreate, int numPartitions)
        {
            var topics = topicsToCreate.Select(t => new TopicSpecification { Name = t, ReplicationFactor = 1, NumPartitions = numPartitions }).ToList();
            await adminClient.CreateTopicsAsync(topics);
            topics.ForEach(t => Console.WriteLine($"{t.Name} created successfully with {t.NumPartitions} partitions"));
        }

        private async Task DeleteTopicsAsync(IEnumerable<string> topicsToDelete)
        {
            await adminClient.DeleteTopicsAsync(topicsToDelete);
            topicsToDelete.ToList().ForEach(t => Console.WriteLine($"{t} deleted successfully"));
        }

        private void PrintAllGroups()
        {
            var groups = adminClient.ListGroups(timeout);

            Console.WriteLine($"Consumer Groups:");
            foreach (var g in groups)
            {
                Console.WriteLine($"Group: {g.Group} {g.Error} {g.State}");
                Console.WriteLine($"Broker: {g.Broker.BrokerId} {g.Broker.Host}:{g.Broker.Port}");
                Console.WriteLine($"Protocol: {g.ProtocolType} {g.Protocol}");

                Console.WriteLine($"Members:");
                foreach (var m in g.Members)
                {
                    Console.WriteLine($"{m.MemberId} {m.ClientId} {m.ClientHost}");
                    Console.WriteLine($"Metadata: {m.MemberMetadata.Length} bytes");
                    Console.WriteLine($"Assignment: {m.MemberAssignment.Length} bytes");
                }
            }
        }

        private async Task PrintConfig(string name, ResourceType type)
        {
            var configResource = new ConfigResource { Name = name, Type = type };
            var results = await adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource });

            results.SelectMany(r => r.Entries.Select(kvp => $"{kvp.Value.Name}: {kvp.Value.Value}"))
                .ToList()
                .ForEach(Console.WriteLine);
        }

        private Tuple<string, ResourceType> GetConfigDetailsFromConsole()
        {
            Console.WriteLine("--");
            Console.WriteLine("select config type");
            Console.WriteLine("[2] topic config");
            Console.WriteLine("[3] group config:");
            Console.WriteLine("[4] broker config");
            Console.WriteLine("--");

            var input = int.Parse(Console.ReadLine());
            Console.WriteLine("name/id:");
            var name = Console.ReadLine();
            var type = (ResourceType)input;
            return Tuple.Create(name, type);
        }

        private void PrintStartMenu()
        {
            Console.WriteLine("---");
            Console.WriteLine("[1] add message");
            Console.WriteLine("[2] show more admin commands");
            Console.WriteLine("[9] exit");
            Console.WriteLine("---");
        }

        private void PrintAdminCommands()
        {
            Console.WriteLine("---");
            Console.WriteLine("[1] show all kafka brokers");
            Console.WriteLine("[2] show all kafka topics");
            Console.WriteLine("[3] show all consumer groups");
            Console.WriteLine("[4] show full metadata");
            Console.WriteLine("[5] create new topics");
            Console.WriteLine("[6] delete existing topics");
            Console.WriteLine("[7] show config details");
            Console.WriteLine("[8] increase partitions in topic");
            Console.WriteLine("[9] back");
            Console.WriteLine("---");
        }
    }
}
