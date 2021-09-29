require 'json'

if ARGV.length != 1
    puts "You need to pass in the messages directory as an argument! e.g. 'ruby filter_dlq_messages.rb 24-Sep-2021'"
    exit
end

messages_dir = ARGV[0]
results_dir = "./results-#{messages_dir}"

puts "Cleaning up previous results..."

Dir.mkdir(results_dir) unless Dir.exist?(results_dir)
Dir.glob("#{results_dir}/*") do |file|
    File.delete(file)
end

results = {}

puts "Filtering messages..."

Dir.glob("#{messages_dir}/*") do |file|
    leftover = true
    log = File.read(file)
    log = JSON.parse(log)
    body = JSON.parse(log["Body"])
    message = JSON.parse(body["Message"])

    topic = message["Topic"]
    topic = body["TopicArn"] unless topic
    topic = message["Content"]["PipelineStreamName"] unless topic
    topic = "#{message["Content"]["database"]}-#{message["Content"]["type"]}-#{message["Content"]["table"]}" unless topic
    
    if topic == "--"
        puts "The following message wasn't filtered:"
        puts message
    else
        results[topic] ? results[topic] = results[topic] + 1 : results[topic] = 1
        File.write("#{results_dir}/#{topic}.json", body.to_json + "\n", mode: "a")
    end
end

File.write("#{results_dir}/summary.json", results.to_json)

puts "Filtering done. Groups of similar messages can be found under #{results_dir}. Summary of results can be found in #{results_dir}/summary.json."