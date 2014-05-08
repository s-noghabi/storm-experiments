#!/usr/bin/env ruby

require 'yaml'
require 'pp'
config = {
  'numWorkers' => 1, 
  'topologyName' => '', 
  'elements' => {
    # remember, spout first
    'spout' => {
      'tasks' => 1, 
      'executeLatency' => 3, 
    }, 
    'p1' => {
      'tasks' => 1, 
      'executeLatency' => 3, 
    }, 
  }
}

(1..5).each do |i|
  topName = "ExecuteLatency_#{i}"
  keys = config['elements'].keys
  config['elements'][keys[1]]['executeLatency'] = i
  config['topologyName'] = topName
  filedir = "./runs/#{config['topologyName']}"
  `mkdir -p #{filedir}`
  fullpath="#{filedir}/topology.yaml"
  File.open(fullpath, 'w') {|f| f.write config.to_yaml }
  puts "storm jar target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar srg.LinTopology #{fullpath} #{topName}"
  `storm jar target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar srg.LinTopology #{fullpath} #{topName}`
  sleep(60*12)
  `storm kill #{topName}`
end
