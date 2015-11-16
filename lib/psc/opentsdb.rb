require "psc/opentsdb/version"
gem 'httparty'
require 'httparty'

  module OpenTsdb
    class Server
      include HTTParty
      attr_accessor :port,:server_url

      def initialize(server_url,port = 4242)
        @server_url = server_url
        @port = port
        self.class.base_uri @server_url + ":" + @port.to_s
      end

      def suggest(type,query=nil,max=1000)

        # type must be one of metrics, tagk, tagv
        # query matches entire strings to front of stored data
        # max is max results returned

        data = Hash.new
        data['type'] = type
        unless query.nil?
          data['q'] = query
        end
        data['max'] = max
        response = self.class.post('/api/suggest',body: data.to_json)
        return response.parsed_response
      end

      def create_metric(metric)
        data = Hash.new
        data['metric'] = [metric]
        response = self.class.post('/api/uid/assign',body: data.to_json)
        puts '***'
        puts response.parsed_response.to_s
        if response.code == 400
          raise MetricCreationError , metric + " could not be created. " + response.parsed_response['metric_errors'][metric].to_s
          return
        elsif response.code != 200
          raise MetricCreationError , metric + " could not be created.  Unknown Error"
          return
        end
        response
      end

      def put(data_points)
        # data_points = array of data point objects
        data = Array.new
        data_points.each do |dp|
          data.push(dp.to_hash)
        end
        response = self.class.post('/api/put', body: data.to_json)

      end
    end

    class DataPoint
      attr_accessor :metric,:timestamp,:value,:tags
      def initialize(metric,timestamp,value,tags)
        @metric = metric
        #DateTime
        @timestamp = timestamp.to_time.to_i
        @value = value
        #Array of Tags
        @tags = tags
      end

      def to_hash
        h = {}
        h[:metric] = @metric
        h[:timestamp] = @timestamp
        h[:value] = @value
        h[:tags] = {}
        tags.each do |tag|
          h[:tags][tag.tag_name] = tag.tag_value
        end
        h
      end
    end

    class Tag
      attr_accessor :tag_name,:tag_value
      def initialize(tag_name,tag_value)
        @tag_name = tag_name
        @tag_value = tag_value
      end
    end

    class MetricCreationError < StandardError
      def initialize(message)
        super(message)
      end
    end
  end

