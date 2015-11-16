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

      # return array of aggregators
      def aggregators
        do_get('/api/aggregators')
      end

      # return hash with version info
      def version
        do_get('/api/version')
      end

      # return hash with config info
      def config
        do_get('/api/config')
      end

      def config_filters
        do_get('/api/config/filters')
      end

      def stats
        do_get('/api/stats')
      end

      def stats_threads
        do_get('/api/stats/threads')
      end

      def stats_jvm
        do_get('/api/stats/jvm')
      end

      def stats_region_clients
        do_get('/api/stats/region_clients')
      end

      def serializers
        do_get('/api/serializers')
      end

      def s(file_path)
        begin
          response = self.class.get("/s/#{file_path}")
        rescue StandardError => e
          raise OpenTsdbError, "Error contacting OpenTSDB server"
        end

        if response.code == 404
          raise OpenTsdbError, "File not found"
          return
        elsif response.code != 200
          raise OpenTsdbError, "File could not be retrieved.  Unknown Error."
          return
        end

        response.parsed_response
      end

      def drop_caches
        begin
          response = self.class.post('/api/dropcaches')
        rescue StandardError => e
          raise OpenTsdbError, "Error contacting OpenTSDB server"
        end
        if response.code != 200
          raise OpenTsdbError, "Cache could not be dropped.  " + response.parsed_response['status'] + ": " + response.parsed_response['message']
        end
        response
      end

      def create(type,name)
        #type must be one of metric, tagk (tag name), tagv (tag value)
        valid_types = ['metric','tagk','tagv']
        unless valid_types.include?(type)
          raise CreationError, type + " is not a valid type for the create method"
          return
        end

        if name.empty?
          raise CreationError, name.to_s + " is not a valid value for the create method"
          return
        end

        data = Hash.new
        data[type] = [name]
        begin
          response = self.class.post('/api/uid/assign',body: data.to_json)
        rescue StandardError => e
          raise OpenTsdbError, "Couldn't create #{type} name #{name}.  Error contacting OpenTSDB server"
          return
        end
        response
      end

      def create_metric(metric)
        response = create('metric',metric)
        if response.code == 400
          raise CreationError , metric + " could not be created. " + response.parsed_response['metric_errors'][metric].to_s
          return
        elsif response.code != 200
          raise CreationError , metric + " could not be created.  Unknown Error"
          return
        end
        response
      end

      def create_tag_name(tagk)
        response = create('tagk',tagk)
        if response.code == 400
          raise CreationError , tagk + " could not be created. " + response.parsed_response['tagk_errors'][tagk].to_s
          return
        elsif response.code != 200
          raise CreationError , tagk + " could not be created.  Unknown Error"
          return
        end
        response
      end

      def create_tag_value(tagv)
        response = create('tagv',tagv)
        if response.code == 400
          raise CreationError , tagv + " could not be created. " + response.parsed_response['tagv_errors'][tagk].to_s
          return
        elsif response.code != 200
          raise CreationError , tagv + " could not be created.  Unknown Error"
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

      protected

      def do_get(end_point)
        begin
          response = self.class.get(end_point)
        rescue StandardError => e
          raise OpenTsdbError, "Error contacting OpenTSDB server"
        end
        response.parsed_response
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

    class OpenTsdbError < StandardError
    end

    class CreationError < OpenTsdbError
    end

  end

