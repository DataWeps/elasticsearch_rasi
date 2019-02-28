# encoding:utf-8
require 'elasticsearch'
require 'typhoeus'
require 'typhoeus/adapters/faraday'
require 'active_support/core_ext/hash/deep_merge'
require 'active_support/core_ext/object/blank'

# helper methods
require 'elasticsearch_rasi/util'
require 'elasticsearch_rasi/helpers/config'
require 'utils/translate_lang_to_country'

# alias query methods
require 'elasticsearch_rasi/document'
require 'elasticsearch_rasi/mention'
require 'elasticsearch_rasi/node'

module ElasticsearchRasi
  SLICES     = 250
  BULK_STORE = 250
  DEFAULT_ANOTHER_METHODS = %i[index update bulk].freeze
  LOG_FILE = File.join(File.dirname(__FILE__), '.', 'log/elasticsearch.log')

  class Client
    attr_accessor :config, :es

    # idx - index name OR index type
    # opts - optional configuration:
    #   :url - database url (default localhost)
    #   :ua  - Curburger::Client instance options
    #   :direct_idx - true|false
    #   :logging    - true|false (default false)
    #   :log_file
    #   :logg_level - GLogg::??
    def initialize(idx, opts = {})
      opts[:direct_idx] = opts.include?(:direct_idx) ? opts[:direct_idx] : false

      # direct set index(es)
      @config = ElasticsearchRasi::Config.new(idx, opts)

      if (@config.connect[:host] || @config.connect[:hosts] || []).present?
        @config.connect[:retry_on_failure]   ||= true
        @config.connect[:reload_connections] ||= true
      end

      @es = Elasticsearch::Client.new(@config.connect.dup)
      @es_another = create_another_clients
    end

    def mention
      @mention ||= ElasticsearchRasi::Mention.new(@es, @config, @es_another)
    end

    def node
      @node ||= ElasticsearchRasi::Node.new(@es, @config, @es_another)
    end

    def document
      @document ||= ElasticsearchRasi::Document.new(@es, @config, @es_another)
    end

    def translate_lang_to_country(language)
      TranslateLangToCountry.translate_lang_to_country(language)
    end

  private

    def create_another_clients
      if @config.connect_another.present?
        (@config.connect_another || []).map do |connect|
          { es: Elasticsearch::Client.new(connect[:connect].dup),
            config: connect }
        end
      else
        []
      end
    end
  end
end
