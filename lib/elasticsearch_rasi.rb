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
    include Request
    attr_accessor :config, :es
    attr_reader :es_another

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
      default_config_connect_params!(@config.connect)

      @es = create_client(@config.connect)
      @es_another = create_another_clients(@config)
    end

    def mention
      @mention ||= ElasticsearchRasi::Mention.new(@es, @es_another, @config)
    end

    def node
      @node ||= ElasticsearchRasi::Node.new(@es, @es_another, @config)
    end

    def document
      @document ||= ElasticsearchRasi::Document.new(@es, @es_another, @config, :document)
    end

    def translate_lang_to_country(language)
      ElasticsearchRasi::TranslateLangToCountry.translate_lang_to_country(language)
    end

  private

    def default_config_connect_params!(connect)
      return if connect.blank? || (connect[:host] || connect[:hosts] || []).blank?

      connect[:retry_on_failure]   ||= true
      connect[:reload_connections] ||= true
    end

    def create_client(connect_config)
      Elasticsearch::Client.new(connect_config.dup)
    end

    def create_another_clients(common_config)
      if @config.connect_another.present?
        (@config.connect_another || []).map do |connect_config|
          default_config_connect_params!(connect_config[:connect])
          { es:     create_client(connect_config[:connect].dup),
            config: common_config.clone.merge(connect_config) }
        end
      else
        []
      end
    end
  end
end
