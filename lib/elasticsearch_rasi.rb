# encoding:utf-8
require 'oj'
require 'elasticsearch'
require 'typhoeus'
require 'typhoeus/adapters/faraday'
require 'active_support/core_ext/hash/deep_merge'

# helper methods
require 'elasticsearch_rasi/util'
require 'elasticsearch_rasi/query'
require 'elasticsearch_rasi/request'
require 'elasticsearch_rasi/common'

# alias query methods
require 'elasticsearch_rasi/scroll'
require 'elasticsearch_rasi/rotation'
require 'elasticsearch_rasi/mention'
require 'elasticsearch_rasi/node'

class ElasticsearchRasi
  include Query
  include Request
  include Scroll
  include Rotation
  include Common

  Oj.default_options = { mode: :compat }
  SLICES     = 250
  BULK_STORE = 500
  LOG_FILE   = File.join(File.dirname(__FILE__), '.', 'log/elasticsearch.log')

  attr_accessor :config

  # idx - index name OR index type
  # opts - optional configuration:
  #   :url - database url (default localhost)
  #   :ua  - Curburger::Client instance options
  #   :direct_idx - true|false
  #   :logging    - true|false (default false)
  #   :log_file
  #   :logg_level - GLogg::??
  def initialize(idx, opts = {})
    direct_idx = opts.include?(:direct_idx) ? opts[:direct_idx] : false

    # direct set index(es)
    @config =
      if direct_idx
        # main node index (fb_page, topic, article etc ...)
        ES && ES.include?(idx.to_sym) ? ES[idx.to_sym] : opts
      else
        opts = (ES[idx.to_sym] || {}).deep_merge(opts)
        raise(ArgumentError, "Missing defined index '#{idx}'") if
          !opts || opts.empty?
        ES[idx.to_sym].deep_symbolize_keys.merge(
          node_file:               opts[:file] ? opts[:file][:node] : nil,
          mention_file:            opts[:file] ? opts[:file][:mention] : nil,
          idx_node_read:           get_index(opts, :node, :read),
          idx_node_write:          get_index(opts, :node, :write),
          idx_mention_read:        get_index(opts, :mention, :read),
          idx_mention_write:       get_index(opts, :mention, :write),
          idx_node_read_client:    get_index(opts, :node, :read, :client),
          idx_mention_read_client: get_index(opts, :mention, :read, :client),
          node_type:               opts[:node_type] || 'document',
          mention_type:            opts[:mention_type] || 'document',
          node_alias:              opts[:node_alias],
          mention_alias:           opts[:mention_alias]).merge(connect: opts[:connect])
      end
    @es = Elasticsearch::Client.new(@config[:connect].dup)
    @es_another =
      if @config.include?(:connect_another) && !@config[:connect_another].empty?
        @config[:connect_another].map do |connect|
          {
            es: Elasticsearch::Client.new(connect[:connect].dup),
            config: connect }
        end
      else
        []
      end
  end

  def mention
    @mention ||= ElasticsearchRasi::Mention.new(@es, @config, @es_another)
    @mention
  end

  def node
    @node ||= ElasticsearchRasi::Node.new(@es, @config, @es_another)
    @node
  end

  def document
    @document ||= ElasticsearchRasi::Document.new(@es, @config, @es_another)
    @document
  end

private

  def get_index(opts, type, access, client_type = :system)
    return nil unless opts && !opts.empty?
    if client_type == :client && opts.include?("#{type}_client".to_sym)
      opts["#{type}_client".to_sym]
    elsif client_type == :system
      base  = "#{opts[:prefix]}#{opts[:base]}"
      index = "#{base}#{opts[:"#{type}_suffix"]}"
      "#{index}#{opts[:"#{type}_#{access}"]}"
    end
  end

  # query - direct GET query through URL
  # return nil in case of error, documents (unprepared) otherwise
  # def direct_query(idx, query, what = '_search')
  #   url = "#{idx}/#{what}?#{query}"
  #   request_elastic :get, url
  # end # direct_query
end
