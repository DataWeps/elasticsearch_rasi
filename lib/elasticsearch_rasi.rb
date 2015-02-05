# encoding:utf-8
require 'glogg'
require 'curburger'
require 'oj'

# helper methods
require 'elasticsearch_rasi/util'
require 'elasticsearch_rasi/query'
require 'elasticsearch_rasi/request'

# alias query methods
require 'elasticsearch_rasi/scroll'
require 'elasticsearch_rasi/node'
require 'elasticsearch_rasi/mention'
require 'elasticsearch_rasi/rotation'

class ElasticSearchRasi

  include Query
  include Request
  include Scroll
  include Node
  include Mention
  include Rotation

  Oj.default_options = {:mode => :compat}
  SLICES     = 250
  BULK_STORE = 500
  LOG_FILE   = File.join(File.dirname(__FILE__), '.', 'log/elasticsearch.log')
  attr_accessor :idx, :idx_node_read, :idx_node_write
  attr_accessor :idx_mention_read, :idx_mention_write
  attr_accessor :idx_opts

  # idx - index name OR index type
  # opts - optional configuration:
  #   :url - database url (default localhost)
  #   :ua  - Curburger::Client instance options
  #   :direct_idx - true|false
  #   :logging    - true|false (default false)
  #   :log_file
  #   :logg_level - GLogg::??
  def initialize(idx, opts = {})
    opts[:direct_idx] = false unless opts.include?(:direct_idx)
    @direct_idx       = opts[:direct_idx] || false

    $ES ||= {
      # example
      # -------
      # :db_url => 'http://localhost:9200',

      # :yelp => {
      #   :base           => 'yelp_cz',
      #   :node_suffix    => '_places',
      #   :mention_suffix => '_reviews',

      #   :node_read      => '',
      #   :node_write     => '',

      #   :mention_read   => '',
      #   :mention_write  => '_current',

      # },

      # :logging => true,
      # :log_file => 'elasticsearch.log'
    }

    if (opts[:logging] || $ES[:logging])
      GLogg.ini(
        opts[:log_file]  || $ES[:log_file]  || LOG_FILE,
        opts[:log_level] || $ES[:log_level] || GLogg::L_INF
      )
    else
      GLogg.ini(nil, GLogg::L_NIL)
    end
    @url   = opts[:url] || $ES[:url] || 'http://127.0.0.1:9200'

    # direct set index(es)
    if @direct_idx
      # main node index (fb_page, topic, article etc ...)
      @idx          = $ES.include?(idx.to_sym) ?
        $ES[idx.to_sym] : idx
    else
      raise ArgumentError.new("Missing defined index '#{idx}'") unless
        $ES.include?(idx.to_sym)
      @idx_opts          = $ES[idx.to_sym]
      @idx_node_read     = get_index(:node, :read)
      @idx_node_write    = get_index(:node, :write)
      @idx_mention_read  = get_index(:mention, :read)
      @idx_mention_write = get_index(:mention, :write)
    end

    @ua_opts = {
      :ignore_kill    => true,
      :req_norecode   => true,
      :retry_45       => false,
      :req_retry_wait => 1,
      :req_attempts   => 2,
      :logging        => opts[:logging] || false,
    }.merge(opts[:ua] || $ES[:ua] || {})
    @ua = Curburger.new @ua_opts
  end

  def get_index(type, access)
    return nil unless @idx_opts && !@idx_opts.empty?
    base  = "#{@idx_opts[:prefix]}#{@idx_opts[:base]}"
    index = "#{base}#{@idx_opts[:"#{type}_suffix"]}"
    "#{index}#{@idx_opts[:"#{type}_#{access}"]}"
  end


  # alias method for getting node document (page, user, group...)
  def get_documents_by_mget(id, idx = @idx, type = 'document')
    return {} unless id
    id = [id] unless id.kind_of?(Array)
    return {} if id.empty?

    url, docs = "#{idx}/_mget", {}
    array_slice_indexes(id).each { |slice|
      data = Oj.dump({'ids' => slice})
      response  = request_elastic(
        :get,
        url,
        {:data => data}
      ) or return nil
      response['docs'].each { |doc|
        next unless doc['exists'] # non-existent document
        docs[doc['_id']] = doc['_source']
      }
    }
    docs
  end

  # alias method for requesting direct document
  #   - uses get_doc method
  #   - return {document} without id =>
  def get_document(key, idx = @idx, type = 'document')
    response = get_doc(key, idx, type) or return nil
    response.values.first
  end

  # ids - single id or array of ids
  # return nil in case of error, {id => doc} of documents found otherwise
  def get_docs(ids, idx = @idx, type = 'document')
    return {} unless ids
    ids = [ids] unless ids.kind_of?(Array)
    return {} if ids.empty?

    if ids.count == 1 && @direct_idx == false
      return get_doc(ids.first, idx, type)
    end

    url, docs = "#{idx}/_search", {}
    array_slice_indexes(ids).each { |slice|
      data = get_docs_query(
        {'ids' => {'type' => type, 'values' => slice}},
        slice.count
      )
      response  = request_elastic(
        :get,
        url,
        {:data => Oj.dump(data)}
      ) or return nil
      parse_response response, docs
    }
    docs
  end # get_docs

  # get document from ES with direct query trough GET request
  #   - return nil in case of error, otherwise {id => document}
  def get_doc(key, idx = @idx, type = 'document')
    response = request_elastic(
      :get, "#{idx}/#{type}/#{key}"
    )
    return {} unless response && response.kind_of?(Hash) &&
      response.include?('exists') && response['exists']
    {response['_id'] => response['_source']}
  end # save_docs

  # docs - [docs] or {id => doc}
  def save_docs(docs, idx = @idx, type = 'document')
    return true unless docs && !docs.empty? # nothing to save
    to_save = []
    if docs.kind_of?(Hash) # convert to array
      if docs.include?('id')
        to_save << docs
      else
        docs.each_pair { |id, doc| to_save << doc.merge({'id' => id}) }
      end
    elsif docs.kind_of? Array
      to_save = docs
    else # failsafe
      raise "Incorrect docs supplied (#{docs.class})"
    end

    # saving single document via direct POST request
    if to_save.count == 1
      response = request_elastic(
        :post,
        "#{idx}/#{type}/#{to_save.first['id']}",
        Oj.dump(to_save.first)
      )
      return response
    end

    # more than 1 document save via BULK
    array_slice_indexes(to_save, BULK_STORE).each { |slice|
      bulk = ''
      slice.each { |doc|
        bulk += %Q(
          {"index": {"_index": "#{idx}", "_id": "#{doc['id']}", "_type": "#{type}"}}\n)
        bulk += Oj.dump(doc) + "\n"
      }
      return nil if bulk.empty? # should not happen
      bulk    += "\n" # empty line in the end required
      request_elastic(:post, "_bulk", bulk)
    }
    true
  end # save_docs

  # query - hash of the query to be done
  # return nil in case of error, rsp['hits'] otherwise
  def search(query, idx)
    url, data = "#{idx}/_search", Oj.dump(query)
    response  = request_elastic(
      :post, url, data
    ) or return {}
    parse_response response
  end # count

  # query - hash of the query to be done
  # return nil in case of error, document count otherwise
  def count(query, idx)
    url  = "#{idx}/_search"
    rsp = request_elastic(
      :post,
      url,
      Oj.dump(query)
    ) or return 0
    rsp['hits']['total'].to_i
  end # count

  # query - direct GET query through URL
  # return nil in case of error, documents (unprepared) otherwise
  def direct_query(idx, query, what = '_search')
    url = "#{idx}/#{what}?#{query}"
    request_elastic :get, url
  end # direct_query

  private

  # prepare array indexes,lengths in the manner of slices
  # e.g. for a.length=25 and cnt=10 return [[0,10],[10,10],[20,5]]
  def array_slice_indexes(ids, cnt = SLICES)
    rslt = []
    rslt << ids.shift(cnt) until ids.empty?
    rslt
  end

  # translate results from ES to {id => doc}
  def parse_response(response, docs = {})
    response['hits']['hits'].each { |doc|
      docs[doc['_id']] = doc['_source']
    }
  end

end # ElasticSearch
