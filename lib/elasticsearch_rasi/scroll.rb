# encoding:utf-8
require 'elasticsearch_rasi/request'

require 'active_support/core_ext/hash'

module ElasticsearchRasi
  module Scroll
    include Request
    SCROLL = '1m'.freeze
    def scroll_search(query, idx, params, &block)
      params.symbolize_keys!
      scroll = scroll_scan(query, idx, params) || (return 0)
      scroll_each(scroll, &block)
    end

    # query - hash of the query to be done
    # opts can override scroll validity, size, etc.
    # return nil in case of error, otherwise scroll hash
    # {:scroll => <scroll_param>, :scroll_id => <scroll_id>, :total => total}
    def scroll_scan(query, idx, params = {})
      response = request(
        :search,
        { index:  Common.prepare_read_index(idx, @read_date, @read_date_months),
          scroll: SCROLL,
          body:   query }.merge(params)) || (return false)
      Common.response_error(response)
      {
        'hits' => { 'hits' => response['hits']['hits'] },
        scroll:    params[:scroll] || SCROLL,
        scroll_id: response['_scroll_id'],
        total:     response['hits']['total'].to_i }
    end

    # wrapper to scroll each document for the initialized scan
    # scan - hash as returned by scan method above
    # each document is yielded for processing
    # return nil in case of error (any of the requests failed),
    # count of documents scrolled otherwise
    def scroll_each(scan)
      scan.delete(:total)
      response = { 'hits' => scan.delete('hits') }
      count = 0
      loop do
        response['hits']['hits'].each do |document|
          yield(document)
          count += 1
        end
        response = request(:scroll, scan)
        break unless response
        scan[:scroll_id] = response['_scroll_id']
        break if !response || response['hits']['hits'].empty?
      end
      count
    end
  end
end
