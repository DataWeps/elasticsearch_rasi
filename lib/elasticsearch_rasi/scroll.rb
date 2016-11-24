# encoding:utf-8
class ElasticsearchRasi
  module Scroll
    SCROLL = '1m'
    def scan_search(query, idx, params, &block)
      scroll = scan(query, idx, params) || (return 0)
      scroll_each(scroll, &block)
    end

    # query - hash of the query to be done
    # opts can override scroll validity, size, etc.
    # return nil in case of error, otherwise scroll hash
    # {:scroll => <scroll_param>, :scroll_id => <scroll_id>, :total => total}
    def scan(query, idx, params = {})
      response = request(
        :search,
        {
          index: idx, scroll: SCROLL, body: query, search_type: 'scan'
        }.merge(params)) || (return false)
      {
        scroll:    params['scroll'] || SCROLL,
        scroll_id: response['_scroll_id'],
        total:     response['hits']['total'].to_i
      }
    end # scan

    # wrapper to scroll each document for the initialized scan
    # scan - hash as returned by scan method above
    # each document is yielded for processing
    # return nil in case of error (any of the requests failed),
    # count of documents scrolled otherwise
    def scroll_each(scan, &block)
      scan.delete(:total)
      count = 0
      loop do
        response = request(:scroll, scan)
        break unless response
        scan[:scroll_id] = response['_scroll_id']
        break if !response || response['hits']['hits'].empty?
        response['hits']['hits'].each do |document|
          block.call(document)
          count += 1
        end
      end
      count
    end # scroll_each
  end
end
