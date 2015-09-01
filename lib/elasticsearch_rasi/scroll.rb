# encoding:utf-8

class ElasticSearchRasi

  module Scroll

    def scan_search(query, idx, opts, &block)
      scroll = scan(query, idx, opts) or return 0
      scroll_each scroll, &block
    end


    # query - hash of the query to be done
    # opts can override scroll validity, size, etc.
    # return nil in case of error, otherwise scroll hash
    # {:scroll => <scroll_param>, :scroll_id => <scroll_id>, :total => total}
    def scan(query, idx, opts = {})
      opts = {
        "scroll" => ElasticSearchRasi::SCROLL,
        "size"   => ElasticSearchRasi::SLICES
      }.merge(Util.hash_keys_to_str(opts))
      url = "#{idx}/_search?search_type=scan&#{Util.param_str opts}"

      rsp = request_elastic(
        :post,
        url,
        Oj.dump(query),
      ) or return false

      {
        :scroll    => opts['scroll'],
        :scroll_id => rsp['_scroll_id'],
        :total     => rsp['hits']['total'].to_i
      }
    end # scan

    # wrapper to scroll each document for the initialized scan
    # scan - hash as returned by scan method above
    # each document is yielded for processing
    # return nil in case of error (any of the requests failed),
    # count of documents scrolled otherwise
    def scroll_each scan, &block
      count, total = 0, nil
      while true
        url = "_search/scroll?scroll=#{CGI.escape scan[:scroll]}&" +
          "scroll_id=#{CGI.escape scan[:scroll_id]}"

        rsp = request_elastic(:get, url)

        unless rsp
          GLogg.l_f { 'ElasticSearch.scroll_each: FAILED SCROLL' }
          return count
        end

        scan[:scroll_id] = rsp['_scroll_id']
        total ||= rsp['hits']['total'].to_i

        rsp['hits']['hits'].each { |document|
          block.call document
          count += 1
        }
        break if rsp['hits']['hits'].empty?
      end
      count
    end # scroll_each

  end

end
