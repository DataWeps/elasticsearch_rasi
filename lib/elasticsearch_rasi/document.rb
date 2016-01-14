# encoding:utf-8
require 'elasticsearch_rasi/base'
require 'elasticsearch_rasi/common'
require 'elasticsearch_rasi/scroll'

class ElasticsearchRasi
  class Document
    include Base
    include Common
    include Scroll

    def initialize(es, config)
      @config = config
      @es = es
    end

    def get_document(
      ids, idx = @config[:idx_read], type = @config[:type], source = true)

      if @config[:alias]
        get_docs_by_filter(ids, idx, type, source)
      else
        get_docs_by_mget(ids, idx, type, source)
      end
    end

    def get_ids(ids, idx = @config[:idx_read], type = @config[:type])
      get_document(ids, idx, type, false)
    end

    # alias method for saving node document (page, user, group...)
    def save_document(
      mentions,
      idx = @config[:idx_write],
      type = @config[:type],
      method = :index)
      save_docs(mentions, idx, type, method)
    end

    def update_document(
      mentions,
      idx = @config[:idx_write],
      type = @config[:type],
      method = :update)
      save_docs(mentions, idx, type, method)
    end

    def scroll(query, idx = @config[:idx_read], params = {}, &block)
      scan_search(query, idx, params, &block)
    end

    def count(query, idx = @config[:idx_read], type = @config[:type])
      query_count(query, idx, type)
    end


    def refresh(idx = @config[:idx_read])
      @es.indices.refresh(index: idx)
    end
  end
end
