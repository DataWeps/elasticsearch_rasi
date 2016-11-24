# encoding:utf-8
require 'elasticsearch_rasi/base'
require 'elasticsearch_rasi/common'
require 'elasticsearch_rasi/scroll'

class ElasticsearchRasi
  class Document
    include Base
    include Common
    include Scroll

    def initialize(es, config, es_another = [])
      @config = config
      @es = es
      @es_another = es_another
    end

    def get_docs(
      ids, idx = @config[:idx_read], type = @config[:type], source = true)

      if @config[:alias]
        get_docs_by_filter(ids, idx, type, source)
      else
        get_docs_by_mget(ids, idx, type, source)
      end
    end

    # return one document['_source'] by its id
    def get(id, idx = @config[:idx_read], type = @config[:type])
      get_doc(id, idx, type)
    end

    # returns just ids
    def get_ids(ids, idx = @config[:idx_read], type = @config[:type])
      if @config[:alias]
        get_docs_by_filter(ids, idx, type, false)
      else
        get_docs_by_mget(ids, idx, type, false)
      end
    end

    # alias method for saving node document (page, user, group...)
    def save_document(
      mentions,
      method = :index,
      idx = @config[:idx_write],
      type = @config[:type])
      save_docs(mentions, method, idx, type)
    end

    def update_document(
      mentions,
      method = :update,
      idx = @config[:idx_write],
      type = @config[:type])
      save_docs(mentions, method, idx, type)
    end

    def scroll(query, params = {}, idx = @config[:idx_read], &block)
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
