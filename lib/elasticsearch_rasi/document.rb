# encoding:utf-8
require 'elasticsearch_rasi/query'
require 'elasticsearch_rasi/scroll'
require 'elasticsearch_rasi/save'

require 'active_support/core_ext/time/calculations'

module ElasticsearchRasi
  class Document
    include Query
    include Save
    include Scroll

    attr_reader :config, :rasi_type, :write_date, :max_age, :read_date, :read_date_months

    def initialize(es, es_anothers, config, type)
      @rasi_type        = type
      @config           = prepare_document_config(type, config)
      @max_age          = nil
      @read_date_months = []
      @es               = es
      @es_another       = prepare_es_another!(es_anothers)
    end

    def get_docs(
      ids:,
      idx: @config.idx_read,
      type: @config.type,
      with_source: true,
      source: nil)

      if @config.alias
        query_docs_by_filter(ids, idx, type, with_source, source)
      else
        query_docs_by_mget(ids, idx, type, with_source, source)
      end
    end

    # return one document['_source'] by its id
    def get(id:, idx: @config.idx_read, type: @config.type, just_source: true, source: nil)
      response =
        if @config.alias
          query_docs_by_filter([id], idx, type, true, source)
        else
          query_docs_by_mget([id], idx, type, true, source)
        end
      return {} if response.blank?
      just_source ? response[id.to_s] : response
    end

    # returns just ids
    def get_ids(ids:, idx: @config.idx_read, type: @config.type)
      if @config.alias
        query_docs_by_filter(ids, idx, type, false)
      else
        query_docs_by_mget(ids, idx, type, false)
      end
    end

    # alias method for saving node document (page, user, group...)
    def save_document(
      docs:,
      method: :index,
      idx: @config.idx_write,
      type: @config.type)
      save_docs(docs, method, idx, type)
    end

    def update_document(
      docs:,
      method: :update,
      idx: @config.idx_write,
      type: @config.type)
      save_docs(docs, method, idx, type)
    end

    def delete_document(
      docs:,
      idx: @config.idx_write,
      type: @config.type)
      save_docs(docs, :delete, idx, type)
    end

    def scroll(query:, params: {}, idx: @config.idx_read, &block)
      scroll_search(query, idx, params, &block)
    end

    def scan_with_total(query:, params: {}, idx: @config.idx_read)
      scroll_scan(query, idx, params)
    end

    def count(query:, idx: @config.idx_read, type: @config.type)
      query_count(query, idx, type)
    end

    def refresh(idx: @config.idx_read)
      @es.indices.refresh(index: idx)
    end

    def search(query:, idx: @config.idx_read, type: @config.type)
      query_search(query, idx, type)
    end

  private

    def prepare_document_config(type, config)
      config = config.clone.merge(
        file:      config["#{type}_file".to_sym],
        idx_read:  config["idx_#{type}_read".to_sym],
        idx_write: config["idx_#{type}_write".to_sym],
        alias:     config["#{type}_alias".to_sym],
        rasi_type: type,
        type:      config["#{type}_type"] || 'document')
      config.compute_dates!
      config
    end

    def prepare_es_another!(es_anothers)
      (es_anothers || []).each do |another_es|
        another_es[:config] = prepare_document_config(@rasi_type, another_es[:config])
      end
      es_anothers
    end
  end
end
