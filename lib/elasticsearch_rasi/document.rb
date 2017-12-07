# encoding:utf-8
require 'elasticsearch_rasi/base'
require 'elasticsearch_rasi/common'
require 'elasticsearch_rasi/scroll'

require 'active_support/core_ext/time/calculations'

class ElasticsearchRasi
  class Document
    attr_reader :config, :rasi_type, :write_date, :max_age, :read_date, :read_date_months
    include Base
    include Common
    include Scroll

    def initialize(es, config, es_another = [])
      @config     = config
      @max_age    = nil
      @read_date_months = []
      @write_date =
        if @config["#{@rasi_type}_write_date".to_sym].present? \
          # for data moving between elastics, we need to keep recognizing index basen on published_at
          #   but at the same time, we want to save all the mentions into the database
          #   and ignore max_age
          @max_age =
            if @config[:ignore_max_age]
              nil
            else
              Time.now \
                  .months_ago(@config["#{@rasi_type}_max_age".to_sym] || 6) \
                  .beginning_of_month.to_i
            end
          true
        else
          false
        end
      @read_date =
        if @config["#{@rasi_type}_read_date".to_sym].present?
          from_month = Time.now.months_ago(@config["#{@rasi_type}_max_age".to_sym] || 6).beginning_of_month
          this_month = Time.now.end_of_month
          loop do
            break if from_month > this_month
            @read_date_months <<
              "#{@config["#{@rasi_type}_read_date_base".to_sym] || @config["idx_#{@rasi_type}_read".to_sym]}" \
                "_#{from_month.strftime('%Y%m')}"
            from_month = from_month.months_since(1)
          end
          true
        else
          false
        end

      @es = es
      @es_another = es_another
    end

    def get_docs(
      ids:,
      idx: @config[:idx_read],
      type: @config[:type],
      with_source: true,
      source: nil)

      if @config[:alias]
        get_docs_by_filter(ids, idx, type, with_source, source)
      else
        get_docs_by_mget(ids, idx, type, with_source, source)
      end
    end

    # return one document['_source'] by its id
    def get(id:, idx: @config[:idx_read], type: @config[:type], just_source: true, source: nil)
      if @config[:alias]
        response = get_docs_by_filter([id], idx, type, true, source)
        just_source ? response[id] : response
      else
        get_doc(id, idx, type, just_source, source)
      end
    end

    # returns just ids
    def get_ids(ids:, idx: @config[:idx_read], type: @config[:type])
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

    def scan_with_total(query, params = {}, idx = @config[:idx_read])
      scan(query, idx, params)
    end

    def count(query, idx = @config[:idx_read], type = @config[:type])
      query_count(query, idx, type)
    end

    def refresh(idx = @config[:idx_read])
      @es.indices.refresh(index: idx)
    end

    def search(query, idx = @config[:idx_read], type = @config[:type])
      query_search(query, idx, type)
    end
  end
end
