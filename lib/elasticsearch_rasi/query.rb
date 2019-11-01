# encoding:utf-8
require 'elasticsearch_rasi/helpers/queries'
require 'elasticsearch_rasi/request'

module ElasticsearchRasi
  module Query
    include Request

    def get_docs_query(query, size = ElasticsearchRasi::SLICES)
      Queries.prepare_query(:docs_query, query, size)
    end

    def get_docs_query_ids(query, size = ElasticsearchRasi::SLICES)
      Queries.prepare_query(:docs_query_ids, query, size)
    end

    def get_filter_query(query)
      Queries.prepare_query(:filter_query, query)
    end

    def get_bool_query(query)
      Queries.prepare_query(:bool_query, query)
    end

    def get_count_query(query)
      Queries.prepare_query(:count_query, query)
    end

    def get_mget_query(ids, indices)
      Queries.prepare_query(:mget_query, ids, nil, indices)
    end

    def query_docs_by_mget(ids, idx = @idx, type = 'document', with_source = true, source = nil)
      docs_query(
        request_type: :mget,
        ids: ids,
        idx: idx,
        type: type,
        with_source: with_source,
        source: source,
        query_block: proc do |slice|
          get_mget_query(
            slice,
            Common.prepare_read_index(idx, @read_date, @read_date_months))
        end,
        parse_block: proc do |response|
          response['docs'].each_with_object({}) do |doc, mem|
            next unless doc['found']
            mem[doc['_id']] = doc['_source']
          end
        end)
    end

    # query - hash of the query to be done
    # return nil in case of error, rsp['hits'] otherwise
    def query_search(query, idx, type = 'document')
      response = request(
        :search,
        index: Common.prepare_read_index(idx, @read_date, @read_date_months),
        type: type,
        body: query) || (return {})
      Common.parse_response(response)
    end

    # query - hash of the query to be done
    # return nil in case of error, document count otherwise
    def query_count(query, idx, type = 'document')
      response = request(
        :search,
        index: Common.prepare_read_index(idx, @read_date, @read_date_months),
        type:  type,
        body:  query)
      response['hits']['total'].to_i || 0
    end

    # alias method for getting documents
    # - use for index with read alias - we have to use use _ids filter query
    def query_docs_by_filter(ids, idx = @idx, type = 'document', with_source = true, source = nil)
      docs_query(
        request_type: :search,
        ids: ids,
        idx: idx,
        type: type,
        with_source: with_source,
        source: source,
        query_block: proc { |slice| get_docs_query({ ids: { type: type, values: slice } }, slice.size) },
        parse_block: proc { |response| Common.parse_response(response) })
    end

    # get document from ES with direct query trough GET request
    #   - return nil in case of error, otherwise {id => document}
    def query_doc(id, idx = @idx, type = 'document', just_source = true, source = nil)
      params = {
        index: Common.prepare_read_index(idx, @read_date, @read_date_months),
        type: type,
        id: id,
        ignore: 404 }
      params[:_source] ||= source if source
      response = request(:get, params)
      return {} if !response || !response.is_a?(Hash) ||
                   !(response['exists'] || response['found'])
      if just_source
        response['_source']
      else
        { response['_id'] => response['_source'] }
      end
    end

  private

    def docs_query(
      request_type:,
      ids:,
      idx:,
      query_block:,
      parse_block:,
      type: 'document',
      with_source: true,
      source: nil)

      return {} unless ids
      ids = [ids].flatten.compact
      return {} if ids.empty?

      docs = {}
      params = { type: type }
      unless request_type == :mget
        params[:index] = Common.prepare_read_index(idx, @read_date, @read_date_months)
      end
      Common.array_slice_indices(ids).each do |slice|
        slice_params = params.merge(body: query_block.call(slice))
        slice_params[:_source] = false  unless with_source
        slice_params[:_source] = source unless source.nil?
        response = request(request_type, slice_params) || (return nil)
        Common.response_error(response)
        docs.merge!(parse_block.call(response))
      end
      with_source ? docs : docs.keys
    end
  end
end
