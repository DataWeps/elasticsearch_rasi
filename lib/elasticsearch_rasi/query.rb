# encoding:utf-8
require 'elasticsearch_rasi/helpers/queries'
require 'elasticsearch_rasi/common'

module ElasticsearchRasi
  module Query
    def get_docs_query(query, size = ElasticsearchRasi::SLICES)
      Queries.prepare_query(:docs_query, query, size)
    end

    def get_docs_query_ids(query, size = ElasticsearchRasi::SLICES)
      Queries.prepare_query(:docs_query_ids, query, size)
    end

    def get_bool_query(query)
      Queries.prepare_query(:bool_query, query)
    end

    def get_count_query(query)
      Queries.prepare_query(:count_query, query)
    end

    def get_docs_by_mget(ids, idx = @idx, type = 'document', with_source = true, source = nil)
      docs_query(
        request_type: :mget,
        ids: ids,
        idx: idx,
        type: type,
        with_source: with_source,
        source: source,
        query_block: Proc.new { |slice| get_docs_query(ids: slice) },
        parse_block: Proc.new do |response|
          response['docs'].each do |doc|
            next if !doc['exists'] && !doc['found']
            docs[doc['_id']] = doc['_source']
          end
        end)
    end

    # query - hash of the query to be done
    # return nil in case of error, rsp['hits'] otherwise
    def query_search(query, idx, type = 'document')
      response = request(
        :search,
        index: prepare_read_index(idx),
        type: type,
        body: query) || (return {})
      parse_response(response)
    end # count

    # query - hash of the query to be done
    # return nil in case of error, document count otherwise
    def query_count(query, idx, type = 'document')
      response = request(
        :search,
        index: prepare_read_index(idx),
        type:  type,
        body:  query)
      response['hits']['total'].to_i || 0
    end

    # alias method for getting documents
    # - use for index with read alias - we have to use use _ids filter query
    def get_docs_by_filter(ids, idx = @idx, type = 'document', with_source = true, source = nil)
      docs_query(
        request_type: :search,
        ids: ids,
        idx: idx,
        type: type,
        with_source: with_source,
        source: source,
        query_block: Proc.new { |slice| get_docs_query({ ids: { type: type, values: slice } }, slice.size) },
        parse_block: Proc.new { |response| parse_response(response) })
    end

    # get document from ES with direct query trough GET request
    #   - return nil in case of error, otherwise {id => document}
    def get_doc(id, idx = @idx, type = 'document', just_source = true, source = nil)
      params = {
        index: prepare_read_index(idx),
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
    end # save_docs

  private

    def docs_query(request_type:, ids:, idx:, type: 'document', with_source: true, source: nil, query_block:, parse_block:)
      return {} unless ids
      ids = [ids].flatten
      return {} if ids.empty?

      docs = {}
      params = { index: prepare_read_index(idx), type: type }
      array_slice_indexes(ids).each do |slice|
        slice_params = params.merge(body: query_block.call(slice))
        slice_params[:_source] = false unless with_source
        slice_params[:_source] = source unless source.nil?
        response = request(request_type, slice_params) || (return nil)
        docs     = parse_block.call(response)
      end
      with_source ? docs : docs.keys
    end
  end
end
