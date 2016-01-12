# encoding: utf-8
require 'active_support/core_ext/hash'

require_relative 'request'
require_relative 'query'
class ElasticsearchRasi
  module Base
    include Request
    include Query
    # alias method for getting documents
    # - use for index without read alias - we can use _mget query
    def get_docs_by_mget(id, idx = @idx, type = 'document', source = true)
      return {} unless id
      id = [id].flatten
      return {} if id.empty?

      docs = {}
      params = { index: idx, type: type }
      array_slice_indexes(id).each do |slice|
        slice_params = params.merge(
          body: { ids: slice }
        )
        slice_params.merge!(fields: []) unless source
        response = request(:mget, slice_params) || (return nil)
        response['docs'].each do |doc|
          next if !doc['exists'] && !doc['found']
          docs[doc['_id']] = doc['_source']
        end
      end
      source ? docs : docs.keys
    end

    # alias method for requesting direct document
    #   - uses get_doc method
    #   - return {document} without id =>
    def get_document(id, idx = @idx, type = 'document')
      response = request(
        :get, id: id, index: idx, type: type, ingnore: 404) || (return nil)
      response.values.first
    end

    # alias method for getting documents
    # - use for index with read alias - we have to use use _ids filter query
    def get_docs_by_filter(ids, idx = @idx, type = 'document', source = true)
      return {} unless ids
      ids = [ids] unless ids.is_a?(Array)
      return {} if ids.empty?

      docs = {}
      params = { index: idx, type: type }
      array_slice_indexes(ids).each do |slice|
        slice_params = params.merge(body: get_docs_query(
          { ids: { type: type, values: slice } },
          slice.size))
        slice_params[:fields] = [] unless source
        response = request(:search, slice_params) || (return nil)
        parse_response(response, docs)
      end
      source ? docs : docs.keys
    end # get_docs

    # get document from ES with direct query trough GET request
    #   - return nil in case of error, otherwise {id => document}
    def get_doc(id, idx = @idx, type = 'document')
      response = request(:get, index: idx, type: type, id: id)
      return {} if !response || !response.is_a?(Hash) ||
                   !(response['exists'] || response['found'])
      { response['_id'] => response['_source'] }
    end # save_docs

    # docs - [docs] or {id => doc}
    def save_docs(docs, idx = @idx, type = 'document', method = :index)
      return true unless docs && !docs.empty? # nothing to save
      to_save =
        if docs.is_a?(Hash) # convert to array
          docs.stringify_keys!
          if docs.include?('_id')
            [docs]
          else
            docs.map { |id, doc| doc.merge('_id' => id) }
          end
        else
          docs
        end
      raise "Incorrect docs supplied (#{docs.class})" unless to_save.is_a?(Array)
      errors = []
      array_slice_indexes(to_save, BULK_STORE).each do |slice|
        response = request(:bulk, body: create_bulk(slice, idx, type, method))
        next unless response['errors']
        errors << response['items'].map do |item|
          next unless item[item.keys[0]].include?('error')
          item[item.keys[0]]
        end
      end
      errors.compact!

      errors.empty? ? true : errors.flatten
    end # save_docs

    # query - hash of the query to be done
    # return nil in case of error, rsp['hits'] otherwise
    def search(query, idx, type = 'document')
      response = request(
        :search,
        index: idx,
        type: type,
        body: query
      ) || (return {})
      parse_response(response)
    end # count

    # query - hash of the query to be done
    # return nil in case of error, document count otherwise
    def query_count(query, idx, type = 'document')
      response = request(
        :count,
        index: idx,
        type:  type,
        body:  query)
      response['count'].to_i || 0
    end
  end
end
