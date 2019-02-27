# encoding: utf-8
require 'active_support/core_ext/hash'
require 'active_support/core_ext/object/blank'

require_relative 'request'
require_relative 'query'
class ElasticsearchRasi
  module Base
    include Request
    include Query
    # alias method for getting documents
    # - use for index without read alias - we can use _mget query
    def get_docs_by_mget(id, idx = @idx, type = 'document', with_source = true, source = nil)
      return {} unless id
      id = [id].flatten
      return {} if id.empty?

      docs = {}
      params = { index: prepare_read_index(idx), type: type }
      array_slice_indexes(id).each do |slice|
        slice_params = params.merge(body: { ids: slice })
        slice_params[:_source] = false unless with_source
        slice_params[:_source] = source unless source.nil?
        response = request(:mget, slice_params) || (return nil)
        response['docs'].each do |doc|
          next if !doc['exists'] && !doc['found']
          docs[doc['_id']] = doc['_source']
        end
      end
      with_source ? docs : docs.keys
    end

    # alias method for getting documents
    # - use for index with read alias - we have to use use _ids filter query
    def get_docs_by_filter(ids, idx = @idx, type = 'document', with_source = true, source = nil)
      return {} unless ids
      ids = [ids] unless ids.is_a?(Array)
      return {} if ids.empty?
      response = nil
      docs = {}

      params = { index: prepare_read_index(idx), type: type }
      array_slice_indexes(ids).each do |slice|
        slice_params = params.merge(body: get_docs_query(
          { ids: { type: type, values: slice } },
          slice.size))
        slice_params[:_source] = [] if
          !with_source || (source.is_a?(Array) && source.blank?)
        slice_params[:_source] ||= source if source.present?
        response = request(:search, slice_params)
        parse_response(response, docs)
      end
      with_source ? docs : docs.keys
    end # get_docs

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

    # docs - [docs] or {id => doc}
    def save_docs(docs, method = :index, idx = @idx, type = 'document')
      result = { ok: true, errors: [] }
      return result if docs.blank?
      to_save =
        if docs.is_a?(Hash) # convert to array
          docs.stringify_keys!
          if docs.include?('_id')
            [docs]
          else
            docs.map do |id, doc|
              next unless id
              doc.merge('_id' => id)
            end.compact
          end
        else
          docs
        end
      raise("Incorrect docs supplied (#{docs.class})") unless to_save.is_a?(Array)
      array_slice_indexes(to_save, BULK_STORE).each do |slice|
        bulk = create_bulk(slice, idx, method, type)
        next if bulk.blank?

        response = request(:bulk, body: bulk)
        sleep(@config[:bulk_sleep].to_i) if @config[:bulk_sleep]

        next if response['errors'].blank?
        result[:errors] <<
          if response['items'].blank?
            response['errors']
          else
            # typical es error
            (response['items'] || []).map do |item|
              next unless item[item.keys[0]].include?('error')
              item[item.keys[0]]
            end.compact
          end
      end
      result[:ok] = false unless result[:errors].empty?
      result
    end # save_docs

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
  end
end
