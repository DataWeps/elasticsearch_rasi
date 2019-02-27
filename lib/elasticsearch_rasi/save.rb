require 'active_support/core_ext/hash'

require 'elasticsearch_rasi/common'
require 'elasticsearch_rasi/request'

module ElasticsearchRasi
  module Save
    include Request
    BULK_STORE = 500

    # docs - [docs] or {id => doc}
    def save_docs(docs, method = :index, idx = @idx, type = 'document')
      result = { ok: true, errors: [] }
      return result if docs.blank?
      to_save = prepare_docs(docs)

      raise(TypeError, "Incorrect docs supplied (#{docs.class})") unless to_save.is_a?(Array)
      slice_save!(result, to_save, method, idx, type)
      result[:ok] = false unless result[:errors].empty?
      result
    end

    def create_bulk(slice, idx, method = :index, type = 'document')
      slice.map do |doc|
        id_save = doc.delete('_id') || next
        index = Common.prepare_index(idx, doc, @max_age, @write_date) || next
        Common.prepare_search_author!(doc)
        doc = { :doc => doc } if method == :update
        {
          method => {
            _index: index,
            _id:    id_save,
            _type:  type }.merge(doc.empty? ? {} : { data: doc }) }
      end.compact
    end

  private

    def slice_save!(result, to_save, method, idx, type)
      Common.array_slice_indices(to_save, BULK_STORE).each do |slice|
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
      result
    end

    def prepare_docs(docs)
      if docs.is_a?(Hash) # convert to array
        docs.stringify_keys!
        if docs.include?('_id')
          [docs]
        else
          docs.map do |id, doc|
            if !id || !doc.is_a?(Hash)
              raise(TypeError,
                    "Wrong HashType { id => doc } of '#{docs}'")
            end
            doc.merge('_id' => id)
          end.compact
        end
      else
        docs
      end
    end
  end
end
