# encoding: utf-8
require 'active_support/core_ext/hash'
require 'active_support/core_ext/object/blank'

require_relative 'request'
require_relative 'query'
module ElasticsearchRasi
  class Base
    class << self
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
    end
  end
end
