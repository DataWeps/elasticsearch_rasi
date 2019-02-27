# encoding: utf-8
require 'unicode_utils/downcase'
require 'digest/sha1'

require_relative 'err/parse_response_error'

module ElasticsearchRasi
  class Common
    class << self
      # prepare array indexes,lengths in the manner of slices
      # e.g. for a.length=25 and cnt=10 return [[0,10],[10,10],[20,5]]
      def array_slice_indices(ids, cnt = SLICES)
        rslt = []
        rslt << ids.shift(cnt) until ids.empty?
        rslt
      end

      def response_error(response)
        raise(ParseResponseError, response.to_s) if \
          response.blank? || response.include?('errors')
      end

      # translate results from ES to {id => doc}
      def parse_response(response, docs = {})
        response_error(response)
        response['hits']['hits'].each_with_object(docs) do |doc, mem|
          mem[doc['_id']] = doc['_source']
        end
      end

      def prepare_index(index, doc, max_age, write_date)
        return index if doc.blank? || !write_date || doc['published_at'].blank?
        parsed_published_at = Time.parse(doc['published_at'])
        # max_age could be nil in the case of ignore_max_age
        return nil if max_age &&
                      (parsed_published_at.to_i < @max_age ||
                       parsed_published_at.to_i > (Time.now.to_i + (3 * 3600)))
        "#{index}_#{parsed_published_at.strftime('%Y%m')}"
      end

      def prepare_read_index(index, read_date, read_date_months)
        return index unless read_date
        read_date_months.join(',')
      end

      def prepare_search_author!(doc)
        return if doc['author'].blank? || doc['author']['name'].blank?
        author =
          if doc['author'].is_a?(Hash)
            doc['author']['name']
          else
            doc['author']
          end

        doc['search_author'] = {
          'name' => author,
          'author_hash' => compute_author_hash(author) }
      end

      def compute_author_hash(author)
        UnicodeUtils.downcase(Digest::SHA1.hexdigest(author))
      end
    end
  end
end
